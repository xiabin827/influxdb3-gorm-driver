package dialector

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/InfluxCommunity/influxdb3-go/v2/influxdb3"
	"gorm.io/gorm"
	"gorm.io/gorm/callbacks"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/logger"
	"gorm.io/gorm/migrator"
	"gorm.io/gorm/schema"
)

// InfluxDBStmt 实现 driver.Stmt 接口
type InfluxDBStmt struct {
	query  string
	client *influxdb3.Client
}

func (s *InfluxDBStmt) Close() error {
	return nil
}

func (s *InfluxDBStmt) NumInput() int {
	return -1 // 不确定参数数量
}

func (s *InfluxDBStmt) Exec(args []driver.Value) (driver.Result, error) {
	// 转换查询和参数
	influxQuery, err := translateQuery(s.query, argsToInterfaces(args)...)
	if err != nil {
		return nil, err
	}

	// 执行查询
	_, err = s.client.Query(context.Background(), influxQuery)
	if err != nil {
		return nil, err
	}

	return driverResult{&InfluxDBResult{rowsAffected: 1}}, nil
}

func (s *InfluxDBStmt) Query(args []driver.Value) (driver.Rows, error) {
	// 转换查询和参数
	influxQuery, err := translateQuery(s.query, argsToInterfaces(args)...)
	if err != nil {
		return nil, err
	}

	// 执行查询
	iterator, err := s.client.Query(context.Background(), influxQuery)
	if err != nil {
		return nil, err
	}

	// 将 InfluxDB 查询结果转换为 driver.Rows
	rows := wrapRows(&InfluxDBRows{
		Iterator: iterator,
	})
	return rows, nil
}

// 辅助函数：将 driver.Value 数组转换为 interface{} 数组
func argsToInterfaces(args []driver.Value) []interface{} {
	result := make([]interface{}, len(args))
	for i, v := range args {
		result[i] = v
	}
	return result
}

// InfluxDBResult 实现 sql.Result 接口
type InfluxDBResult struct {
	rowsAffected int64
}

func (r *InfluxDBResult) LastInsertId() (int64, error) {
	return 0, nil
}

func (r *InfluxDBResult) RowsAffected() (int64, error) {
	return r.rowsAffected, nil
}

type Config struct {
	Host       string
	Token      string
	Database   string
	ClientOpts *influxdb3.ClientConfig
	Conn       gorm.ConnPool
	Client     *influxdb3.Client // 允许直接传入已有的客户端

	// Additional configuration options
	DisableNanoTimestamps     bool // Disables nanosecond precision in timestamps
	DefaultStringSize         uint // Default size for string fields
	DefaultBinarySize         uint // Default size for binary fields
	SkipInitializeWithVersion bool // Skip smart configure based on detected version
	DefaultDatetimePrecision  *int // Default datetime precision
}

// Dialector InfluxDB3 dialector
type Dialector struct {
	*Config
}

// Name 返回数据库方言的名称
func (dialector *Dialector) Name() string {
	return "influxdb3"
}

// Open 打开数据库连接
func Open(dsn string) gorm.Dialector {
	// 解析DSN格式: "host=xxx token=xxx database=xxx"
	configs := make(map[string]string)
	for _, v := range strings.Split(dsn, " ") {
		if parts := strings.SplitN(v, "=", 2); len(parts) == 2 {
			configs[parts[0]] = parts[1]
		}
	}

	d := &Dialector{
		Config: &Config{
			Host:     configs["host"],
			Token:    configs["token"],
			Database: configs["database"],
		},
	}
	return d
}

// New 创建新的连接
func New(config Config) gorm.Dialector {
	return &Dialector{Config: &config}
}

// Initialize 初始化数据库连接
func (dialector *Dialector) Initialize(db *gorm.DB) (err error) {
	if db == nil {
		return errors.New("GORM DB对象为空")
	}

	// 验证配置
	if dialector.Config == nil {
		return errors.New("InfluxDB配置为空")
	}

	// 创建或使用已有的客户端
	if dialector.Conn != nil {
		db.ConnPool = dialector.Conn
	} else {
		var client *influxdb3.Client
		if dialector.Client != nil {
			client = dialector.Client
		} else {
			// 未提供客户端，需验证连接配置
			if dialector.Host == "" {
				return errors.New("InfluxDB主机地址为空")
			}

			if dialector.ClientOpts != nil {
				client, err = influxdb3.New(*dialector.ClientOpts)
			} else {
				client, err = influxdb3.New(influxdb3.ClientConfig{
					Host:     dialector.Host,
					Token:    dialector.Token,
					Database: dialector.Database,
				})
			}
			if err != nil {
				return fmt.Errorf("failed to initialize InfluxDB client: %w", err)
			}
		}

		// 创建连接池
		connPool := &InfluxDBConnPool{
			client: client,
		}

		// 验证连接
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		// 尝试执行简单查询验证连接
		_, err = client.Query(ctx, "SELECT VERSION()")
		if err != nil {
			return fmt.Errorf("无法连接到InfluxDB: %w", err)
		}

		// 存储客户端到 dialector 中，便于后续使用
		dialector.Client = client

		// 设置连接池
		db.ConnPool = connPool
	}

	// 初始化回调
	callbacks.RegisterDefaultCallbacks(db, &callbacks.Config{})

	// 注册自定义子句构造器
	//db.ClauseBuilders["LIMIT"] = dialector.buildLimitClause

	// 注册自定义回调
	// 注：暂不需要实现自定义回调函数
	// dialector.RegisterCallbacks(db)

	return nil
}

// 转换查询和参数
func translateQuery(query string, args ...any) (string, error) {
	// 如果查询为空，返回错误
	if query == "" {
		return "", errors.New("查询语句为空")
	}

	// 替换参数占位符
	for _, arg := range args {
		switch v := arg.(type) {
		case string:
			// 字符串需要加引号
			query = strings.Replace(query, "?", fmt.Sprintf("'%s'", strings.ReplaceAll(v, "'", "''")), 1)
		case time.Time:
			// 时间格式化为 RFC3339
			query = strings.Replace(query, "?", fmt.Sprintf("'%s'", v.Format(time.RFC3339)), 1)
		case nil:
			// NULL 值
			query = strings.Replace(query, "?", "NULL", 1)
		default:
			// 其他类型直接转换为字符串
			query = strings.Replace(query, "?", fmt.Sprintf("%v", v), 1)
		}
	}

	// 处理 SELECT 语句
	if strings.HasPrefix(strings.ToUpper(query), "SELECT") {
		// 将 SQL 查询转换为 Flux 查询
		// 这里是一个简化版本，实际实现需要更复杂的解析和转换

		// 示例：
		// 输入：SELECT * FROM "table_name" WHERE "column" = 'value'
		// 输出：SELECT * FROM "table_name" WHERE "column" = 'value'

		// InfluxDB 3.0 支持 SQL 语法，可能不需要转换
		// 但这里我们可以添加一些优化或特殊处理

		// 处理表名中的引号
		query = strings.Replace(query, "`", "\"", -1)

		return query, nil
	} else if strings.HasPrefix(strings.ToUpper(query), "INSERT") {
		// 处理 INSERT 语句
		// InfluxDB 使用行协议而不是 INSERT 语句
		// 实际实现需要将 INSERT 转换为行协议
		return "", errors.New("不支持 INSERT 语句，请使用 GORM 的 Create 方法")
	} else if strings.HasPrefix(strings.ToUpper(query), "UPDATE") {
		// 处理 UPDATE 语句
		return "", errors.New("不支持 UPDATE 语句，请使用 GORM 的 Update 方法")
	} else if strings.HasPrefix(strings.ToUpper(query), "DELETE") {
		// 处理 DELETE 语句
		return "", errors.New("不支持 DELETE 语句，请使用 GORM 的 Delete 方法")
	}

	// 对于其他类型的查询，直接返回
	return query, nil
}

// Migrator 返回迁移工具
func (dialector *Dialector) Migrator(db *gorm.DB) gorm.Migrator {
	return Migrator{migrator.Migrator{Config: migrator.Config{
		DB:        db,
		Dialector: dialector,
	}}}
}

// DataTypeOf 返回给定字段的数据类型
func (dialector Dialector) DataTypeOf(field *schema.Field) string {
	// 根据Go类型映射到InfluxDB类型
	switch field.DataType {
	case schema.Bool:
		return "BOOLEAN"
	case schema.Int, schema.Uint:
		return "INTEGER"
	case schema.Float:
		return "DOUBLE"
	case schema.String:
		// 使用默认字符串大小
		if dialector.DefaultStringSize > 0 && field.Size == 0 && field.PrimaryKey {
			field.Size = int(dialector.DefaultStringSize)
		}
		return "STRING"
	case schema.Time:
		return "TIMESTAMP"
	case schema.Bytes:
		// 使用默认二进制大小
		if dialector.DefaultBinarySize > 0 && field.Size == 0 {
			field.Size = int(dialector.DefaultBinarySize)
		}
		return "BINARY"
	default:
		return "STRING"
	}
}

// DefaultValueOf 返回字段的默认值表达式
func (dialector Dialector) DefaultValueOf(field *schema.Field) clause.Expression {
	return clause.Expr{SQL: ""}
}

// BindVarTo 绑定变量
func (dialector Dialector) BindVarTo(writer clause.Writer, stmt *gorm.Statement, v interface{}) {
	writer.WriteByte('?')
}

// QuoteTo 添加引号
func (dialector Dialector) QuoteTo(writer clause.Writer, str string) {
	writer.WriteByte('"')
	writer.WriteString(str)
	writer.WriteByte('"')
}

// Explain 解析SQL
func (dialector Dialector) Explain(sql string, vars ...interface{}) string {
	return logger.ExplainSQL(sql, nil, `"`, vars...)
}

// 构建LIMIT子句
func (dialector *Dialector) buildLimitClause(c clause.Clause, builder clause.Builder) {
	if limit, ok := c.Expression.(clause.Limit); ok {
		if limit.Limit != nil {
			builder.WriteString(fmt.Sprintf("LIMIT %d", *limit.Limit))
			if limit.Offset > 0 {
				builder.WriteString(fmt.Sprintf(" OFFSET %d", limit.Offset))
			}
		} else if limit.Offset > 0 {
			builder.WriteString(fmt.Sprintf("OFFSET %d", limit.Offset))
		}
	}
}

// Migrator InfluxDB3的迁移器
type Migrator struct {
	migrator.Migrator
}

// AutoMigrate 自动迁移表结构
func (m Migrator) AutoMigrate(dst ...interface{}) error {
	// InfluxDB通常不需要这样的显式迁移
	return nil
}

// HasTable 检查表是否存在
func (m Migrator) HasTable(value interface{}) bool {
	var tableName string

	if v, ok := value.(string); ok {
		tableName = v
	} else {
		stmt := &gorm.Statement{DB: m.DB}
		if err := stmt.Parse(value); err == nil {
			tableName = stmt.Table
		}
	}

	// 查询是否存在该measurement
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if dialector, ok := m.Dialector.(*Dialector); ok {
		// 使用InfluxDB客户端查询
		query := fmt.Sprintf(`SHOW MEASUREMENTS WHERE name = '%s'`, tableName)
		if iterator, err := dialector.Client.Query(ctx, query); err == nil {
			defer func() {
				// InfluxDB3客户端迭代器可能没有Close方法，这里处理可能的错误
				if iterator != nil {
					// 消费所有数据
					for iterator.Next() {
						// 继续迭代直到结束
					}
				}
			}()
			return iterator.Next() // 如果有下一行，表示表存在
		}
	}

	return false
}

// CreateTable 创建表
func (m Migrator) CreateTable(values ...interface{}) error {
	// InfluxDB会在写入数据时自动创建measurement
	return nil
}

// DropTable 删除表
func (m Migrator) DropTable(values ...interface{}) error {
	for _, value := range values {
		var tableName string

		if v, ok := value.(string); ok {
			tableName = v
		} else {
			stmt := &gorm.Statement{DB: m.DB}
			if err := stmt.Parse(value); err == nil {
				tableName = stmt.Table
			}
		}

		// 构建删除measurement的查询
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		if dialector, ok := m.Dialector.(*Dialector); ok {
			// 使用InfluxDB客户端执行删除
			query := fmt.Sprintf(`DROP MEASUREMENT "%s"`, tableName)
			if _, err := dialector.Client.Query(ctx, query); err != nil {
				return err
			}
		}
	}

	return nil
}
