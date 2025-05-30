package dialector

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
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

// 实现 driver.Rows 接口的包装器
type driverRows struct {
	*InfluxDBRows
	currentRow map[string]any
}

func (r driverRows) Columns() []string {
	cols, _ := r.InfluxDBRows.Columns()
	return cols
}

func (r driverRows) Close() error {
	return r.InfluxDBRows.Close()
}

func (r driverRows) Next(dest []driver.Value) error {
	if !r.InfluxDBRows.Next() {
		if err := r.InfluxDBRows.Err(); err != nil {
			return err
		}
		return io.EOF
	}

	// 获取当前行的数据
	cols, _ := r.InfluxDBRows.Columns()

	// 检查是否有列存在，如果为空则跳过处理
	if len(cols) == 0 {
		return nil
	}

	// 如果列数与目标变量数不匹配，使用可用的列数
	colsToProcess := len(cols)
	if len(dest) < colsToProcess {
		colsToProcess = len(dest)
	}

	// 将当前行的数据复制到目标变量
	for i := 0; i < colsToProcess; i++ {
		col := cols[i]
		val, ok := r.InfluxDBRows.rowData[col]
		if !ok {
			dest[i] = nil
			continue
		}

		// 根据不同的数据类型进行转换
		switch v := val.(type) {
		case string:
			dest[i] = v
		case int:
			dest[i] = int64(v)
		case int32:
			dest[i] = int64(v)
		case int64:
			dest[i] = v
		case float32:
			dest[i] = float64(v)
		case float64:
			// 如果列名包含"action_type"或"duration"，转换为整数
			if col == "action_type" || col == "duration" {
				dest[i] = int64(v)
			} else {
				dest[i] = v
			}
		case bool:
			dest[i] = v
		case time.Time:
			dest[i] = v
		case nil:
			dest[i] = nil
		default:
			dest[i] = fmt.Sprintf("%v", v)
		}
	}

	// 将剩余的目标变量设置为nil
	for i := colsToProcess; i < len(dest); i++ {
		dest[i] = nil
	}

	return nil
}

// 实现 driver.Result 接口的包装器
type driverResult struct {
	*InfluxDBResult
}

func (r driverResult) LastInsertId() (int64, error) {
	return r.InfluxDBResult.LastInsertId()
}

func (r driverResult) RowsAffected() (int64, error) {
	return r.InfluxDBResult.RowsAffected()
}

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

// InfluxDBRows 实现结果集接口
type InfluxDBRows struct {
	Iterator *influxdb3.QueryIterator
	rowData  map[string]any
	columns  []string
	current  bool
	err      error
}

// Next 移动到下一行
func (r *InfluxDBRows) Next() bool {
	if r.Iterator == nil {
		r.err = errors.New("查询迭代器为空")
		return false
	}

	r.current = r.Iterator.Next()
	if r.current {
		r.rowData = r.Iterator.Value()
		if r.rowData == nil {
			r.err = errors.New("迭代器返回的行数据为空")
			r.current = false
			return false
		}

		// 每次获取新行数据时，都更新列信息
		// 如果列为空或者当前行包含新的列，则更新列信息
		if len(r.columns) == 0 {
			r.columns = make([]string, 0, len(r.rowData))
			for key := range r.rowData {
				r.columns = append(r.columns, key)
			}
		}
	} else {
		if err := r.Iterator.Err(); err != nil {
			r.err = err
		}
	}

	return r.current
}

// Scan 扫描当前行到目标变量
func (r *InfluxDBRows) Scan(dest ...any) error {
	if !r.current {
		return errors.New("没有当前行")
	}

	if len(dest) != len(r.columns) {
		return fmt.Errorf("目标数量 (%d) 与列数 (%d) 不匹配", len(dest), len(r.columns))
	}

	for i, col := range r.columns {
		val, ok := r.rowData[col]
		if !ok {
			return fmt.Errorf("列 %s 不存在", col)
		}

		// 根据不同的数据类型进行转换
		switch d := dest[i].(type) {
		case *string:
			switch v := val.(type) {
			case string:
				*d = v
			case nil:
				*d = ""
			default:
				*d = fmt.Sprintf("%v", v)
			}
		case *int:
			switch v := val.(type) {
			case int:
				*d = v
			case int64:
				*d = int(v)
			case float64:
				*d = int(v)
			case nil:
				*d = 0
			default:
				return fmt.Errorf("无法将 %T 转换为 int", val)
			}
		case *int64:
			switch v := val.(type) {
			case int:
				*d = int64(v)
			case int64:
				*d = v
			case float64:
				*d = int64(v)
			case nil:
				*d = 0
			default:
				return fmt.Errorf("无法将 %T 转换为 int64", val)
			}
		case *float64:
			switch v := val.(type) {
			case float64:
				*d = v
			case int:
				*d = float64(v)
			case int64:
				*d = float64(v)
			case nil:
				*d = 0
			default:
				return fmt.Errorf("无法将 %T 转换为 float64", val)
			}
		case *bool:
			switch v := val.(type) {
			case bool:
				*d = v
			case nil:
				*d = false
			default:
				return fmt.Errorf("无法将 %T 转换为 bool", val)
			}
		case *time.Time:
			switch v := val.(type) {
			case time.Time:
				*d = v
			case string:
				if t, err := time.Parse(time.RFC3339, v); err == nil {
					*d = t
				} else {
					return fmt.Errorf("无法将字符串 %s 转换为时间: %v", v, err)
				}
			case nil:
				*d = time.Time{}
			default:
				return fmt.Errorf("无法将 %T 转换为 time.Time", val)
			}
		default:
			return fmt.Errorf("不支持的目标类型: %T", dest[i])
		}
	}

	return nil
}

// Err 返回迭代过程中的错误
func (r *InfluxDBRows) Err() error {
	return r.err
}

// Close 关闭结果集
func (r *InfluxDBRows) Close() error {
	return nil
}

// Columns 返回列名
func (r *InfluxDBRows) Columns() ([]string, error) {
	if r.columns == nil {
		r.columns = []string{} // 确保返回空切片而不是nil
	}
	return r.columns, nil
}

// NextResultSet 移动到下一个结果集
func (r *InfluxDBRows) NextResultSet() bool {
	// InfluxDB 不支持多个结果集
	return false
}

// ColumnTypes 返回列类型信息
func (r *InfluxDBRows) ColumnTypes() ([]*sql.ColumnType, error) {
	return nil, errors.New("未实现")
}

// 创建一个包装的 InfluxDBRows
func wrapRows(rows *InfluxDBRows) driverRows {
	return driverRows{
		InfluxDBRows: rows,
		currentRow:   make(map[string]any),
	}
}

// InfluxDBConnPool 实现 gorm.ConnPool 接口
type InfluxDBConnPool struct {
	client *influxdb3.Client
}

// PrepareContext 实现 gorm.ConnPool 接口
func (p *InfluxDBConnPool) PrepareContext(ctx context.Context, query string) (*sql.Stmt, error) {
	// 注意：这里不能直接返回自定义 stmt，因为它不是 *sql.Stmt 类型
	// 这里的实现是一个简化版，实际上需要更复杂的机制来封装自定义 stmt
	return &sql.Stmt{}, nil
}

// ExecContext 实现 gorm.ConnPool 接口
func (p *InfluxDBConnPool) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	// 转换查询和参数
	influxQuery, err := translateQuery(query, args...)
	if err != nil {
		return nil, err
	}

	// 执行查询
	_, err = p.client.Query(ctx, influxQuery)
	if err != nil {
		return nil, err
	}

	return &InfluxDBResult{rowsAffected: 1}, nil
}

// QueryContext 实现 gorm.ConnPool 接口
func (p *InfluxDBConnPool) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	// 创建一个自定义的 driver.Conn 实现
	connector := &driverConnector{
		pool: p,
	}

	// 创建标准的sql.DB对象
	db := sql.OpenDB(connector)
	defer db.Close()

	// 执行实际查询
	return db.QueryContext(ctx, query, args...)
}

// QueryRowContext 实现 gorm.ConnPool 接口
func (p *InfluxDBConnPool) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	// 简化实现
	return &sql.Row{}
}

// BeginTx 实现 gorm.ConnPool 接口
func (p *InfluxDBConnPool) BeginTx(ctx context.Context, opts *sql.TxOptions) (gorm.ConnPool, error) {
	// InfluxDB 不支持事务，返回错误
	return nil, errors.New("InfluxDB does not support transactions")
}

// Commit 实现 gorm.ConnPool 接口
func (p *InfluxDBConnPool) Commit() error {
	return nil
}

// Rollback 实现 gorm.ConnPool 接口
func (p *InfluxDBConnPool) Rollback() error {
	return nil
}

// Close 实现 gorm.ConnPool 接口
func (p *InfluxDBConnPool) Close() error {
	if p.client != nil {
		return p.client.Close()
	}
	return nil
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
	db.ClauseBuilders["LIMIT"] = dialector.buildLimitClause

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

// driverConnector 实现 driver.Connector 接口
type driverConnector struct {
	pool *InfluxDBConnPool
}

func (c *driverConnector) Connect(context.Context) (driver.Conn, error) {
	return &driverConn{pool: c.pool}, nil
}

func (c *driverConnector) Driver() driver.Driver {
	return &InfluxDBDriver{}
}

// driverConn 实现 driver.Conn 接口
type driverConn struct {
	pool *InfluxDBConnPool
}

func (c *driverConn) Prepare(query string) (driver.Stmt, error) {
	return &InfluxDBStmt{query: query, client: c.pool.client}, nil
}

func (c *driverConn) Close() error {
	return nil
}

func (c *driverConn) Begin() (driver.Tx, error) {
	return nil, fmt.Errorf("transactions not supported")
}

// Query 实现 driver.Queryer 接口
func (c *driverConn) Query(query string, args []driver.Value) (driver.Rows, error) {
	// 转换查询和参数
	influxQuery, err := translateQuery(query, argsToInterfaces(args)...)
	if err != nil {
		return nil, err
	}

	// 执行查询
	iterator, err := c.pool.client.Query(context.Background(), influxQuery)
	if err != nil {
		return nil, err
	}

	// 创建自定义的 InfluxDBRows
	influxRows := &InfluxDBRows{
		Iterator: iterator,
		rowData:  make(map[string]any),
		columns:  []string{}, // 初始化为空切片而不是nil
	}

	// 预先获取第一条数据以填充列信息
	if influxRows.Next() {
		// 获取列名
		columns, err := influxRows.Columns()
		if err != nil {
			return nil, err
		}
		influxRows.columns = columns
	}

	// 返回包装后的行对象
	return wrapRows(influxRows), nil
}

// InfluxDBDriver 实现 driver.Driver 接口
type InfluxDBDriver struct{}

func (d *InfluxDBDriver) Open(name string) (driver.Conn, error) {
	return nil, fmt.Errorf("use OpenConnector instead")
}
