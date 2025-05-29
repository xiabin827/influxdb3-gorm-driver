package dialector

import (
	"fmt"
	"strings"

	"github.com/InfluxCommunity/influxdb3-go/v2/influxdb3"
	"gorm.io/gorm"
	"gorm.io/gorm/callbacks"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/logger"
	"gorm.io/gorm/migrator"
	"gorm.io/gorm/schema"
)

type Config struct {
	Host       string
	Token      string
	Database   string
	ClientOpts *influxdb3.ClientConfig
	Conn       gorm.ConnPool
}

// Dialector InfluxDB3 dialector
type Dialector struct {
	*Config
	Client *influxdb3.Client
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
	// 初始化回调
	callbacks.RegisterDefaultCallbacks(db, &callbacks.Config{})

	// 创建或使用已有的客户端
	if dialector.Client == nil {
		var client *influxdb3.Client
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
		dialector.Client = client
	}

	// 注册自定义子句构造器
	db.ClauseBuilders["LIMIT"] = dialector.buildLimitClause

	// 注册自定义回调
	dialector.RegisterCallbacks(db)

	return nil
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
		return "STRING"
	case schema.Time:
		return "TIMESTAMP"
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
	return logger.ExplainSQL(sql, nil, `'`, vars...)
}

// 构建LIMIT子句
func (dialector *Dialector) buildLimitClause(c clause.Clause, builder clause.Builder) {
	if limit, ok := c.Expression.(clause.Limit); ok {
		if limit.Limit != nil {
			builder.WriteString(fmt.Sprintf("LIMIT %v", limit.Limit))
			if limit.Offset > 0 {
				builder.WriteString(fmt.Sprintf(" OFFSET %v", limit.Offset))
			}
		} else if limit.Offset > 0 {
			builder.WriteString(fmt.Sprintf("OFFSET %v", limit.Offset))
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
