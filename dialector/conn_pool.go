package dialector

import (
	"context"
	"database/sql"
	"github.com/InfluxCommunity/influxdb3-go/v2/influxdb3"
	"gorm.io/gorm"
)

// 验证 InfluxDBConnPool 是否实现了 gorm.ConnPool 接口
var _ gorm.ConnPool = &InfluxDBConnPool{}

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
	// 创建一个自定义的 driver.Conn 实现
	connector := &driverConnector{
		pool: p,
	}

	// 创建标准的sql.DB对象
	db := sql.OpenDB(connector)
	defer db.Close()

	// 执行实际查询
	return db.QueryRowContext(ctx, query, args...)
}

// Close 实现 gorm.ConnPool 接口
func (p *InfluxDBConnPool) Close() error {
	if p.client != nil {
		return p.client.Close()
	}
	return nil
}
