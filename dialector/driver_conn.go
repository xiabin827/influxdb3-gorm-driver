package dialector

import (
	"context"
	"database/sql/driver"
	"fmt"
)

var (
	_ driver.Driver    = &InfluxDBDriver{}
	_ driver.Connector = &driverConnector{}
)

// InfluxDBDriver 实现 driver.Driver 接口
type InfluxDBDriver struct{}

func (d *InfluxDBDriver) Open(name string) (driver.Conn, error) {
	return nil, fmt.Errorf("use OpenConnector instead")
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
		columns:  []string{},
	}

	// 预先获取第一行数据以填充列信息
	if iterator.Next() {
		influxRows.skipNext = true
		// 获取第一行数据
		rowData := iterator.Value()
		if rowData != nil {
			// 提取列名
			influxRows.columns = make([]string, 0, len(rowData))
			for key := range rowData {
				influxRows.columns = append(influxRows.columns, key)
			}
		}
	}

	// 返回包装后的行对象
	return wrapRows(influxRows), nil
}
