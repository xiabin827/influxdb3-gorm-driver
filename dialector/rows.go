package dialector

import (
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"github.com/InfluxCommunity/influxdb3-go/v2/influxdb3"
	"io"
	"strings"
	"time"
)

// InfluxDBRows 实现结果集接口
type InfluxDBRows struct {
	Iterator *influxdb3.QueryIterator
	columns  []string // 列名列表
	skipNext bool     //true时不需要执行Next方法
	err      error
}

func (r *InfluxDBRows) Index() int64 {
	if r.Iterator == nil {
		return 0
	}

	if index, ok := r.Iterator.Index().(int64); ok {
		return index
	}

	return 0
}

// Next 移动到下一行
func (r *InfluxDBRows) Next() bool {
	if r.Iterator == nil {
		r.err = errors.New("查询迭代器为空")
		return false
	}

	// 如果skipNext为true，直接返回
	if r.skipNext {
		r.skipNext = false
		return true
	}

	if !r.Iterator.Next() {
		return false
	}

	rowData := r.Iterator.Value()
	// 如果列为空，则更新列信息
	if len(r.columns) == 0 {
		r.columns = make([]string, 0, len(rowData))
		for key := range rowData {
			r.columns = append(r.columns, key)
		}
	}

	return true
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

	// 如果没有目标变量但有列，这是一个特殊情况
	// 在sql.Rows.Next()调用期间可能发生，此时只需返回成功
	if len(dest) == 0 {
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
		val, ok := r.InfluxDBRows.Iterator.Value()[col]
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
			// 特殊处理：如果是COUNT函数的结果，转换为整数
			if strings.Contains(strings.ToLower(col), "count(") || strings.Contains(strings.ToLower(col), "count") ||
				col == "action_type" || col == "duration" {
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
