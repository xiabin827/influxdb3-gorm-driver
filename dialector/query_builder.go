package dialector

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/InfluxCommunity/influxdb3-go/v2/influxdb3"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/schema"
)

// Query 执行查询
func (dialector *Dialector) Query(ctx context.Context, db *gorm.DB, query string, args ...any) (rows *QueryRows, err error) {
	if dialector.Client == nil {
		return nil, errors.New("未初始化InfluxDB客户端")
	}

	// 检查查询是否为空
	if query == "" {
		return nil, errors.New("查询语句为空")
	}

	// 转换GORM生成的SQL查询为InfluxDB查询
	influxQuery, err := dialector.translateQuery(db, query, args...)
	if err != nil {
		return nil, fmt.Errorf("转换查询失败: %w", err)
	}

	// 再次检查转换后的查询是否为空
	if influxQuery == "" {
		return nil, errors.New("转换后的查询语句为空")
	}

	// 执行查询
	iterator, err := dialector.Client.Query(ctx, influxQuery)
	if err != nil {
		return nil, fmt.Errorf("执行查询失败: %w", err)
	}

	// 确保迭代器不为空
	if iterator == nil {
		return nil, errors.New("查询结果为空")
	}

	// 将结果包装为GORM可用的格式
	return &QueryRows{
		Iterator: iterator,
		Schema:   db.Statement.Schema,
	}, nil
}

// 转换GORM查询为InfluxDB查询
func (dialector *Dialector) translateQuery(db *gorm.DB, query string, args ...any) (string, error) {
	if query != "" {
		// 如果提供了原始SQL查询，直接使用
		return query, nil
	}

	// 从GORM语句构建查询
	stmt := db.Statement
	if stmt == nil {
		return "", errors.New("语句对象为空")
	}

	if stmt.Schema == nil {
		return "", errors.New("缺少schema信息")
	}

	if stmt.Table == "" {
		// 尝试从Schema获取表名
		if stmt.Schema != nil {
			stmt.Table = stmt.Schema.Table
		}

		// 如果表名仍为空，返回错误
		if stmt.Table == "" {
			return "", errors.New("缺少表信息")
		}
	}

	// 获取表名和字段
	tableName := stmt.Table
	var selectFields []string

	// 处理SELECT子句
	if len(stmt.Selects) > 0 {
		selectFields = stmt.Selects
	} else if stmt.Schema != nil {
		// 使用所有字段
		for _, field := range stmt.Schema.Fields {
			dbName := field.DBName
			if dbName != "" {
				selectFields = append(selectFields, dialector.QuoteString(dbName))
			}
		}
	}

	if len(selectFields) == 0 {
		selectFields = []string{"*"}
	}

	// 构建基本查询
	queryBuilder := strings.Builder{}
	queryBuilder.WriteString("SELECT ")
	queryBuilder.WriteString(strings.Join(selectFields, ", "))
	queryBuilder.WriteString(" FROM ")
	queryBuilder.WriteString(dialector.QuoteString(tableName))

	// 处理WHERE子句
	if len(stmt.Clauses) > 0 {
		if whereClause, ok := stmt.Clauses["WHERE"]; ok {
			if where, ok := whereClause.Expression.(clause.Where); ok && len(where.Exprs) > 0 {
				queryBuilder.WriteString(" WHERE ")

				for i, expr := range where.Exprs {
					if i > 0 {
						queryBuilder.WriteString(" AND ")
					}

					// 转换不同类型的表达式
					switch e := expr.(type) {
					case clause.Eq:
						queryBuilder.WriteString(dialector.QuoteString(e.Column.(string)))
						queryBuilder.WriteString(" = ")
						queryBuilder.WriteString(formatValue(e.Value))
					case clause.Neq:
						queryBuilder.WriteString(dialector.QuoteString(e.Column.(string)))
						queryBuilder.WriteString(" != ")
						queryBuilder.WriteString(formatValue(e.Value))
					case clause.Gt:
						queryBuilder.WriteString(dialector.QuoteString(e.Column.(string)))
						queryBuilder.WriteString(" > ")
						queryBuilder.WriteString(formatValue(e.Value))
					case clause.Gte:
						queryBuilder.WriteString(dialector.QuoteString(e.Column.(string)))
						queryBuilder.WriteString(" >= ")
						queryBuilder.WriteString(formatValue(e.Value))
					case clause.Lt:
						queryBuilder.WriteString(dialector.QuoteString(e.Column.(string)))
						queryBuilder.WriteString(" < ")
						queryBuilder.WriteString(formatValue(e.Value))
					case clause.Lte:
						queryBuilder.WriteString(dialector.QuoteString(e.Column.(string)))
						queryBuilder.WriteString(" <= ")
						queryBuilder.WriteString(formatValue(e.Value))
					case clause.Like:
						queryBuilder.WriteString(dialector.QuoteString(e.Column.(string)))
						queryBuilder.WriteString(" LIKE ")
						queryBuilder.WriteString(formatValue(e.Value))
					case clause.IN:
						queryBuilder.WriteString(dialector.QuoteString(e.Column.(string)))
						queryBuilder.WriteString(" IN (")
						values := reflect.ValueOf(e.Values)
						for i := 0; i < values.Len(); i++ {
							if i > 0 {
								queryBuilder.WriteString(", ")
							}
							queryBuilder.WriteString(formatValue(values.Index(i).Interface()))
						}
						queryBuilder.WriteString(")")
					default:
						return "", fmt.Errorf("不支持的表达式类型: %T", expr)
					}
				}
			}
		}
	}

	// 处理ORDER BY子句
	if orderClause, ok := stmt.Clauses["ORDER"]; ok {
		if order, ok := orderClause.Expression.(clause.OrderBy); ok && len(order.Columns) > 0 {
			queryBuilder.WriteString(" ORDER BY ")

			for i, column := range order.Columns {
				if i > 0 {
					queryBuilder.WriteString(", ")
				}
				queryBuilder.WriteString(dialector.QuoteString(fmt.Sprint(column.Column)))
				if column.Desc {
					queryBuilder.WriteString(" DESC")
				}
			}
		}
	}

	// 处理LIMIT子句
	if limitClause, ok := stmt.Clauses["LIMIT"]; ok {
		if limit, ok := limitClause.Expression.(clause.Limit); ok {
			if limit.Limit != nil {
				queryBuilder.WriteString(fmt.Sprintf(" LIMIT %d", *limit.Limit))
			}
			if limit.Offset > 0 {
				queryBuilder.WriteString(fmt.Sprintf(" OFFSET %d", limit.Offset))
			}
		}
	}

	return queryBuilder.String(), nil
}

// 格式化值
func formatValue(value any) string {
	switch v := value.(type) {
	case string:
		return fmt.Sprintf("'%s'", strings.ReplaceAll(v, "'", "''"))
	case time.Time:
		return fmt.Sprintf("'%s'", v.Format(time.RFC3339))
	case nil:
		return "NULL"
	default:
		return fmt.Sprintf("%v", v)
	}
}

// QuoteString 为标识符添加引号
func (dialector Dialector) QuoteString(str string) string {
	return fmt.Sprintf(`"%s"`, str)
}

// QueryRows 查询结果行
type QueryRows struct {
	Iterator *influxdb3.QueryIterator
	Schema   *schema.Schema
	rowData  map[string]any
	columns  []string
	current  bool
	err      error // 存储迭代过程中的错误
}

// Next 移动到下一行
func (r *QueryRows) Next() bool {
	if r.Iterator == nil {
		r.err = errors.New("查询迭代器为空")
		return false
	}

	// 捕获可能的 panic
	defer func() {
		if p := recover(); p != nil {
			r.err = fmt.Errorf("迭代过程中发生panic: %v", p)
			r.current = false
		}
	}()

	r.current = r.Iterator.Next()
	if r.current {
		// 安全地获取当前行数据
		r.rowData = r.Iterator.Value()
		if r.rowData == nil {
			r.err = errors.New("迭代器返回的行数据为空")
			r.current = false
			return false
		}

		// 第一次获取列名
		if len(r.columns) == 0 && len(r.rowData) > 0 {
			for key := range r.rowData {
				r.columns = append(r.columns, key)
			}
		}
	} else {
		// 检查迭代器错误
		if err := r.Iterator.Err(); err != nil {
			r.err = fmt.Errorf("迭代器错误: %w", err)
		}
	}

	return r.current
}

// Err 返回迭代过程中的错误
func (r *QueryRows) Err() error {
	return r.err
}

// Scan 扫描当前行到目标变量
func (r *QueryRows) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}

	if !r.current {
		return errors.New("没有当前行数据，请先调用Next()")
	}

	if r.rowData == nil {
		return errors.New("当前行数据为空")
	}

	// 确保有足够的列
	if len(r.columns) == 0 {
		return errors.New("没有列信息")
	}

	if len(dest) != len(r.columns) {
		return fmt.Errorf("列数(%d)与目标变量数(%d)不匹配", len(r.columns), len(dest))
	}

	// 捕获可能的 panic
	defer func() {
		if p := recover(); p != nil {
			r.err = fmt.Errorf("扫描过程中发生panic: %v", p)
		}
	}()

	for i, colName := range r.columns {
		if val, ok := r.rowData[colName]; ok {
			// 跳过nil值
			if val == nil {
				continue
			}

			destVal := reflect.ValueOf(dest[i])
			if destVal.Kind() != reflect.Ptr {
				return fmt.Errorf("目标值必须是指针，列 %s", colName)
			}

			if !destVal.IsValid() || destVal.IsNil() {
				return fmt.Errorf("目标指针无效或为nil，列 %s", colName)
			}

			destElem := destVal.Elem()
			srcVal := reflect.ValueOf(val)

			if srcVal.Type().AssignableTo(destElem.Type()) {
				destElem.Set(srcVal)
			} else {
				// 尝试类型转换
				switch destElem.Kind() {
				case reflect.String:
					destElem.SetString(fmt.Sprintf("%v", val))
				case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
					floatVal, ok := val.(float64)
					if ok {
						destElem.SetInt(int64(floatVal))
					} else {
						return fmt.Errorf("无法将 %T 转换为 int 类型, 列 %s", val, colName)
					}
				case reflect.Float32, reflect.Float64:
					floatVal, ok := val.(float64)
					if ok {
						destElem.SetFloat(floatVal)
					} else {
						return fmt.Errorf("无法将 %T 转换为 float 类型, 列 %s", val, colName)
					}
				case reflect.Bool:
					boolVal, ok := val.(bool)
					if ok {
						destElem.SetBool(boolVal)
					} else {
						return fmt.Errorf("无法将 %T 转换为 bool 类型, 列 %s", val, colName)
					}
				default:
					return fmt.Errorf("不支持的类型转换: %v 到 %v, 列 %s", srcVal.Type(), destElem.Type(), colName)
				}
			}
		} else {
			return fmt.Errorf("列名 %s 不存在于结果中", colName)
		}
	}

	return nil
}

// Close 关闭结果集
func (r *QueryRows) Close() error {
	// InfluxDB的QueryIterator不需要显式关闭
	return nil
}
