package dialector

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// Create 创建记录
func (dialector *Dialector) Create(ctx context.Context, db *gorm.DB) error {
	if dialector.Client == nil {
		return errors.New("未初始化InfluxDB客户端")
	}

	// 获取要写入的数据
	stmt := db.Statement
	if stmt.Schema == nil {
		return errors.New("缺少schema信息")
	}

	// 构建行协议
	lineProtocol, err := buildLineProtocol(stmt)
	if err != nil {
		return fmt.Errorf("构建行协议失败: %w", err)
	}

	// 写入数据
	err = dialector.Client.Write(ctx, []byte(lineProtocol))
	if err != nil {
		return fmt.Errorf("写入数据失败: %w", err)
	}

	// 返回行影响数
	db.RowsAffected = 1

	return nil
}

// 构建InfluxDB行协议
func buildLineProtocol(stmt *gorm.Statement) (string, error) {
	if stmt.Schema == nil {
		return "", errors.New("缺少schema信息")
	}

	// 获取表名/measurement
	measurement := stmt.Table

	// 准备标签和字段
	tags := make(map[string]string)
	fields := make(map[string]interface{})
	var timestamp time.Time

	// 使用反射获取结构体的值
	reflectValue := stmt.ReflectValue
	if reflectValue.Kind() == reflect.Ptr {
		reflectValue = reflectValue.Elem()
	}

	if reflectValue.Kind() != reflect.Struct {
		return "", errors.New("只支持结构体类型")
	}

	// 处理所有字段
	for _, field := range stmt.Schema.Fields {
		fieldValue, isZero := field.ValueOf(stmt.Context, reflectValue)
		if fieldValue == nil || isZero {
			continue
		}

		// 检查字段标签
		dbName := field.DBName
		isTag := false

		// 检查标签以确定是否为tag
		if tagSettings, ok := field.TagSettings["GORM:TAG"]; ok && tagSettings == "TAG" {
			isTag = true
		} else if tagSettings, ok := field.TagSettings["TYPE"]; ok && tagSettings == "tag" {
			isTag = true
		}

		// 根据字段类型和标签设置分配为tag或field
		if isTag {
			// 将值转换为字符串
			tags[dbName] = fmt.Sprintf("%v", fieldValue)
		} else if dbName == "time" || dbName == "Time" || dbName == "_time" {
			// 处理时间戳
			switch t := fieldValue.(type) {
			case time.Time:
				timestamp = t
			default:
				// 忽略非time.Time类型的时间字段
			}
		} else if dbName != "_measurement" && dbName != "Measurement" && dbName != "ID" {
			// 处理普通字段
			fields[dbName] = fieldValue
		}
	}

	// 构建行协议字符串
	lineProtocol := measurement

	// 添加标签
	for k, v := range tags {
		lineProtocol += fmt.Sprintf(",%s=%s", k, v)
	}

	// 添加字段
	lineProtocol += " "
	firstField := true
	for k, v := range fields {
		if !firstField {
			lineProtocol += ","
		}

		// 根据值类型添加适当的格式
		switch v := v.(type) {
		case string:
			lineProtocol += fmt.Sprintf("%s=\"%s\"", k, v)
		case int, int8, int16, int32, int64:
			lineProtocol += fmt.Sprintf("%s=%di", k, v)
		case uint, uint8, uint16, uint32, uint64:
			lineProtocol += fmt.Sprintf("%s=%di", k, v)
		case float32, float64:
			lineProtocol += fmt.Sprintf("%s=%v", k, v)
		case bool:
			lineProtocol += fmt.Sprintf("%s=%t", k, v)
		default:
			lineProtocol += fmt.Sprintf("%s=\"%v\"", k, v)
		}

		firstField = false
	}

	// 添加时间戳（如果有）
	if !timestamp.IsZero() {
		// 纳秒精度时间戳
		lineProtocol += fmt.Sprintf(" %d", timestamp.UnixNano())
	}

	return lineProtocol, nil
}

// Update 更新记录
func (dialector *Dialector) Update(ctx context.Context, db *gorm.DB) error {
	// InfluxDB不支持直接更新，需要删除旧数据并插入新数据
	return errors.New("InfluxDB不支持直接更新，请使用删除并重新插入")
}

// Delete 删除记录
func (dialector *Dialector) Delete(ctx context.Context, db *gorm.DB) error {
	if dialector.Client == nil {
		return errors.New("未初始化InfluxDB客户端")
	}

	// 从条件构建DELETE查询
	query, err := buildDeleteQuery(db)
	if err != nil {
		return fmt.Errorf("构建删除查询失败: %w", err)
	}

	// 执行查询
	_, err = dialector.Client.Query(ctx, query)
	if err != nil {
		return fmt.Errorf("执行删除失败: %w", err)
	}

	// 默认设置影响行数
	db.RowsAffected = 1

	return nil
}

// 构建DELETE查询
func buildDeleteQuery(db *gorm.DB) (string, error) {
	stmt := db.Statement
	if stmt.Schema == nil || stmt.Table == "" {
		return "", errors.New("缺少表或schema信息")
	}

	// InfluxDB的DELETE语法
	query := fmt.Sprintf("DELETE FROM %s", stmt.Table)

	// 添加WHERE条件
	if len(stmt.Clauses) > 0 {
		if whereClause, ok := stmt.Clauses["WHERE"]; ok {
			if where, ok := whereClause.Expression.(clause.Where); ok && len(where.Exprs) > 0 {
				query += " WHERE "

				for i, expr := range where.Exprs {
					if i > 0 {
						query += " AND "
					}

					// 这里需要根据表达式类型构建WHERE子句
					// 简化处理，仅支持基本条件
					switch e := expr.(type) {
					case clause.Eq:
						query += fmt.Sprintf("%s = %v", e.Column, formatValueForQuery(e.Value))
					case clause.Neq:
						query += fmt.Sprintf("%s != %v", e.Column, formatValueForQuery(e.Value))
					case clause.Gt:
						query += fmt.Sprintf("%s > %v", e.Column, formatValueForQuery(e.Value))
					case clause.Gte:
						query += fmt.Sprintf("%s >= %v", e.Column, formatValueForQuery(e.Value))
					case clause.Lt:
						query += fmt.Sprintf("%s < %v", e.Column, formatValueForQuery(e.Value))
					case clause.Lte:
						query += fmt.Sprintf("%s <= %v", e.Column, formatValueForQuery(e.Value))
					default:
						return "", fmt.Errorf("不支持的删除条件: %T", expr)
					}
				}
			}
		}
	}

	return query, nil
}

// 格式化查询中的值
func formatValueForQuery(value interface{}) string {
	switch v := value.(type) {
	case string:
		return fmt.Sprintf("'%s'", v)
	case time.Time:
		return fmt.Sprintf("'%s'", v.Format(time.RFC3339))
	default:
		return fmt.Sprintf("%v", v)
	}
}
