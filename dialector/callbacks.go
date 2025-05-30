package dialector

import (
	"context"
	"errors"

	"gorm.io/gorm"
)

// 定义错误
var (
	ErrEmptySQL = errors.New("SQL语句为空")
)

// RegisterCallbacks 注册GORM回调
func (dialector *Dialector) RegisterCallbacks(db *gorm.DB) {
	// 注册创建回调
	if callback := db.Callback().Create().Get("gorm:create"); callback == nil {
		db.Callback().Create().Replace("gorm:create", func(db *gorm.DB) {
			if err := dialector.Create(context.Background(), db); err != nil {
				_ = db.AddError(err)
			}
		})
	} else {
		db.Callback().Create().Replace("gorm:create", callback)
	}

	// 注册查询回调
	if callback := db.Callback().Query().Get("gorm:query"); callback == nil {
		db.Callback().Query().Replace("gorm:query", func(db *gorm.DB) {
			// 检查SQL语句是否已生成
			if db.Statement.SQL.String() == "" {
				_ = db.AddError(ErrEmptySQL)
				return
			}

			rows, err := dialector.Query(context.Background(), db, db.Statement.SQL.String(), db.Statement.Vars...)
			if err != nil {
				_ = db.AddError(err)
				return
			}

			// 确保rows不为nil
			if rows != nil {
				db.Statement.Dest = rows
			} else {
				_ = db.AddError(gorm.ErrRecordNotFound)
			}
		})
	} else {
		db.Callback().Query().Replace("gorm:query", callback)
	}

	// 注册更新回调
	if callback := db.Callback().Update().Get("gorm:update"); callback == nil {
		db.Callback().Update().Replace("gorm:update", func(db *gorm.DB) {
			if err := dialector.Update(context.Background(), db); err != nil {
				_ = db.AddError(err)
			}
		})
	} else {
		db.Callback().Update().Replace("gorm:update", callback)
	}

	// 注册删除回调
	if callback := db.Callback().Delete().Get("gorm:delete"); callback == nil {
		db.Callback().Delete().Replace("gorm:delete", func(db *gorm.DB) {
			if err := dialector.Delete(context.Background(), db); err != nil {
				_ = db.AddError(err)
			}
		})
	} else {
		db.Callback().Delete().Replace("gorm:delete", callback)
	}
}
