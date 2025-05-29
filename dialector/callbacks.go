package dialector

import (
	"gorm.io/gorm"
)

// RegisterCallbacks 注册GORM回调
func (dialector *Dialector) RegisterCallbacks(db *gorm.DB) {
	// 注册创建回调
	if callback := db.Callback().Create().Get("gorm:create"); callback == nil {
		db.Callback().Create().Replace("gorm:create", dialector.Create)
	} else {
		db.Callback().Create().Replace("gorm:create", callback)
	}

	// 注册查询回调
	if callback := db.Callback().Query().Get("gorm:query"); callback == nil {
		db.Callback().Query().Replace("gorm:query", dialector.Query)
	} else {
		db.Callback().Query().Replace("gorm:query", callback)
	}

	// 注册更新回调
	if callback := db.Callback().Update().Get("gorm:update"); callback == nil {
		db.Callback().Update().Replace("gorm:update", dialector.Update)
	} else {
		db.Callback().Update().Replace("gorm:update", callback)
	}

	// 注册删除回调
	if callback := db.Callback().Delete().Get("gorm:delete"); callback == nil {
		db.Callback().Delete().Replace("gorm:delete", dialector.Delete)
	} else {
		db.Callback().Delete().Replace("gorm:delete", callback)
	}
}
