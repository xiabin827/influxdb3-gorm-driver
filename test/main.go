package main

import (
	"fmt"
	"time"

	influxdb3gorm "github.com/xiabin827/influxdb3-gorm-driver"
	"github.com/xiabin827/influxdb3-gorm-driver/dialector"
	"gorm.io/gorm"
)

// UserAction 定义用户行为数据模型
type UserAction struct {
	ActionType  int       `gorm:"column:action_type"`
	AppIDStr    string    `gorm:"column:app_id_str"`
	Brand       string    `gorm:"column:brand"`
	CustomEvent string    `gorm:"column:custom_event"`
	Duration    int       `gorm:"column:duration"`
	Region      string    `gorm:"column:region"`
	Sex         string    `gorm:"column:sex"`
	Time        time.Time `gorm:"column:time"`
	UserID      string    `gorm:"column:user_id"`
}

func main() {
	config := dialector.Config{
		Host:     "http://localhost:8181",
		Token:    "apiv3_sdgB9mevgBIKCSyDliGrASsMAEUVyoKjCUdlXoHn43tMkX3ATa0A1WwDetGng2EdaGX8HaSB1rAoVgmkVzQ9PA",
		Database: "user_actions",
	}
	db, err := gorm.Open(influxdb3gorm.New(config), &gorm.Config{})
	if err != nil {
		panic(err)
	}

	fmt.Println("开始查询...")

	// 使用Find方法代替Raw+Scan
	var results []UserAction
	if err := db.Table("user_actions").Limit(10).Find(&results).Error; err != nil {
		fmt.Println("查询错误:", err)
		return
	}

	fmt.Printf("查询结果数量: %d\n", len(results))

	// 打印第一条结果(如果有)
	if len(results) > 0 {
		fmt.Printf("第一条记录: %+v\n", results[0])
	}
}
