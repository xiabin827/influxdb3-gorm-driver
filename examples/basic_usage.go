package main

import (
	"fmt"
	"log"
	"time"

	influxdb3gorm "github.com/xiabin827/influxdb3-gorm-driver"
	"github.com/xiabin827/influxdb3-gorm-driver/dialector"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// UserAction 用户行为模型
type UserAction struct {
	User      string    `gorm:"column:user;type:tag"` // 使用tag标签指定为InfluxDB的tag
	Action    string    `gorm:"column:action;type:tag"`
	Value     float64   `gorm:"column:value"`
	Timestamp time.Time `gorm:"column:time"` // InfluxDB的时间戳字段
}

// TableName 表名
func (UserAction) TableName() string {
	return "user_action"
}

func main() {
	// 配置连接
	config := dialector.Config{
		Host:     "http://localhost:8181",
		Token:    "your_token",
		Database: "your_database",
	}

	// 打开数据库连接
	db, err := gorm.Open(influxdb3gorm.New(config), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Info), // 启用详细日志
	})
	if err != nil {
		log.Fatalf("连接数据库失败: %v", err)
	}

	// 写入数据
	err = writeData(db)
	if err != nil {
		log.Fatalf("写入数据失败: %v", err)
	}

	// 查询数据
	err = queryData(db)
	if err != nil {
		log.Fatalf("查询数据失败: %v", err)
	}
}

// 写入数据
func writeData(db *gorm.DB) error {
	// 创建一条记录
	action := UserAction{
		User:      "user1",
		Action:    "login",
		Value:     1.0,
		Timestamp: time.Now(),
	}

	// 使用Create方法写入
	result := db.Create(&action)
	if result.Error != nil {
		return fmt.Errorf("创建记录失败: %w", result.Error)
	}

	fmt.Printf("成功写入记录，影响行数: %d\n", result.RowsAffected)
	return nil
}

// 查询数据
func queryData(db *gorm.DB) error {
	// 使用结构体查询
	var actions []UserAction
	result := db.Where("user = ?", "user1").Find(&actions)
	if result.Error != nil {
		return fmt.Errorf("查询记录失败: %w", result.Error)
	}

	fmt.Printf("查询到 %d 条记录:\n", len(actions))
	for i, action := range actions {
		fmt.Printf("[%d] 用户: %s, 行为: %s, 值: %.2f, 时间: %s\n",
			i+1, action.User, action.Action, action.Value, action.Timestamp)
	}

	// 使用原始SQL查询
	var rawResults []map[string]interface{}
	result = db.Raw("SELECT * FROM user_action WHERE user = 'user1' LIMIT 10").Find(&rawResults)
	if result.Error != nil {
		return fmt.Errorf("原始SQL查询失败: %w", result.Error)
	}

	fmt.Printf("\n原始查询结果 (%d 行):\n", len(rawResults))
	for i, row := range rawResults {
		fmt.Printf("[%d] %v\n", i+1, row)
	}

	return nil
}
