package main

import (
	"fmt"
	"time"

	influxdb3gorm "github.com/xiabin827/influxdb3-gorm-driver"
	"github.com/xiabin827/influxdb3-gorm-driver/dialector"
	"gorm.io/gorm"
)

// Weather 天气测量数据模型
type Weather struct {
	ID          uint      `gorm:"primarykey"`
	Measurement string    `gorm:"column:_measurement"`      // InfluxDB measurement名称
	Time        time.Time `gorm:"column:time"`              // 时间戳
	Location    string    `gorm:"column:location;type:tag"` // 标记为tag
	Temperature float64   `gorm:"column:temperature"`       // 温度字段
	Humidity    float64   `gorm:"column:humidity"`          // 湿度字段
}

// TableName 自定义表名
func (Weather) TableName() string {
	return "weather_data"
}

func main() {
	// 使用DSN字符串连接
	dsn := "host=http://localhost:8181 token=your_token database=your_database"

	// 方法1: 使用DSN字符串
	db, err := gorm.Open(influxdb3gorm.Open(dsn), &gorm.Config{})
	if err != nil {
		panic("无法连接到数据库: " + err.Error())
	}

	// 方法2: 使用配置对象
	config := dialector.Config{
		Host:     "http://localhost:8181",
		Token:    "your_token",
		Database: "your_database",
	}

	db, err = gorm.Open(influxdb3gorm.New(config), &gorm.Config{})
	if err != nil {
		panic("无法连接到数据库: " + err.Error())
	}

	// 创建记录
	weather := Weather{
		Measurement: "weather_data",
		Time:        time.Now(),
		Location:    "Beijing",
		Temperature: 25.5,
		Humidity:    60.0,
	}

	// 插入数据
	result := db.Create(&weather)
	if result.Error != nil {
		fmt.Printf("创建记录失败: %v\n", result.Error)
	} else {
		fmt.Printf("创建记录成功，影响行数: %d\n", result.RowsAffected)
	}

	// 查询数据
	var weatherRecords []Weather
	result = db.Where("location = ?", "Beijing").
		Where("time > ?", time.Now().Add(-24*time.Hour)).
		Order("time desc").
		Limit(10).
		Find(&weatherRecords)

	if result.Error != nil {
		fmt.Printf("查询记录失败: %v\n", result.Error)
	} else {
		fmt.Printf("查询成功，找到 %d 条记录\n", len(weatherRecords))
		for _, record := range weatherRecords {
			fmt.Printf("时间: %v, 位置: %s, 温度: %.1f°C, 湿度: %.1f%%\n",
				record.Time, record.Location, record.Temperature, record.Humidity)
		}
	}
}
