# InfluxDB3 GORM 驱动

这个项目提供了一个GORM驱动，用于连接和操作InfluxDB 3.x时序数据库。通过这个驱动，您可以使用熟悉的GORM API与InfluxDB进行交互。

## 功能特点

- 支持InfluxDB 3的SQL和时序数据操作
- 与GORM兼容，可以使用熟悉的API
- 支持标签和字段的映射
- 支持基本的CRUD操作

## 安装

```bash
go get -u github.com/xiabin827/influxdb3-gorm-driver
```

## 使用方法

### 连接到InfluxDB 3

```go
import (
    influxdb3gorm "github.com/xiabin827/influxdb3-gorm-driver"
    "gorm.io/gorm"
)

// 方法1: 使用DSN字符串连接
dsn := "host=http://localhost:8181 token=your_token database=your_database"
db, err := gorm.Open(influxdb3gorm.Open(dsn), &gorm.Config{})

// 方法2: 使用配置对象
import "github.com/xiabin/influxdb3-gorm-driver/dialector"

config := dialector.Config{
    Host:     "http://localhost:8181",
    Token:    "your_token",
    Database: "your_database",
}
db, err := gorm.Open(influxdb3gorm.New(config), &gorm.Config{})
```

### 定义模型

在InfluxDB中，数据模型与关系型数据库不同。我们使用tag和field标记来映射InfluxDB的数据结构：

```go
type Weather struct {
    ID          uint      `gorm:"primarykey"`
    Measurement string    `gorm:"column:_measurement"` // InfluxDB measurement名称
    Time        time.Time `gorm:"column:time"`         // 时间戳
    Location    string    `gorm:"column:location;type:tag"` // 标记为tag
    Temperature float64   `gorm:"column:temperature"`  // 温度字段
    Humidity    float64   `gorm:"column:humidity"`     // 湿度字段
}

// 自定义表名
func (Weather) TableName() string {
    return "weather_data"
}
```

### 插入数据

```go
weather := Weather{
    Measurement: "weather_data",
    Time:        time.Now(),
    Location:    "Beijing",
    Temperature: 25.5,
    Humidity:    60.0,
}

result := db.Create(&weather)
```

### 查询数据

```go
var weatherRecords []Weather
result := db.Where("location = ?", "Beijing").
    Where("time > ?", time.Now().Add(-24*time.Hour)).
    Order("time desc").
    Limit(10).
    Find(&weatherRecords)
```

## 注意事项

1. InfluxDB是时序数据库，其数据模型与关系型数据库有很大不同
2. 迁移功能有限，InfluxDB会在写入数据时自动创建表和列
3. 某些GORM功能在InfluxDB中可能不完全支持，如外键关系
4. tag字段自动索引，无需手动创建索引

## 当前限制

1. 驱动目前处于早期开发阶段，可能存在不稳定性
2. 某些高级查询功能可能需要直接使用SQL
3. 事务支持有限

## 许可证

MIT

## 贡献

欢迎提交问题和PR！ 