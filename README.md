# InfluxDB3 GORM 驱动

这个项目提供了一个GORM驱动，用于连接和操作InfluxDB 3.x时序数据库。通过这个驱动，您可以使用熟悉的GORM API与InfluxDB进行交互。

## 功能特点

- 支持InfluxDB 3的SQL和时序数据操作
- 与GORM兼容，可以使用熟悉的API
- 支持标签和字段的映射
- 支持基本的CRUD操作
- 增强的错误处理和空指针保护
- 支持自定义InfluxDB客户端配置
- 支持使用已有的InfluxDB客户端

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
import "github.com/xiabin827/influxdb3-gorm-driver/dialector"

config := dialector.Config{
    Host:     "http://localhost:8181",
    Token:    "your_token",
    Database: "your_database",
    // 可选配置
    DefaultStringSize: 256, // 字符串字段默认大小
    DefaultBinarySize: 1024, // 二进制字段默认大小
    DisableNanoTimestamps: false, // 是否禁用纳秒精度时间戳
}
db, err := gorm.Open(influxdb3gorm.New(config), &gorm.Config{})

// 方法3: 使用InfluxDB客户端配置
import "github.com/InfluxCommunity/influxdb3-go/v2/influxdb3"

clientConfig := influxdb3.ClientConfig{
    Host:     "http://localhost:8181",
    Token:    "your_token",
    Database: "your_database",
    // 其他InfluxDB客户端配置
}
db, err := gorm.Open(influxdb3gorm.NewWithConfig(clientConfig), &gorm.Config{})

// 方法4: 使用已有的InfluxDB客户端
client, err := influxdb3.New(clientConfig)
if err != nil {
    log.Fatalf("创建InfluxDB客户端失败: %v", err)
}
db, err := gorm.Open(influxdb3gorm.NewWithClient("your_database", client), &gorm.Config{})
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

### 使用 Map 接收查询结果

```go
var results []map[string]interface{}
result := db.Table("weather_data").
    Where("location = ?", "Beijing").
    Limit(10).
    Find(&results)

for _, row := range results {
    fmt.Printf("温度: %.2f, 湿度: %.2f\n", 
        row["temperature"].(float64), 
        row["humidity"].(float64))
}
```

### 高级查询

InfluxDB 3支持SQL查询，可以直接使用Raw方法执行：

```go
var results []map[string]interface{}
db.Raw(`SELECT mean("temperature") as avg_temp, 
       max("temperature") as max_temp 
       FROM weather_data 
       WHERE time > now() - interval '1 day' 
       GROUP BY time(1h), location`).Find(&results)
```

### 删除数据

```go
// 删除符合条件的数据
db.Where("location = ?", "Beijing").
   Where("time < ?", time.Now().Add(-30*24*time.Hour)).
   Delete(&Weather{})
```

## 错误处理

驱动包含内置的错误转换器，将InfluxDB特定错误转换为GORM错误：

```go
result := db.Where("non_existent_field = ?", "value").Find(&records)
if errors.Is(result.Error, gorm.ErrRecordNotFound) {
    // 处理记录未找到的情况
}
```

## 最佳实践

1. **始终检查错误**：所有操作后都应检查返回的错误
   ```go
   result := db.Find(&records)
   if result.Error != nil {
       log.Fatalf("查询失败: %v", result.Error)
   }
   ```

2. **使用结构体标签**：正确标记tag和field
   ```go
   type UserAction struct {
       User   string  `gorm:"column:user;type:tag"`  // 标记为tag
       Value  float64 `gorm:"column:value"`          // 默认为field
   }
   ```

3. **设置超时**：对于长时间运行的查询，设置上下文超时
   ```go
   ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
   defer cancel()
   result := db.WithContext(ctx).Find(&records)
   ```

4. **批量写入**：对于大量数据，使用批量写入提高性能
   ```go
   var weatherData []Weather
   // 添加多条数据
   result := db.CreateInBatches(weatherData, 100)
   ```

5. **使用合适的数据类型**：InfluxDB对于不同数据类型有优化
   ```go
   // 推荐的数据类型映射
   // Go bool -> InfluxDB BOOLEAN
   // Go int/uint -> InfluxDB INTEGER
   // Go float32/float64 -> InfluxDB DOUBLE
   // Go string -> InfluxDB STRING
   // Go time.Time -> InfluxDB TIMESTAMP
   // Go []byte -> InfluxDB BINARY
   ```

## 最近更新

### v0.3.0 (2023-10-15)
- 添加了更多配置选项，包括DefaultStringSize和DefaultBinarySize
- 添加了对现有InfluxDB客户端的支持
- 改进了错误处理和转换机制
- 优化了查询性能
- 扩展了迁移器功能

### v0.2.0 (2023-08-15)
- 修复了查询时可能导致空指针引用的问题
- 增强了错误处理和诊断信息
- 添加了连接验证功能
- 改进了类型转换机制
- 添加了更多使用示例

## 注意事项

1. InfluxDB是时序数据库，其数据模型与关系型数据库有很大不同
2. 迁移功能有限，InfluxDB会在写入数据时自动创建表和列
3. 某些GORM功能在InfluxDB中可能不完全支持，如外键关系
4. tag字段自动索引，无需手动创建索引
5. InfluxDB专为时序数据优化，对于非时间序列相关的操作效率可能较低

## 当前限制

1. 驱动目前处于早期开发阶段，可能存在不稳定性
2. 某些高级查询功能可能需要直接使用SQL
3. 事务支持有限
4. 更新操作需要删除旧数据并插入新数据，因为InfluxDB不支持原地更新

## 许可证

MIT

## 贡献

欢迎提交问题和PR！ 