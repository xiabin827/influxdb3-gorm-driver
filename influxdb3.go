package influxdb3gorm

import (
	"github.com/xiabin827/influxdb3-gorm-driver/dialector"
	"gorm.io/gorm"
)

// Open 打开InfluxDB3数据库连接
// dsn格式: "host=xxx token=xxx database=xxx"
func Open(dsn string) gorm.Dialector {
	return dialector.Open(dsn)
}

// New 使用配置创建新的InfluxDB3连接
func New(config dialector.Config) gorm.Dialector {
	return dialector.New(config)
}
