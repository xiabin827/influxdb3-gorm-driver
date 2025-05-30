package main

import (
	"context"
	"fmt"
	"testing"

	"github.com/InfluxCommunity/influxdb3-go/v2/influxdb3"
)

func TestDirectClientQuery(t *testing.T) {
	client, err := influxdb3.New(influxdb3.ClientConfig{
		Host:     "http://localhost:8181",
		Token:    "apiv3_sdgB9mevgBIKCSyDliGrASsMAEUVyoKjCUdlXoHn43tMkX3ATa0A1WwDetGng2EdaGX8HaSB1rAoVgmkVzQ9PA",
		Database: "user_actions",
	})
	if err != nil {
		t.Fatalf("无法创建客户端: %v", err)
	}
	defer client.Close()

	// 执行简单查询
	ctx := context.Background()
	query := "SELECT action_type, app_id_str, brand, custom_event, duration, region, sex, time, user_id FROM user_actions LIMIT 10"

	iterator, err := client.Query(ctx, query)
	if err != nil {
		t.Fatalf("查询失败: %v", err)
	}

	// 打印列信息
	columns := []string{}
	count := 0

	// 遍历结果
	for iterator.Next() {
		count++
		record := iterator.Value()

		// 第一条记录时，获取所有列名
		if count == 1 {
			for key := range record {
				columns = append(columns, key)
			}
			fmt.Printf("列名: %v\n", columns)
		}

		// 打印记录
		fmt.Printf("记录 %d: %v\n", count, record)
	}

	if err := iterator.Err(); err != nil {
		t.Fatalf("迭代过程中出错: %v", err)
	}

	fmt.Printf("共查询到 %d 条记录\n", count)
}
