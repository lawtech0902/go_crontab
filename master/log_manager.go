package master

import (
	"context"
	"github.com/mongodb/mongo-go-driver/mongo"
	"github.com/mongodb/mongo-go-driver/mongo/options"
	"go_projects/go_crontab_dist/go_crontab/common"
	"time"
)

// mongodb日志管理
type LogManager struct {
	client        *mongo.Client
	logCollection *mongo.Collection
}

var (
	G_logManager *LogManager
)

// 初始化
func InitLogManager() (err error) {
	var (
		client *mongo.Client
		ctx    context.Context
	)

	// 建立mongodb连接
	ctx, _ = context.WithTimeout(context.Background(), time.Duration(G_config.MongodbConnectTimeout))
	if client, err = mongo.Connect(ctx, G_config.MongodbUri); err != nil {
		return
	}

	G_logManager = &LogManager{
		client:        client,
		logCollection: client.Database("cron").Collection("log"),
	}

	return
}

// 查看任务日志
func (logManager *LogManager) ListLog(name string, skip int, limit int) (logArr []*common.JobLog, err error) {
	var (
		filter     *common.JobLogFilter
		logSort    *common.SortLogByStartTime
		cursor     mongo.Cursor
		findOpt    *options.FindOptions
		skipInt64  int64
		limitInt64 int64
		jobLog     *common.JobLog
	)

	logArr = make([]*common.JobLog, 0)
	skipInt64 = int64(skip)
	limitInt64 = int64(limit)

	// 过滤条件
	filter = &common.JobLogFilter{
		JobName: name,
	}

	// 按照任务开始时间倒排
	logSort = &common.SortLogByStartTime{
		SortOrder: -1,
	}

	// 使用cursor进行查询
	findOpt = &options.FindOptions{
		Sort:  logSort,
		Skip:  &skipInt64,
		Limit: &limitInt64,
	}
	if cursor, err = logManager.logCollection.Find(context.TODO(), filter, findOpt); err != nil {
		return
	}

	// 延迟释放cursor
	defer cursor.Close(context.TODO())

	for cursor.Next(context.TODO()) {
		jobLog = &common.JobLog{}

		// 反序列化BSON
		if err = cursor.Decode(jobLog); err != nil {
			continue // 有日志不合法
		}

		logArr = append(logArr, jobLog)
	}

	return
}
