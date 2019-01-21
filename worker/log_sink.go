package worker

import (
	"context"
	"github.com/mongodb/mongo-go-driver/mongo"
	"go_projects/go_crontab_dist/go_crontab/common"
	"time"
)

// mongodb存储日志
type LogSink struct {
	client         *mongo.Client
	logCollection  *mongo.Collection
	logChan        chan *common.JobLog
	autoCommitChan chan *common.LogBatch
}

var (
	G_logSink *LogSink
)

// 批量写入日志
func (logSink *LogSink) saveLogs(batch *common.LogBatch) {
	logSink.logCollection.InsertMany(context.TODO(), batch.Logs)
}

// 日志存储协程
func (logSink *LogSink) writeLoop() {
	var (
		log          *common.JobLog
		logBatch     *common.LogBatch
		commitTimer  *time.Timer
		timeoutBatch *common.LogBatch
	)

	for {
		select {
		case log = <-logSink.logChan:
			if logBatch == nil {
				logBatch = &common.LogBatch{}

				// 让这个批次超时自动提交(1s)
				commitTimer = time.AfterFunc(
					time.Duration(G_config.JobLogCommitTimeout)*time.Millisecond,
					func(batch *common.LogBatch) func() {
						return func() {
							logSink.autoCommitChan <- batch
						}
					}(logBatch),
				)
			}

			// 将新日志追加到批次中
			logBatch.Logs = append(logBatch.Logs, log)

			// 如果批次满了，则立即发送
			if len(logBatch.Logs) >= G_config.JobLogBatchSize {
				// 发送日志
				logSink.saveLogs(logBatch)

				// 清空logBatch
				logBatch = nil

				// 取消定时器
				commitTimer.Stop()
			}
		case timeoutBatch = <-logSink.autoCommitChan: // 过期批次
			// 判断过期批次是否仍旧是当前的日志批次
			if timeoutBatch != logBatch {
				continue // 跳过已提交的批次
			}

			// 将批次写入mongodb
			logSink.saveLogs(timeoutBatch)

			// 清空logBatch
			logBatch = nil
		}
	}
}

// 初始化
func InitLogSink() (err error) {
	var (
		ctx    context.Context
		client *mongo.Client
	)

	// 建立mongodb连接
	ctx, _ = context.WithTimeout(context.Background(), time.Duration(G_config.MongodbConnectTimeout)*time.Millisecond)
	if client, err = mongo.Connect(ctx, G_config.MongodbUri); err != nil {
		return
	}

	// 选择db和collection
	G_logSink = &LogSink{
		client:         client,
		logCollection:  client.Database("cron").Collection("log"),
		logChan:        make(chan *common.JobLog, 1000),
		autoCommitChan: make(chan *common.LogBatch, 1000),
	}

	// 启动一个mongodb处理协程
	go G_logSink.writeLoop()

	return
}

// 发送日志
func (logSink *LogSink) Append(jobLog *common.JobLog) {
	select {
	case logSink.logChan <- jobLog:
	default:
		// 队列满了就丢弃
	}
}
