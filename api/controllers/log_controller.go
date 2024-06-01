package controllers

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/hpcloud/tail"
	log "github.com/sirupsen/logrus"
	"go-mysql-elasticsearch/common"
	"go-mysql-elasticsearch/config"
	"go-mysql-elasticsearch/datamodels"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
)

type LogController struct {
	BaseController
}

var logCtl *LogController

func NewLogController(action [3]string, ids string, timeRange [2]int64, resumeId int64) *LogController {
	logCtl = &LogController{
		BaseController: BaseController{
			ActionArr:  action,
			Ids:        ids,
			TimeRange:  timeRange,
			ResumeId:   resumeId,
			ActionName: common.GenActionName(action),
			IndexName:  common.GenLogIndexName(action),
			AliasName:  common.GenAliasName(action),
		},
	}
	return logCtl
}

func (lCtl *LogController) Collect() {
	log.Info(fmt.Sprintf("Starting:   %s %s Chunk start...\n", lCtl.ActionArr[0], lCtl.ActionArr[1]))

	// 检查索引是否存在，不存在则创建
	if !common.ExistsOrCreate(lCtl.IndexName, "indices/"+lCtl.ActionArr[1]+".json") {
		log.Panic(fmt.Sprintf("Index %s not exist and create index failed\n", lCtl.IndexName))
		return
	}

	// 索引创建完成修改索引别名
	if !common.SetLogAlias(lCtl.IndexName, lCtl.AliasName) {
		log.Panic(fmt.Sprintf("Add alias (%s) to (%s) failed!\n", lCtl.AliasName, lCtl.IndexName))
	}
	log.Info(fmt.Sprintf("Alias:      Add alias (%s) to (%s) success\n", lCtl.AliasName, lCtl.IndexName))
	common.SetRefresh(lCtl.AliasName, config.Config.ESConfig.RefreshInterval)
	log.Info(fmt.Sprintf("Refresh:    set refresh_interval to %s for %s\n", config.Config.ESConfig.RefreshInterval, lCtl.AliasName))
	// 无数据时每隔1秒钟向redis请求一次, 从redis队列中获取日志记录
	var (
		err      error
		wg             = &sync.WaitGroup{}
		counter  int32 = 1
		wt             = make(chan struct{}, 1)
		dataChan       = make(chan []interface{}, config.Config.ESConfig.BulkNumber)
		logSlice []interface{}
	)

	wg.Add(1)
	go common.WriteToES(lCtl.AliasName, dataChan, config.Config.ESConfig.ESNumbers, common.Insert, &counter, wg)

	for {
		select {
		case <-wt:
			var msgStr string
			for {
				logSlice, err = lCtl.GetLogFromQueue(config.Config.ESConfig.BulkNumber)
				if len(logSlice) == 0 {
					msgStr = "no data"
					goto ENDFOR
				}
				if err != nil {
					msgStr = err.Error()
				}
				dataChan <- logSlice
			}
		ENDFOR:
			log.Info(fmt.Sprintf("Empty:      current get empty log from log queue: %s", msgStr))
		default:
			time.Sleep(time.Millisecond * 1000)
			wt <- struct{}{}
		}
	}
}

func (lCtl *LogController) Parse() {
	log.Info(fmt.Sprintf("Starting:   %s %s Parse start...\n", lCtl.ActionArr[0], lCtl.ActionArr[1]))
	var (
		err        error
		ok         bool
		bytes      []byte
		logCollect *datamodels.LogCollect
		logJsons   = make([]string, 0, config.Config.ESConfig.BulkNumber)
		tails      *tail.Tail
		line       *tail.Line
		logconf    = tail.Config{
			ReOpen:    true,                                 // 重新打开
			Follow:    true,                                 // 是否跟随
			Location:  &tail.SeekInfo{Offset: 0, Whence: 0}, // 从文件的哪个地方开始读
			MustExist: false,                                // 文件不存在不报错
			Poll:      true,
		}
		timer *time.Ticker
		rw    *sync.RWMutex
	)
	if tails, err = tail.TailFile(config.Config.LogConfig.Path, logconf); err != nil {
		log.Panic(fmt.Sprintf("tail file error occurred: %s", err.Error()))
	}
	timer = time.NewTicker(1 * time.Second)
	rw = new(sync.RWMutex)
	for {
		select {
		case line, ok = <-tails.Lines:
			if !ok {
				log.Error("tail file close reopen, filename:%s\n", tails.Filename)
				time.Sleep(time.Second)
				continue
			}
			//log.Info(fmt.Sprintf("%s\n", line.Text))
			log.Info(fmt.Sprintf("Parse log line sequentially ...\n"))
			// 调用文件行解析成 LogCollect
			if logCollect, err = lCtl.ParseLogLine(line.Text); err != nil {
				log.Error(fmt.Sprintf("%s\n", err.Error()))
				continue
			}
			// 如果解析的日志为 nil, 则忽略
			if logCollect == nil {
				continue
			}
			if bytes, err = json.Marshal(logCollect); err != nil {
				log.Error("marshal LogCollect error occurred: %s\n", err.Error())
				continue
			}
			rw.Lock()
			logJsons = append(logJsons, string(bytes))
			if len(logJsons) >= int(config.Config.ESConfig.BulkNumber) {
				// 推入redis然后置空
				common.Lpush(lCtl.AliasName, logJsons)
				logJsons = make([]string, 0, config.Config.ESConfig.BulkNumber)
			}
			rw.Unlock()
		case <-timer.C:
			log.Info(fmt.Sprintf("Check log line periodically\n"))
			rw.Lock()
			if len(logJsons) >= 0 {
				common.Lpush(lCtl.AliasName, logJsons)
				logJsons = make([]string, 0, config.Config.ESConfig.BulkNumber)
			}
			rw.Unlock()
		}
	}
}

func (lCtl *LogController) GetLogFromQueue(number int32) (logSlice []interface{}, err error) {
	var (
		i       int32
		jsonLog string
	)
	logSlice = make([]interface{}, 0)
	for i = 0; i < number; i++ {
		jsonLog, err = common.RPop(lCtl.AliasName)
		if jsonLog != "" {
			var logCollect = &datamodels.LogCollect{}
			if err = json.Unmarshal([]byte(jsonLog), &logCollect); err != nil {
				continue
			} else {
				logSlice = append(logSlice, logCollect)
			}
		}
	}
	return
}

func (lCtl *LogController) ParseLogLine(line string) (logCollect *datamodels.LogCollect, err error) {
	var (
		fields, strSlice, ipSlice []string
		local                     *time.Location
		theTime                   time.Time
		urlPath                   *url.URL
		uuidStr, realIpStr        string
		statusCode                int64
		timeFloat                 float64
		h                         = md5.New()
	)
	fields = strings.Split(line, "|")
	// 统一 nginx 日志字段有 14 个
	// $time_local | $host | $status | $request_length | $bytes_sent | $upstream_addr | $upstream_response_time | $http_referer | $remote_addr | $remote_user | $request | $http_user_agent | $http_x_forwarded_for | $http_fn
	if len(fields) != 14 {
		err = errors.New("nginx fields number not match")
		return
	}
	if local, err = time.LoadLocation("Local"); err != nil {
		return
	}
	if theTime, err = time.ParseInLocation("02/Jan/2006:15:04:05 +0800", strings.Trim(fields[0], " "), local); err != nil {
		return
	}
	strSlice = strings.Split(strings.Trim(fields[10], " "), " ")
	if len(strSlice) != 3 {
		err = errors.New("http request length not match")
		return
	}
	if urlPath, err = url.ParseRequestURI(strSlice[1]); err != nil {
		return
	}
	ipSlice = strings.Split(strings.Trim(fields[12], " "), ",")
	if len(ipSlice) > 0 {
		realIpStr = ipSlice[0]
	}
	if statusCode, err = strconv.ParseInt(strings.Trim(fields[2], " "), 10, 32); err != nil {
		return
	}
	if timeFloat, err = strconv.ParseFloat(strings.Trim(fields[6], " "), 64); err != nil {
		err = errors.New(fmt.Sprintf("parse response time error %s", err.Error()))
		return
	}
	// 仅搜集响应时长大于 200ms 的访问日志
	if timeFloat < (config.Config.LogConfig.ResponseTime / 1000) {
		return
	}
	h.Write([]byte(fmt.Sprintf("%d%s%s%s%s%s%s%s",
		theTime.UnixNano(), fields[1], strSlice[0], strSlice[1], fields[2], fields[6], fields[11], fields[12])))
	uuidStr = hex.EncodeToString(h.Sum(nil))

	logCollect = &datamodels.LogCollect{
		ID:           uuidStr,
		TheTime:      theTime,
		Domain:       strings.Trim(fields[1], " "),
		HttpMethod:   strSlice[0],
		RequestRoute: urlPath.Path,
		RequestParam: urlPath.RawQuery,
		Status:       int32(statusCode),
		ResponseTime: timeFloat * 1000, // 秒转化成毫秒
		UserAgent:    strings.Trim(fields[11], " "),
		RealIp:       realIpStr,
	}
	return
}
