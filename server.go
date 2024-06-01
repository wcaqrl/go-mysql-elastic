package main

import (
	"flag"
	"fmt"
	"github.com/go-mysql-org/go-mysql/mysql"
	log "github.com/sirupsen/logrus"
	"go-mysql-elasticsearch/api/controllers"
	"go-mysql-elasticsearch/common"
	"go-mysql-elasticsearch/config"
	"go-mysql-elasticsearch/logger"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"
)

func main() {
	var (
		h, job, yield                                                 bool
		action, ids, startTime, endTime, driver, filename, configFile string
		resumeId                                                      int64
		actionArr                                                     [3]string
		timeArray                                                     = [2]int64{0, 0}
		cfg                                                           *config.AppConfig
		numCpu                                                        int
		pos                                                           uint
		err                                                           error
	)

	flag.BoolVar(&h, "h", false, "this help")
	flag.StringVar(&action, "op", "", "input action you wanna perform")
	flag.StringVar(&ids, "ids", "", "input ids joined with comma")
	flag.StringVar(&startTime, "start_time", "", "input start_time as 2021-07-06 15:02:20")
	flag.StringVar(&endTime, "end_time", "", "input end_time as 2021-07-08 19:50:07, must greater than start_time")
	flag.Int64Var(&resumeId, "resume_id", 0, "input resume id, such as 100001, must be positive integer")
	flag.BoolVar(&yield, "yield", false, "yield from bin-log")
	flag.BoolVar(&job, "job", false, "boot up the job watcher")
	flag.StringVar(&driver, "driver", "redis", "choose a job driver as per your requirement")
	flag.StringVar(&configFile, "config", "app.ini", "input config filename, such as app.ini")
	flag.StringVar(&filename, "filename", "", "input mysql binlog file's name")
	flag.UintVar(&pos, "pos", 0, "assign a position for binlog file")
	flag.Parse()

	if h {
		flag.Usage = common.Usage
		flag.Usage()
		return
	}

	//初始化配置文件
	cfg = config.InitConfig(configFile)
	//初始化日志记录
	logger.InitLogger()
	numCpu = runtime.GOMAXPROCS(runtime.NumCPU())
	log.Info(fmt.Sprintf("You are running with %s mode, %s level, %d cpus\n", cfg.ENV, cfg.Level, numCpu))
	if cfg.ENV == "dev" {
		// 开发环境用来性能分析
		go func() {
			if err = http.ListenAndServe("0.0.0.0:6060", nil); err != nil {
				fmt.Printf("start pprof failed \n")
			}
		}()
	}

	// 如果是获取获取position
	if action == "position" {
		var (
			positionDao common.PositionStorage
			positioner  mysql.Position
		)
		if positionDao, err = common.NewPositionStorage(config.Config.StorageConfig); err != nil {
			return
		}

		// 如果 position 文件和位置 均不为空, 则认为是设置position
		if filename != "" && uint32(pos) > 0 {
			if err = positionDao.Save(mysql.Position{Name: filename, Pos: uint32(pos)}); err != nil {
				log.Error(fmt.Sprintf("Position:   Set position error:%s\n", err.Error()))
			} else {
				log.Info(fmt.Sprintf("Position:   Set new position %s %d\n", filename, pos))
			}
			return
		}

		// 获取当前position
		if positioner, err = positionDao.Get(); err != nil {
			log.Error(fmt.Sprintf("Position:   Get position error:%s\n", err.Error()))
			return
		}
		log.Info(fmt.Sprintf("Position:   Current position is: %s %d\n", positioner.Name, positioner.Pos))
		return
	}
	//初始化redis连接
	initRedis(cfg.RedisConfig)

	if yield {
		// 如果需要检查elasticsearch cpu
		if cfg.ESConfig.CpuInterval > 0 {
			initElasticsearch(cfg.ESConfig)
		}
		//初始化transfer
		if _, err = common.NewTransfer(); err != nil {
			log.Error(fmt.Sprintf("transfer initialize error:  %s", err.Error()))
			return
		}
		//启动transfer
		common.StartTransfer()
		//接收信号并关闭
		var (
			sin   os.Signal
			sTerm = make(chan os.Signal, 1)
		)
		signal.Notify(sTerm, os.Kill, os.Interrupt, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
		sin = <-sTerm
		log.Info(fmt.Sprintf("process stoped, under signal: %s ", sin.String()))
		common.CloseTransfer()
		common.CloseStorage()
		return
	}

	// 如果操作类型是 clear_version
	if action == "clear_version" {
		log.Info(fmt.Sprintf("Clear:      Clear version_id %d\n", common.Del([]string{config.Config.RedisConfig.VersionKey})))
		return
	}

	if strings.Contains(action, "log_parse") {
		if actionArr, err = common.GetActionSegments(action); err != nil {
			log.Error(fmt.Sprintf("%s\n", err.Error()))
			return
		}
		controllers.NewLogController(actionArr, "", [2]int64{0, 0}, 0).Parse()
		return
	}

	// 如果操作类型是日志搜集, 就将日志写到elasticsearch从服务器上
	if strings.Contains(action, "log_collect") {
		if cfg.ESConfig.ReplicaHost != "" && cfg.ESConfig.ReplicaPort != "" {
			cfg.ESConfig.Host = cfg.ESConfig.ReplicaHost
			cfg.ESConfig.Port = cfg.ESConfig.ReplicaPort
		}
	}

	//初始化elastic连接
	initElasticsearch(cfg.ESConfig)

	if job {
		WatchJob()
		return
	} else {
		if action == "" || !common.JudgeIds(ids) || resumeId < 0 {
			flag.Usage()
			return
		}

		// 判断时间范围传参是否合法
		if timeArray, err = common.GetTimeRange([2]string{startTime, endTime}); err != nil {
			log.Error(fmt.Sprintf("%s\n", err.Error()))
			return
		}

		if actionArr, err = common.GetActionSegments(action); err != nil {
			log.Error(fmt.Sprintf("%s\n", err.Error()))
			return
		}

		// 如果操作类型是日志搜集, 就将日志写到elasticsearch从服务器上
		if strings.Contains(action, "log_collect") {
			controllers.NewLogController(actionArr, "", [2]int64{0, 0}, 0).Collect()
			return
		}

		//判断是否支持action操作类型
		if !common.JudgeSupportAction(actionArr) {
			log.Error(fmt.Sprintf("can not support your operation"))
			flag.Usage = common.Usage
			flag.Usage()
			return
		}
		config.Config.JobChan <- struct{}{}
		invoke(config.Config.JobChan, actionArr, ids, timeArray, resumeId)
	}

	return
}

func invoke(jobChan chan struct{}, actionArr [3]string, ids string, timeArray [2]int64, resumeId int64) {
	var (
		err    error
		ei     interface{}
		refVs  []reflect.Value
		ctl    = *new(controllers.IController)
		CtlMap = map[string]interface{}{
			"video":       controllers.NewVideoController,
			"play":        controllers.NewPlayController,
			"anchor":      controllers.NewAnchorController,
			"media":       controllers.NewMediaController,
			"episode":     controllers.NewEpisodeController,
			"dict":        controllers.NewDictController,
			"topic":       controllers.NewTopicController,
			"actor":       controllers.NewActorController,
			"appexternal": controllers.NewAppExternalController,
			"vmsmedia":    controllers.NewVmsMediaController,
		}
	)

	defer func() {
		<-jobChan
		if ei = recover(); ei != nil {
			log.Error(fmt.Sprintf("invoke %+v  %s %+v error occured: %+v", actionArr, ids, timeArray, ei))
		}
	}()

	//判断是否支持action操作类型
	if !common.JudgeSupportAction(actionArr) {
		log.Error(fmt.Sprintf("can not support your operation"))
		return
	}
	//根据操作类型设置ids参数
	ids = common.SetIds(actionArr, ids)

	// 创建索引对应的控制器并调用对应的增删改方法
	if refVs, err = common.CallFunc(CtlMap, actionArr[1], actionArr, ids, timeArray, resumeId); err != nil {
		log.Panic(err.Error())
	}
	for _, r := range refVs {
		ctl = r.Interface().(controllers.IController)
	}
	switch actionArr[2] {
	case "":
		ctl.Chunk()
	case "update":
		ctl.Update()
	case "delete":
		ctl.Delete()
	case "ccr":
		ctl.CCR()
	}

	return
}

func WatchJob() {
	log.Info(fmt.Sprintf("now watcher the job: %s", config.Config.RedisConfig.JobKey))
	var wt = make(chan struct{}, 1)
	for {
		select {
		case <-wt:
			doJob()
		default:
			time.Sleep(time.Millisecond * 100)
			wt <- struct{}{}
		}
	}
}

func doJob() {
	var (
		err             error
		job             = ""
		actionName      = ""
		ids             = ""
		idx, retryTimes int
		ac              = ""
		actionArr       = [3]string{"", "", ""}
		tmpSlice        = make([]string, 0)
		tmpAtSlice      = make([]string, 0)
		jobSlice        = common.ZRangebyscore(config.Config.RedisConfig.JobKey, "-inf", "+inf", 0, 1)
	)
	for _, job = range jobSlice {
		log.Info(fmt.Sprintf("Get job:     %s", job))
		// 只要遍历到了任务,就从任务池中清除该任务
		if _, err = common.ZRem(config.Config.RedisConfig.JobKey, []interface{}{job}); err != nil {
			log.Panic(fmt.Sprintf("Clear:       job remove error: %s", job))
		}
		// 解析job  形如 lemon_video:123,456  | lemon_video_delete:789,678 | lemon_media:643@2 , @符号之后表示第N次重试
		tmpSlice = strings.Split(job, ":")
		if len(tmpSlice) != 2 {
			log.Error(fmt.Sprintf("NOP:         job format invalid: %s", job))
			continue
		}
		actionName = tmpSlice[0]
		tmpAtSlice = strings.Split(tmpSlice[1], "@")
		if len(tmpAtSlice) == 1 {
			ids = tmpAtSlice[0]
		} else if len(tmpAtSlice) == 2 {
			ids = tmpAtSlice[0]
			if retryTimes, err = strconv.Atoi(tmpAtSlice[1]); err != nil {
				log.Error(fmt.Sprintf("NOP:         job retry times invalid: %s", job))
				continue
			}
		} else {
			log.Error(fmt.Sprintf("NOP:         job second segement invalid: %s", job))
			continue
		}

		// 监听的任务队列属于增量消费任务队列,不允许没有id
		if ids == "" || !common.JudgeIds(ids) {
			log.Error(fmt.Sprintf("NOP:         job ids invalid: %s", ids))
			continue
		}
		for idx, ac = range strings.Split(actionName, "_") {
			if idx <= 2 {
				actionArr[idx] = ac
			} else {
				break
			}
		}
		// 判断操作是否合法
		if !common.JudgeSupportAction(actionArr) {
			log.Error(fmt.Sprintf("NOP:         job action invalid: %s", actionName))
			continue
		}
		// 监听消费任务只支持新增和删除
		if actionArr[2] != "" && actionArr[2] != "delete" {
			log.Error(fmt.Sprintf("NOP:         job action not support: %s", actionArr[2]))
			continue
		}
		config.Config.JobChan <- struct{}{}
		if retryTimes > 0 {
			log.Info(fmt.Sprintf("Retry:       %dth retry job: %s", retryTimes, job))
		} else {
			log.Info(fmt.Sprintf("Consume:     executing job: %s", job))
		}
		go invoke(config.Config.JobChan, actionArr, ids, [2]int64{0, 0}, 0)
	}
}

func initRedis(redisCfg config.RedisConfig) {
	var err error
	if _, err = common.NewRedisConn(redisCfg); err != nil {
		log.Panic(fmt.Sprintf("Connect to Redis error:  %s", err.Error()))
	} else {
		log.Info(fmt.Sprintf("Connect:    Connect to Redis success\n"))
	}
}

func initElasticsearch(esCfg config.ESConfig) {
	var err error
	if _, err = common.NewElasticConn(esCfg); err != nil {
		log.Panic(fmt.Sprintf("Connect to ElasticSearch error:  %s", err.Error()))
	} else {
		log.Info(fmt.Sprintf("Connect:    Connect to ElasticSearch success\n"))
	}
}
