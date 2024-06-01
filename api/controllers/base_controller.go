package controllers

import (
	"database/sql"
	"encoding/json"
	"fmt"
	log "github.com/sirupsen/logrus"
	"go-mysql-elasticsearch/common"
	"go-mysql-elasticsearch/config"
	"go-mysql-elasticsearch/services"
	"math"
	"reflect"
	"sync"
	"time"
)

type IController interface {
	Chunk()
	Update()
	Delete()
	CCR()
}

type BaseController struct {
	ActionArr [3]string
	Ids       string
	TimeRange [2]int64
	ResumeId  int64
	// 索引名
	ActionName string
	// 带日期索引名
	IndexName string
	// 索引别名
	AliasName   string
	BaseService services.IService
	Conn        *sql.DB
}

// 执行向elasticsearch写入
func (b *BaseController) PerformES(writeType common.WriteType, period int64) {
	var (
		sqlChan          = make(chan string, config.Config.MysqlConfig.SqlNumbers)
		dataChan         = make(chan []interface{}, config.Config.MysqlConfig.Perpage)
		wg               = &sync.WaitGroup{}
		counter    int32 = 1
		writeIndex string
	)

	switch writeType {
	case common.Insert:
		wg.Add(1)
		go common.GenSqlStr(b.Conn, b.ActionName, b.Ids, b.TimeRange, b.ResumeId, config.Config.MysqlConfig.Perpage, config.Config.Period, sqlChan, config.Config.MysqlConfig.ChunkNumbers, false, &counter, wg)
		wg.Add(1)
		go common.SelectFromMysql(b.BaseService.SelectFromMysql, sqlChan, dataChan, config.Config.MysqlConfig.ChunkNumbers, false, &counter, wg)
		writeIndex = b.IndexName
		if b.Ids != "" || !common.BothZero(b.TimeRange) || b.ResumeId > 0 {
			writeIndex = b.AliasName
		}
		wg.Add(1)
		go common.WriteToES(writeIndex, dataChan, config.Config.ESConfig.ESNumbers, common.Insert, &counter, wg)
		wg.Add(1)
		go common.WriteEnd(&counter, dataChan, wg)
		wg.Wait()
	case common.Upsert:
		wg.Add(1)
		go common.GenSqlStr(b.Conn, b.ActionName, b.Ids, b.TimeRange, b.ResumeId, config.Config.MysqlConfig.Perpage, period, sqlChan, config.Config.MysqlConfig.UpdateNumbers, true, &counter, wg)
		wg.Add(1)
		go common.SelectFromMysql(b.BaseService.SelectFromMysql, sqlChan, dataChan, config.Config.MysqlConfig.UpdateNumbers, true, &counter, wg)
		wg.Add(1)
		go common.WriteToES(b.AliasName, dataChan, config.Config.ESConfig.ESNumbers, common.Upsert, &counter, wg)
		wg.Add(1)
		go common.WriteEnd(&counter, dataChan, wg)
		wg.Wait()
	case common.UpField:
		wg.Add(1)
		go common.GenSqlStr(b.Conn, b.ActionName, b.Ids, b.TimeRange, b.ResumeId, config.Config.MysqlConfig.Perpage, period, sqlChan, config.Config.MysqlConfig.UpdateNumbers, false, &counter, wg)
		wg.Add(1)
		go common.SelectFromMysql(b.BaseService.SelectFromMysql, sqlChan, dataChan, config.Config.MysqlConfig.UpdateNumbers, false, &counter, wg)
		wg.Add(1)
		go common.WriteToES(b.IndexName, dataChan, config.Config.ESConfig.ESNumbers, common.UpField, &counter, wg)
		wg.Add(1)
		go common.WriteEnd(&counter, dataChan, wg)
		wg.Wait()
	case common.UpFieldUpdate:
		wg.Add(1)
		go common.GenSqlStr(b.Conn, b.ActionName, b.Ids, b.TimeRange, b.ResumeId, config.Config.MysqlConfig.Perpage, period, sqlChan, config.Config.MysqlConfig.UpdateNumbers, true, &counter, wg)
		wg.Add(1)
		go common.SelectFromMysql(b.BaseService.SelectFromMysql, sqlChan, dataChan, config.Config.MysqlConfig.UpdateNumbers, true, &counter, wg)
		wg.Add(1)
		go common.WriteToES(b.IndexName, dataChan, config.Config.ESConfig.ESNumbers, common.UpField, &counter, wg)
		wg.Add(1)
		go common.WriteEnd(&counter, dataChan, wg)
		wg.Wait()
	}
}

// 执行保存自定义词库
func (b *BaseController) PerformDict(writeType common.WriteType) {
	var (
		sqlChan        = make(chan string, config.Config.MysqlConfig.SqlNumbers)
		dataChan       = make(chan []interface{}, config.Config.MysqlConfig.Perpage)
		wg             = &sync.WaitGroup{}
		counter  int32 = 1
		rw       *sync.RWMutex
		isUpdate = false
	)
	rw = new(sync.RWMutex)

	if writeType == common.Upsert {
		isUpdate = true
	}

	wg.Add(1)
	go common.GenSqlStr(b.Conn, b.ActionName, b.Ids, b.TimeRange, b.ResumeId, config.Config.MysqlConfig.Perpage, config.Config.Period, sqlChan, config.Config.MysqlConfig.ChunkNumbers, isUpdate, &counter, wg)
	wg.Add(1)
	go common.SelectFromMysql(b.BaseService.SelectFromMysql, sqlChan, dataChan, config.Config.MysqlConfig.ChunkNumbers, isUpdate, &counter, wg)
	wg.Add(1)
	go common.DoingDict(b.ActionArr, dataChan, config.Config.MysqlConfig.UpdateNumbers, common.Insert, &counter, wg, rw)
	wg.Add(1)
	go common.WriteEnd(&counter, dataChan, wg)
	wg.Wait()
}

func (b *BaseController) Chunk() {
	var (
		startT       time.Time
		specifyAlias = common.GenSpecifyAlias(b.ActionArr)
		tc           time.Duration
		period       = int64(0)
	)
	startT = time.Now()
	log.Info(fmt.Sprintf("Starting:    %s %s Chunk start...\n", b.ActionArr[0], b.ActionArr[1]))

	// 当存在ids值或者时间范围不为0时,如果索引不存在,终止执行
	if b.Ids != "" || !common.BothZero(b.TimeRange) || b.ResumeId > 0 {
		if !common.IsExistsIndex(b.AliasName) {
			log.Error(fmt.Sprintf("Index %s not exist\n", b.AliasName))
			return
		}
	} else {
		// 检查索引是否存在，不存在则创建
		if !common.ExistsOrCreate(b.IndexName, "indices/"+b.ActionArr[1]+".json") {
			log.Error(fmt.Sprintf("Index %s is not exist and create index failed\n", b.IndexName))
			return
		}
		// 全量写入时,关闭索引刷新以提升写入性能
		log.Info(fmt.Sprintf("Refresh:    close refresh_interval for %s\n", b.IndexName))
		common.SetRefresh(b.IndexName, -1)
	}

	b.PerformES(common.Insert, period)

	tc = time.Since(startT) //计算耗时
	log.Info(fmt.Sprintf("Costing:     %s %s Chunk is over...,it costs %v\n", b.ActionArr[0], b.ActionArr[1], tc))

	// 当存在ids值或者时间范围不为0时即可返回
	if b.Ids != "" || !common.BothZero(b.TimeRange) || b.ResumeId > 0 {
		if b.ResumeId > 0 {
			goto SetIndex
		}
		return
	}

	// 数据创建完成修改索引别名
	if !common.SetAlias(b.IndexName, b.AliasName) {
		log.Panic(fmt.Sprintf("Add alias (%s) to (%s) failed!\n", b.AliasName, b.IndexName))
	}
	// 增加指定别名,多索引指向同一别名
	common.SetAlias(b.IndexName, specifyAlias)

SetIndex:
	// 开始补充写入时间段内丢失的更新
	period = int64(math.Ceil(tc.Seconds()))
	log.Info(fmt.Sprintf("Starting:    %s %s Update start...,in the past %v\n", b.ActionArr[0], b.ActionArr[1], tc))
	b.PerformES(common.Upsert, period)
	log.Info(fmt.Sprintf("Costing:     %s %s Update in the past %v over...\n", b.ActionArr[0], b.ActionArr[1], tc))

	// 修改索引分片数
	log.Info(fmt.Sprintf("Replicas:   set %d replicas for %s\n", config.Config.ESConfig.Replicas, b.AliasName))
	common.SetReplicas(b.AliasName, config.Config.ESConfig.Replicas)
	log.Info(fmt.Sprintf("Refresh:    set refresh_interval to %s for %s\n", config.Config.ESConfig.RefreshInterval, b.AliasName))
	common.SetRefresh(b.AliasName, config.Config.ESConfig.RefreshInterval)

	log.Info(fmt.Sprintf("Ending:      %s %s Chunk totally costs %v\n", b.ActionArr[0], b.ActionArr[1], time.Since(startT)))

	return
}

func (b *BaseController) Update() {
	var (
		ticker *time.Ticker
		startT time.Time
		tc     time.Duration
		period = config.Config.Period
	)
	ticker = time.NewTicker(time.Duration(config.Config.Refresh) * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			startT = time.Now()
			log.Info(fmt.Sprintf("Starting:     %s %s Update start...\n", b.ActionArr[0], b.ActionArr[1]))
			b.PerformES(common.Upsert, period)
			tc = time.Since(startT) //计算耗时
			log.Info(fmt.Sprintf("Costing:     %s %s Update is over...,it costs %v\n", b.ActionArr[0], b.ActionArr[1], tc))
		}
	}
}

func (b *BaseController) Delete() {
	if b.Ids == "" {
		if !common.DeleteIndices(b.AliasName) {
			log.Panic(fmt.Sprintf("Deleting:    Delete alias (%s) failed!\n", b.AliasName))
		}
	} else {
		if !common.DeleteByIds(b.AliasName, b.Ids) {
			log.Panic(fmt.Sprintf("Deleting:    Delete id (%s) from index %s failed!\n", b.Ids, b.AliasName))
		}
	}
}

func (b *BaseController) CCR() {
	var (
		err                                  error
		ok, flag, needReset                  bool
		leaderClusterName, tmpStr, indexName string
		leaderTransNodes, remoteSeeds        []string
		remoteSeedsIn                        []interface{}
		in                                   interface{}
		leaderSeedsMap, remoteSeedsMap       map[string]string
		remotes                              map[string]map[string][]string
		remoteInfo                           map[string]map[string]interface{}
		body                                 []byte
		followInfo                           common.FollowInfo
	)

	// 获取leader集群当前索引名称, 如果没有索引, 则无需创建follow
	if indexName, err = common.GetIndexNameByAlias(
		config.Config.ESConfig.Host,
		config.Config.ESConfig.Port,
		config.Config.ESConfig.Username,
		config.Config.ESConfig.Password,
		b.AliasName,
	); err != nil {
		log.Error(fmt.Sprintf("get index name in leader cluster failed: %s", err.Error()))
		return
	}

	// 获取leader集群节点配置信息
	if leaderClusterName, leaderTransNodes, err = common.GetNodesStats(); err != nil {
		log.Error(fmt.Sprintf("get leader cluster nodes stats error: %s", err.Error()))
		return
	}
	if len(leaderTransNodes) == 0 {
		log.Error(fmt.Sprintf("there is no transport node in leader cluster"))
		return
	}

	// 检查 follow 集群的 _cluster/settings 是否能连接到主集群上 (只判断永久设置)
	// 先获取follow的 _cluster/settings
	if remotes, err = common.GetClusterSettings(
		config.Config.ESConfig.ReplicaHost,
		config.Config.ESConfig.ReplicaPort,
		config.Config.ESConfig.Username,
		config.Config.ESConfig.Password); err != nil {
		log.Error(fmt.Sprintf("get follow cluster settings error: %s", err.Error()))
		return
	}
	if remotes == nil || len(remotes) == 0 {
		log.Error(fmt.Sprintf("get empty follow cluster settings"))
		return
	}
	// 再获取 follow 的 _remote/info
	if remoteInfo, err = common.GetRemoteInfo(
		config.Config.ESConfig.ReplicaHost,
		config.Config.ESConfig.ReplicaPort,
		config.Config.ESConfig.Username,
		config.Config.ESConfig.Password); err != nil {
		log.Error(fmt.Sprintf("get follow cluster remote info error: %s", err.Error()))
		return
	}
	if remoteInfo == nil || len(remoteInfo) == 0 {
		log.Error(fmt.Sprintf("get empty follow cluster remote info"))
		return
	}

	// 判断 follow 的 remote info 是否包含 leader 的cluster_name
	if _, ok = remoteInfo[leaderClusterName]; ok {
		if _, ok = remoteInfo[leaderClusterName]["connected"]; ok {
			flag, ok = remoteInfo[leaderClusterName]["connected"].(bool)
			// 如果存在,则暂存remote info 下的 seeds 留作比较
			if flag {
				if _, ok = remoteInfo[leaderClusterName]["seeds"]; ok {
					if remoteSeedsIn, ok = remoteInfo[leaderClusterName]["seeds"].([]interface{}); !ok {
						flag = false
						remoteSeeds = make([]string, 0)
					} else {
						flag = true
						remoteSeeds = make([]string, 0)
						for _, in = range remoteSeedsIn {
							remoteSeeds = append(remoteSeeds, in.(string))
						}
					}
				} else {
					flag = false
				}
			}
		} else {
			flag = false
		}
	} else {
		flag = false
	}

	if flag {
		// 比较 follow 的 remote seeds 和 leader 的 []transportAddress 是否完全一样
		remoteSeedsMap = make(map[string]string)
		for _, tmpStr = range remoteSeeds {
			remoteSeedsMap[tmpStr] = ""
		}
		leaderSeedsMap = make(map[string]string)
		for _, tmpStr = range leaderTransNodes {
			leaderSeedsMap[tmpStr] = ""
		}
		flag = reflect.DeepEqual(remoteSeedsMap, leaderSeedsMap)
	}

	// 如果flag为false, 则将重新设置 _cluster/settings 并验证 remote/info 的 connected
	if !flag {
		var (
			remoteSeedsMapSlice map[string][]string
			leaderMap           = make(map[string]interface{})
			remoteMap           = make(map[string]interface{})
			clusterMap          = make(map[string]interface{})
			clusterSettingsMap  = make(map[string]interface{})
		)
		remoteSeedsMapSlice = map[string][]string{"seeds": leaderTransNodes}
		leaderMap[leaderClusterName] = remoteSeedsMapSlice
		remoteMap["remote"] = leaderMap
		clusterMap["cluster"] = remoteMap
		clusterSettingsMap["persistent"] = clusterMap
		if body, err = json.Marshal(clusterSettingsMap); err != nil {
			log.Error(fmt.Sprintf("get map body err: %s", err.Error()))
			return
		}
		log.Info(fmt.Sprintf("Settings:    Put follow cluster settings: %s\n", string(body)))
		// 设置 cluster
		if err = common.PutClusterSettings(
			config.Config.ESConfig.ReplicaHost,
			config.Config.ESConfig.ReplicaPort,
			config.Config.ESConfig.Username,
			config.Config.ESConfig.Password, body); err != nil {
			log.Error(fmt.Sprintf("PutClusterSettings error occured: %s", err.Error()))
			return
		}
		// 再次确认 是否连接上
		if remoteInfo, err = common.GetRemoteInfo(
			config.Config.ESConfig.ReplicaHost,
			config.Config.ESConfig.ReplicaPort,
			config.Config.ESConfig.Username,
			config.Config.ESConfig.Password); err != nil {
			log.Error(fmt.Sprintf("get follow cluster remote info after put settings: %s", err.Error()))
			return
		}
		if remoteInfo == nil || len(remoteInfo) == 0 {
			log.Error(fmt.Sprintf("get empty follow cluster remote info"))
			return
		}
		if _, ok = remoteInfo[leaderClusterName]; !ok {
			log.Error(fmt.Sprintf("can not get follow cluster remote info for %s", leaderClusterName))
			return
		}
		if _, ok = remoteInfo[leaderClusterName]["connected"]; !ok {
			log.Error(fmt.Sprintf("get follow cluster remote info->connected failed for %s", leaderClusterName))
			return
		}
		if flag, ok = remoteInfo[leaderClusterName]["connected"].(bool); !ok || !flag {
			log.Error(fmt.Sprintf("follow cluster can not connected to leader cluster %s", leaderClusterName))
			return
		}
	}

	// 获取follow集群上指定索引的follow信息
	if followInfo, err = common.GetFollowInfoByIndex(
		config.Config.ESConfig.ReplicaHost,
		config.Config.ESConfig.ReplicaPort,
		config.Config.ESConfig.Username,
		config.Config.ESConfig.Password, indexName); err != nil {
		log.Error(fmt.Sprintf("GetFollowInfoByIndex error occured: %s", err.Error()))
		return
	}

	// 判断follow info 信息是否与当前的 follow_index, remote_cluster, leader_index, status 等
	// 是否需要重置follow信息
	needReset = true
	if followInfo.FollowerIndex == indexName &&
		followInfo.RemoteCluster == leaderClusterName &&
		followInfo.LeaderIndex == indexName {
		needReset = false
	}

	// 需要重设follow信息
	if needReset {
		log.Info(fmt.Sprintf("start to set %s follow info", indexName))
		// 先清除follow信息
		if err = common.CloseFollowByIndexName(
			config.Config.ESConfig.ReplicaHost,
			config.Config.ESConfig.ReplicaPort,
			config.Config.ESConfig.Username,
			config.Config.ESConfig.Password,
			indexName); err != nil {
			log.Error(fmt.Sprintf("close follow info failed: %s", err.Error()))
			return
		}

		// 再设置follow信息
		if err = common.SetFollowInfoByIndex(
			config.Config.ESConfig.ReplicaHost,
			config.Config.ESConfig.ReplicaPort,
			config.Config.ESConfig.Username,
			config.Config.ESConfig.Password,
			leaderClusterName, indexName); err != nil {
			log.Error(fmt.Sprintf("set follow info failed: %s", err.Error()))
			return
		} else {
			log.Info(fmt.Sprintf("set %s follow info success", indexName))
		}

		log.Info(fmt.Sprintf("set follow replicas for %s", indexName))
		// 设置follow的replicas数量
		if err = common.SetFollowReplicas(
			config.Config.ESConfig.ReplicaHost,
			config.Config.ESConfig.ReplicaPort,
			config.Config.ESConfig.Username,
			config.Config.ESConfig.Password,
			indexName, config.Config.ESConfig.FollowReplicas); err != nil {
			log.Error(fmt.Sprintf("set follow replicas failed: %s", err.Error()))
			return
		} else {
			log.Info(fmt.Sprintf("set follow replicas for %s success", indexName))
		}

	} else {
		log.Info(fmt.Sprintf("no need to set %s follow info", indexName))
	}
	log.Info(fmt.Sprintf("Ending:      %s %s CCR done", b.ActionArr[0], b.ActionArr[1]))
	return
}
