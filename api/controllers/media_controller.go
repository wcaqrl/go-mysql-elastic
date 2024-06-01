package controllers

import (
	"database/sql"
	"fmt"
	log "github.com/sirupsen/logrus"
	"go-mysql-elasticsearch/common"
	"go-mysql-elasticsearch/config"
	"go-mysql-elasticsearch/repositories"
	"go-mysql-elasticsearch/services"
	"math"
	"time"
)

type MediaController struct {
	BaseController
}

var m *MediaController

func NewMediaController(action [3]string, ids string, timeRange [2]int64, resumeId int64) *MediaController {
	var (
		db           *sql.DB
		err          error
		media        repositories.IRepository
		mediaService services.IService
		actName      = common.GenActionName(action)
	)
	if db, err = common.NewMysqlConn(config.Config.Dsn[actName+"_dsn"]); err != nil {
		log.Panic(err.Error())
	}
	media = repositories.NewMediaManager(action, db)
	mediaService = services.NewMediaService(media)

	m = &MediaController{
		BaseController: BaseController{
			ActionArr:   action,
			Ids:         ids,
			TimeRange:   timeRange,
			ResumeId:    resumeId,
			ActionName:  actName,
			IndexName:   common.GenIndexName(action, ids),
			AliasName:   common.GenAliasName(action),
			BaseService: mediaService,
			Conn:        db,
		},
	}
	return m
}

func (m *MediaController) Chunk() {
	var (
		startT       time.Time
		specifyAlias = common.GenSpecifyAlias(m.ActionArr)
		tc           time.Duration
		period       = int64(0)
	)
	startT = time.Now()
	log.Info(fmt.Sprintf("Starting:   %s %s Chunk start...\n", m.ActionArr[0], m.ActionArr[1]))

	// 当存在ids值或者时间范围不为0时,如果索引不存在,终止执行
	if m.Ids != "" || !common.BothZero(m.TimeRange) || m.ResumeId > 0 {
		if !common.IsExistsIndex(m.AliasName) {
			log.Error(fmt.Sprintf("Index %s not exist\n", m.AliasName))
			return
		}
	} else {
		// 检查索引是否存在，不存在则创建
		// 2021年9月22日 核对 media.json 与 youku_media.json 字段, 决定只使用media
		if !common.ExistsOrCreate(m.IndexName, "indices/"+m.ActionArr[1]+".json") {
			log.Panic(fmt.Sprintf("Index %s not exist and create index failed\n", m.IndexName))
			return
		}
		// 全量写入时,关闭索引刷新以提升写入性能
		log.Info(fmt.Sprintf("Refresh:    close refresh_interval for %s\n", m.IndexName))
		common.SetRefresh(m.IndexName, -1)
	}

	m.PerformES(common.Insert, period)

	tc = time.Since(startT) //计算耗时
	log.Info(fmt.Sprintf("Costing:    %s %s Chunk is over...,it costs %v\n", m.ActionArr[0], m.ActionArr[1], tc))

	// 当存在ids值或者时间范围不为0时即可返回
	if m.Ids != "" || !common.BothZero(m.TimeRange) || m.ResumeId > 0 {
		if m.ResumeId > 0 {
			goto SetIndex
		}
		return
	}

	// 数据创建完成修改索引别名
	if !common.SetAlias(m.IndexName, m.AliasName) {
		log.Panic(fmt.Sprintf("Add alias (%s) to (%s) failed!\n", m.AliasName, m.IndexName))
	}
	// 增加指定别名,多索引指向同一别名
	common.SetAlias(m.IndexName, specifyAlias)

SetIndex:
	// 开始补充写入时间段内丢失的更新
	period = int64(math.Ceil(tc.Seconds()))
	log.Info(fmt.Sprintf("Starting:   %s %s Update start...,in the past %v\n", m.ActionArr[0], m.ActionArr[1], tc))
	m.PerformES(common.Upsert, period)
	log.Info(fmt.Sprintf("Costing:    %s %s Update in the past %v over...\n", m.ActionArr[0], m.ActionArr[1], tc))

	// 修改索引分片数
	log.Info(fmt.Sprintf("Replicas:   set %d replicas for %s\n", config.Config.ESConfig.Replicas, m.AliasName))
	common.SetReplicas(m.AliasName, config.Config.ESConfig.Replicas)
	log.Info(fmt.Sprintf("Refresh:    set refresh_interval to %s for %s\n", config.Config.ESConfig.RefreshInterval, m.AliasName))
	common.SetRefresh(m.AliasName, config.Config.ESConfig.RefreshInterval)

	log.Info(fmt.Sprintf("Ending:     %s %s Chunk totally costs %v\n", m.ActionArr[0], m.ActionArr[1], time.Since(startT)))

	return
}

func (m *MediaController) Update() {
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
			log.Info(fmt.Sprintf("Starting:   %s %s Update start...\n", m.ActionArr[0], m.ActionArr[1]))
			m.PerformES(common.Upsert, period)
			tc = time.Since(startT) //计算耗时
			log.Info(fmt.Sprintf("Costing:    %s %s Update is over...,it costs %v\n", m.ActionArr[0], m.ActionArr[1], tc))
		}
	}

}
