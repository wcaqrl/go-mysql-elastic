package controllers

import (
	"database/sql"
	"fmt"
	log "github.com/sirupsen/logrus"
	"go-mysql-elasticsearch/common"
	"go-mysql-elasticsearch/config"
	"go-mysql-elasticsearch/repositories"
	"go-mysql-elasticsearch/services"
	"strconv"
	"sync"
	"time"
)

type DictController struct {
	BaseController
	MediaCtl, ActorCtl, AnchorCtl *BaseController
}

var d *DictController

func NewDictController(action [3]string, ids string, timeRange [2]int64, resumeId int64) *DictController {
	var (
		err                                                    error
		dictDb, mediaDb, actorDb, anchorDb                     *sql.DB
		dict, media, actor, anchor                             repositories.IRepository
		dictService, mediaService, actorService, anchorService services.IService
		mediaCtl, actorCtl, anchorCtl                          *BaseController
		actionName                                             = common.GenActionName(action)
	)

	if dictDb, err = common.NewMysqlConn(config.Config.Dsn[actionName+"_dsn"]); err != nil {
		log.Panic(err.Error())
	}
	dict = repositories.NewDictManager(action, dictDb)
	dictService = services.NewDictService(dict)

	if mediaDb, err = common.NewMysqlConn(config.Config.Dsn[action[0]+"_media_dsn"]); err != nil {
		log.Panic(err.Error())
	}
	media = repositories.NewMediaManager(action, mediaDb)
	mediaService = services.NewMediaService(media)
	mediaCtl = &BaseController{
		ActionArr:   action,
		Ids:         ids,
		TimeRange:   timeRange,
		ResumeId:    resumeId,
		ActionName:  action[0] + "_media",
		BaseService: mediaService,
		Conn:        mediaDb,
	}

	if actorDb, err = common.NewMysqlConn(config.Config.Dsn[action[0]+"_media_dsn"]); err != nil {
		log.Panic(err.Error())
	}
	actor = repositories.NewActorManager(action, actorDb)
	actorService = services.NewActorService(actor)
	actorCtl = &BaseController{
		ActionArr:   action,
		Ids:         ids,
		TimeRange:   timeRange,
		ResumeId:    resumeId,
		ActionName:  action[0] + "_actor",
		BaseService: actorService,
		Conn:        actorDb,
	}

	if anchorDb, err = common.NewMysqlConn(config.Config.Dsn[action[0]+"_video_dsn"]); err != nil {
		log.Panic(err.Error())
	}
	anchor = repositories.NewAnchorManager(action, anchorDb)
	anchorService = services.NewAnchorService(anchor)
	anchorCtl = &BaseController{
		ActionArr:   action,
		Ids:         ids,
		TimeRange:   timeRange,
		ResumeId:    resumeId,
		ActionName:  action[0] + "_anchor",
		BaseService: anchorService,
		Conn:        anchorDb,
	}

	d = &DictController{
		BaseController: BaseController{
			ActionArr:   action,
			Ids:         ids,
			TimeRange:   timeRange,
			ResumeId:    resumeId,
			ActionName:  actionName,
			BaseService: dictService,
			Conn:        dictDb,
		},
		MediaCtl:  mediaCtl,
		ActorCtl:  actorCtl,
		AnchorCtl: anchorCtl,
	}
	return d
}

func (d *DictController) Chunk() {
	var (
		startT           time.Time
		tc               time.Duration
		wg                     = &sync.WaitGroup{}
		counter          int32 = 1
		sqlChan                = make(chan string, config.Config.MysqlConfig.SqlNumbers)
		dataChan               = make(chan []interface{}, config.Config.MysqlConfig.Perpage)
		rw               *sync.RWMutex
		filePathFormat   = config.Config.ExecPath + "/dict/" + "%s" + d.ActionArr[0] + "_" + config.Config.SftpConfig.FullDictFile
		localFilePath    = fmt.Sprintf(filePathFormat, "")
		tmpLocalFilePath = fmt.Sprintf(filePathFormat, "tmp_")
		//remoteDir = config.Config.SftpConfig.RemoteDir
	)
	rw = new(sync.RWMutex)
	startT = time.Now()
	log.Info(fmt.Sprintf("Starting:    %s %s Chunk start...\n", d.ActionArr[0], d.ActionArr[1]))
	// 向mysql写入
	d.MediaCtl.PerformDict(common.Insert)
	d.ActorCtl.PerformDict(common.Insert)
	d.AnchorCtl.PerformDict(common.Insert)

	// 从mysql取出再写入文件保存
	common.DeleteFile(tmpLocalFilePath)
	wg.Add(1)
	go common.GenSqlStr(d.Conn, d.ActionName, "", d.TimeRange, d.ResumeId, config.Config.MysqlConfig.Perpage, config.Config.Period, sqlChan, config.Config.MysqlConfig.ChunkNumbers, false, &counter, wg)
	wg.Add(1)
	go common.SelectFromMysql(d.BaseService.SelectFromMysql, sqlChan, dataChan, config.Config.MysqlConfig.ChunkNumbers, false, &counter, wg)
	wg.Add(1)
	go common.SaveDict(tmpLocalFilePath, dataChan, config.Config.MysqlConfig.UpdateNumbers, &counter, wg, rw)
	wg.Add(1)
	go common.WriteEnd(&counter, dataChan, wg)
	wg.Wait()

	// 如果生成的词库文件有内容,则替换旧文件
	common.ReplaceFile(localFilePath, tmpLocalFilePath)

	// 同步词库文件到ik插件
	//common.SyncFile(localFilePath,remoteDir)

	tc = time.Since(startT) //计算耗时
	log.Info(fmt.Sprintf("Ending:      %s %s Chunk is over...,it costs %v\n", d.ActionArr[0], d.ActionArr[1], tc))
	return
}

func (d *DictController) Update() {
	var (
		startT           time.Time
		tc               time.Duration
		wg                     = &sync.WaitGroup{}
		counter          int32 = 1
		sqlChan                = make(chan string, config.Config.MysqlConfig.SqlNumbers)
		dataChan               = make(chan []interface{}, config.Config.MysqlConfig.Perpage)
		rw               *sync.RWMutex
		filePathFormat   = config.Config.ExecPath + "/dict/" + "%s" + d.ActionArr[0] + "_" + config.Config.SftpConfig.AddDictFile
		localFilePath    = fmt.Sprintf(filePathFormat, "")
		tmpLocalFilePath = fmt.Sprintf(filePathFormat, "tmp_")
		//remoteDir = config.Config.SftpConfig.RemoteDir
	)
	rw = new(sync.RWMutex)
	startT = time.Now()
	log.Info(fmt.Sprintf("Starting:    %s %s Update start...\n", d.ActionArr[0], d.ActionArr[1]))
	// 向mysql写入
	d.MediaCtl.PerformDict(common.Upsert)
	d.ActorCtl.PerformDict(common.Upsert)
	d.AnchorCtl.PerformDict(common.Upsert)

	// 从mysql取出再写入文件保存
	common.DeleteFile(tmpLocalFilePath)
	wg.Add(1)
	go common.GenSqlStr(d.Conn, d.ActionName, "", d.TimeRange, d.ResumeId, config.Config.MysqlConfig.Perpage, config.Config.Period, sqlChan, config.Config.MysqlConfig.ChunkNumbers, true, &counter, wg)
	wg.Add(1)
	go common.SelectFromMysql(d.BaseService.SelectFromMysql, sqlChan, dataChan, config.Config.MysqlConfig.ChunkNumbers, true, &counter, wg)
	wg.Add(1)
	go common.SaveDict(tmpLocalFilePath, dataChan, config.Config.MysqlConfig.UpdateNumbers, &counter, wg, rw)
	wg.Add(1)
	go common.WriteEnd(&counter, dataChan, wg)
	wg.Wait()

	// 如果生成的词库文件有内容,则替换旧文件
	common.ReplaceFile(localFilePath, tmpLocalFilePath)

	// 同步词库文件到ik插件
	//common.SyncFile(localFilePath,remoteDir)

	tc = time.Since(startT) //计算耗时
	log.Info(fmt.Sprintf("Ending:      %s %s Update is over...,it costs %v\n", d.ActionArr[0], d.ActionArr[1], tc))
	return
}

func (d *DictController) Delete() {
	var (
		err          error
		res          sql.Result
		affectedRows int64
		table        = config.Config.ESConfig.Indices[d.ActionName]["table"]
		stmt         = "delete from % where id in (%s)"

		wg            = &sync.WaitGroup{}
		counter int32 = 1

		sqlChan          = make(chan string, config.Config.MysqlConfig.SqlNumbers)
		dataChan         = make(chan []interface{}, config.Config.MysqlConfig.Perpage)
		rw               *sync.RWMutex
		localFilePath    = config.Config.ExecPath + "/dict/" + d.ActionArr[0] + "_" + config.Config.SftpConfig.FullDictFile
		tmpLocalFilePath = config.Config.ExecPath + "/dict/" + "tmp_" + d.ActionArr[0] + "_" + config.Config.SftpConfig.FullDictFile
		//remoteDir = config.Config.SftpConfig.RemoteDir
	)
	stmt = fmt.Sprintf(stmt, table, d.Ids)
	if res, err = d.Conn.Exec(stmt); err != nil {
		log.Error(fmt.Sprintf("delete from %s failed: %v\n", table, err.Error()))
		return
	}
	affectedRows, _ = res.RowsAffected()
	log.Info(fmt.Sprintf("delete from %s success: %v\n", table, strconv.FormatInt(affectedRows, 10)))

	// 删除完成后再从词库中表取出数据重新生成词库文件
	// 从mysql取出再写入文件保存
	log.Info(fmt.Sprintf("now we redo file %s start...\n", localFilePath))
	wg.Add(1)
	go common.GenSqlStr(d.Conn, d.ActionName, "", d.TimeRange, d.ResumeId, config.Config.MysqlConfig.Perpage, config.Config.Period, sqlChan, config.Config.MysqlConfig.ChunkNumbers, false, &counter, wg)
	wg.Add(1)
	go common.SelectFromMysql(d.BaseService.SelectFromMysql, sqlChan, dataChan, config.Config.MysqlConfig.ChunkNumbers, false, &counter, wg)
	wg.Add(1)
	go common.SaveDict(tmpLocalFilePath, dataChan, config.Config.MysqlConfig.UpdateNumbers, &counter, wg, rw)
	wg.Add(1)
	go common.WriteEnd(&counter, dataChan, wg)
	wg.Wait()

	// 如果生成的词库文件有内容,则替换旧文件
	common.ReplaceFile(localFilePath, tmpLocalFilePath)

	log.Info(fmt.Sprintf("redo file %s over...\n", localFilePath))

	// 同步词库文件到ik插件
	//common.SyncFile(localFilePath,remoteDir)

	return
}
