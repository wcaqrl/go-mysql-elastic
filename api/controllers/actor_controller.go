package controllers

import (
	"database/sql"
	log "github.com/sirupsen/logrus"
	"go-mysql-elasticsearch/common"
	"go-mysql-elasticsearch/config"
	"go-mysql-elasticsearch/repositories"
	"go-mysql-elasticsearch/services"
)

type ActorController struct {
	BaseController
}

var act *ActorController

func NewActorController(action [3]string, ids string, timeRange [2]int64, resumeId int64) *ActorController {
	var (
		db           *sql.DB
		err          error
		actor        repositories.IRepository
		actorService services.IService
	)
	if db, err = common.NewMysqlConn(config.Config.Dsn[action[0]+"_media_dsn"]); err != nil {
		log.Panic(err.Error())
	}
	actor = repositories.NewActorManager(action, db)
	actorService = services.NewActorService(actor)

	act = &ActorController{
		BaseController: BaseController{
			ActionArr:   action,
			Ids:         ids,
			TimeRange:   timeRange,
			ResumeId:    resumeId,
			ActionName:  common.GenActionName(action),
			IndexName:   common.GenIndexName(action, ids),
			AliasName:   common.GenAliasName(action),
			BaseService: actorService,
			Conn:        db,
		},
	}
	return act
}
