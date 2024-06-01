package repositories

import (
	"database/sql"
	"fmt"
	log "github.com/sirupsen/logrus"
	"go-mysql-elasticsearch/common"
	"go-mysql-elasticsearch/config"
	"go-mysql-elasticsearch/datamodels"
	"strconv"
	"strings"
)

type ActorManager struct {
	action    [3]string
	mysqlConn *sql.DB
}

func NewActorManager(action [3]string, db *sql.DB) IRepository {
	return &ActorManager{action: action, mysqlConn: db}
}

func (act *ActorManager) Conn() (err error) {
	if act.mysqlConn == nil {
		if act.mysqlConn, err = common.NewMysqlConn(config.Config.Dsn[act.action[0]+"_media_dsn"]); err != nil {
			return err
		}
	}
	return
}

func (act *ActorManager) SelectFromMysql(sql string) (actorArray []interface{}, err error) {
	var (
		actorIDs       = make([]string, 0)
		countsMap      = make(map[int64]int64)
		result, counts map[int]map[string]string
		actor          *datamodels.Actor
		aID, count     int64
		ok             bool
	)
	actorArray = make([]interface{}, 0)
	if err = act.Conn(); err != nil {
		return
	}
	rows, err := act.mysqlConn.Query(sql)
	if err != nil {
		return
	}
	defer rows.Close()
	result = common.GetResultRows(rows)
	if len(result) == 0 {
		//log.Info(fmt.Sprintf("No Data:    %s",sql))
		return
	}
	for _, v := range result {
		actor = &datamodels.Actor{}
		actorIDs = append(actorIDs, v["actor_id"])
		common.DataToStructByTagSql(v, actor)
		actor.VersionID = common.Incr(config.Config.RedisConfig.VersionKey)
		actorArray = append(actorArray, actor)
	}

	if len(actorIDs) > 0 {
		sql = fmt.Sprintf("select count(m.media_id) as media_count,a.actor_id as actor_id from fm_media  m inner join fm_actor_media a on m.media_id=a.media_id where a.actor_id in (%s) and m.disable=0 group by a.actor_id", strings.Join(actorIDs, ","))
		if rows, err = act.mysqlConn.Query(sql); err != nil {
			log.Error(fmt.Sprintf("sqlselect failed--fm_media: %s", sql))
			return
		}
		defer rows.Close()
		counts = common.GetResultRows(rows)
		if len(counts) > 0 {
			for _, c := range counts {
				aID, _ = strconv.ParseInt(c["actor_id"], 10, 64)
				count, _ = strconv.ParseInt(c["media_count"], 10, 64)
				countsMap[aID] = count
			}
		}
	}

	for _, a := range actorArray {
		if _, ok = countsMap[a.(*datamodels.Actor).ActorID]; ok {
			a.(*datamodels.Actor).AvailableCount = countsMap[a.(*datamodels.Actor).ActorID]
		}
	}

	return
}
