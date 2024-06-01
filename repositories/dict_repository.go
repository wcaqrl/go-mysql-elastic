package repositories

import (
	"database/sql"
	"go-mysql-elasticsearch/common"
	"go-mysql-elasticsearch/config"
	"go-mysql-elasticsearch/datamodels"
)

type DictManager struct {
	action    [3]string
	mysqlConn *sql.DB
}

func NewDictManager(action [3]string, db *sql.DB) IRepository {
	return &DictManager{action: action, mysqlConn: db}
}

func (d *DictManager) Conn() (err error) {
	if d.mysqlConn == nil {
		if d.mysqlConn, err = common.NewMysqlConn(config.Config.Dsn[d.action[0]+"_"+d.action[1]+"_dsn"]); err != nil {
			return err
		}
	}
	return
}

func (d *DictManager) SelectFromMysql(sql string) (dictArray []interface{}, err error) {
	var (
		result map[int]map[string]string
		dict   *datamodels.Dict
	)
	dictArray = make([]interface{}, 0)
	if err = d.Conn(); err != nil {
		return
	}
	rows, err := d.mysqlConn.Query(sql)
	if err != nil {
		return
	}
	defer rows.Close()
	result = common.GetResultRows(rows)
	if len(result) == 0 {
		return
	}
	for _, v := range result {
		dict = &datamodels.Dict{}
		common.DataToStructByTagSql(v, dict)
		dictArray = append(dictArray, dict)
	}
	return
}
