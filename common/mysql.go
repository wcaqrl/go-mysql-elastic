package common

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	log "github.com/sirupsen/logrus"
	"go-mysql-elasticsearch/config"
	"go-mysql-elasticsearch/datamodels"
	"os"
	"reflect"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

//创建mysql 连接
func NewMysqlConn(dsn string) (db *sql.DB, err error) {
	var ok, flag bool
	if db, ok = config.Config.MysqlConfig.DbMap[dsn]; ok {
		if db != nil {
			if db.Ping() == nil {
				return
			}
		}
	}

	if !flag {
		if db, err = sql.Open("mysql", dsn); err != nil {
			log.Panic("Failed to Open mysql dsn,err: " + err.Error())
			os.Exit(1)
		}
		db.SetConnMaxLifetime(time.Duration(config.Config.MysqlConfig.MaxLifeTime) * time.Second)
		// db.SetMaxOpenConns(1000)
		if err = db.Ping(); err != nil {
			log.Panic("Failed to connect to mysql,err: " + err.Error())
			os.Exit(1)
		}
	}
	config.Config.MysqlConfig.DbMap[dsn] = db
	return
}

//获取所有
func GetResultRows(rows *sql.Rows) map[int]map[string]string {
	var (
		columns []string
		i       = 0
		result  = make(map[int]map[string]string)
	)
	//返回所有列
	columns, _ = rows.Columns()
	//这里表示一行所有列的值，用[]byte表示
	vals := make([][]byte, len(columns))
	//这里表示一行填充数据
	scans := make([]interface{}, len(columns))
	//这里scans引用vals，把数据填充到[]byte里
	for k, _ := range vals {
		scans[k] = &vals[k]
	}

	for rows.Next() {
		//填充数据
		rows.Scan(scans...)
		//每行数据
		row := make(map[string]string)
		//把vals中的数据复制到row中i
		for k, v := range vals {
			key := columns[k]
			//这里把[]byte数据转成string
			row[key] = string(v)
		}
		//放入结果集
		result[i] = row
		i++
	}
	return result
}

// 获取id值
func GetID(db *sql.DB, sql string) (id int64, err error) {
	var (
		result map[int]map[string]string
		tmpID  int64
	)
	//log.Info(fmt.Sprintf("id sql: %v",sql))
	rows, err := db.Query(sql)
	if err != nil {
		return
	}
	defer rows.Close()
	result = GetResultRows(rows)
	if len(result) == 0 {
		return
	}
	for _, v := range result {
		tmpID, _ = strconv.ParseInt(v["id"], 10, 64)
		id = tmpID
		break
	}
	return
}

//遍历生成sql语句
func GenSqlStr(db *sql.DB, action string, ids string, timeRange [2]int64, resumeId int64, perpage int32, period int64, sqlChan chan string, number chan struct{}, isUpdate bool, counter *int32, wg *sync.WaitGroup) {
	defer wg.Done()
	var (
		id          int64
		page        int32 = 2147483647
		err         error
		selectSql   string
		idSqlStr    string
		timestamp   = time.Now().Unix() - period
		patchStr    string
		table       string
		upSqlFormat string
		// 替换的select_sql中的子串成update_sql
		upSqlRegex = regexp.MustCompile("(id>=%d).*(order)")
	)

	table = config.Config.Indices[action]["table"]

	// 判断是否按id列表查询
	if isUpdate {
		if strings.Contains(action, "_episode") {
			upSqlFormat = ReplaceToUpdate(upSqlRegex, config.Config.Indices[action]["select_sql"], "and e.update_time>%d")
		} else {
			upSqlFormat = ReplaceToUpdate(upSqlRegex, config.Config.Indices[action]["select_sql"], "and update_time>%d")
		}
		selectSql = fmt.Sprintf(upSqlFormat, table, id, timestamp, perpage)
	} else {
		// 存在 ids 比 timeRange 优先级更高
		if ids != "" {
			selectSql = fmt.Sprintf(strings.Replace(
				config.Config.ESConfig.Indices[action]["select_sql"], ">=%d",
				" in ("+ids+") ", 1), table, perpage)
		} else if !BothZero(timeRange) {
			if strings.Contains(action, "_episode") {
				upSqlFormat = ReplaceToUpdate(upSqlRegex, config.Config.Indices[action]["select_sql"], "and (e.update_time between %d and %d)")
			} else {
				upSqlFormat = ReplaceToUpdate(upSqlRegex, config.Config.Indices[action]["select_sql"], "and (update_time between %d and %d)")
			}
			selectSql = fmt.Sprintf(upSqlFormat, table, id, timeRange[0], timeRange[1], perpage)
		} else {
			if resumeId > 0 {
				id = resumeId
			}
			selectSql = fmt.Sprintf(config.Config.Indices[action]["select_sql"], table, id, perpage)
		}
	}

	sqlChan <- selectSql
	//log.Info(fmt.Sprintf("SelectSql:  %v\n",selectSql))

	// 如果是按id列表查询,则无需再生成新的sql语句
	if !isUpdate {
		if ids != "" {
			// 传递恒为空的sql表示结束
			sqlChan <- ""
			return
		}
	}

	if config.Config.MysqlConfig.ESMAXPAGE > 0 {
		page = config.Config.MysqlConfig.ESMAXPAGE
	}
	for i := int32(1); ; i++ {
		patchStr = " and disable=0 "
		// topic,media,video,anchor,actor 索引不经disable=0过滤
		// 2021-10-20 再添加 play, appexternal 索引不经disable=0过滤
		if strings.Contains(action, "_topic") ||
			strings.Contains(action, "_media") ||
			strings.Contains(action, "_episode") ||
			strings.Contains(action, "_video") ||
			strings.Contains(action, "_anchor") ||
			strings.Contains(action, "_dispatch") ||
			strings.Contains(action, "_actor") ||
			strings.Contains(action, "_play") ||
			strings.Contains(action, "_appexternal") ||
			strings.Contains(action, "_dict") {
			patchStr = ""
		}
		if isUpdate {
			if strings.Contains(action, "_episode") {
				patchStr = " and e.update_time>" + strconv.FormatInt(timestamp, 10)
			} else {
				patchStr = " and update_time>" + strconv.FormatInt(timestamp, 10)
			}
		} else {
			if !BothZero(timeRange) {
				if strings.Contains(action, "_episode") {
					patchStr += fmt.Sprintf(" and (e.update_time between %d and %d) ", timeRange[0], timeRange[1])
				} else {
					patchStr += fmt.Sprintf(" and (update_time between %d and %d) ", timeRange[0], timeRange[1])
				}
			}
		}

		idSqlStr = fmt.Sprintf(config.Config.Indices[action]["id_sql"], table, id, patchStr, perpage)
		//log.Info(fmt.Sprintf("ID Sql:     %s\n",idSqlStr))
		number <- struct{}{}
		if id, err = GetID(db, idSqlStr); id != 0 {
			//log.Info(fmt.Sprintf("Get id: %6d from mysql,there are %4d goroutines now\n",id,runtime.NumGoroutine()))
		}
		<-number
		if err != nil {
			log.Panic(fmt.Sprintf("Get id from mysql error ocurred: %v\n", err.Error()))
		}
		if id == 0 || i >= page {
			// 传递恒为空的sql表示结束
			sqlChan <- ""
			//sqlChan <- "select id from (select 1 as id,0 as update_time) tmp where id>=9999999999 and update_time>2145888000"
			break
		}

		if isUpdate {
			selectSql = fmt.Sprintf(upSqlFormat, table, id, timestamp, perpage)
		} else {
			if !BothZero(timeRange) {
				selectSql = fmt.Sprintf(upSqlFormat, table, id, timeRange[0], timeRange[1], perpage)
			} else {
				selectSql = fmt.Sprintf(config.Config.Indices[action]["select_sql"], table, id, perpage)
			}
		}
		//log.Info(fmt.Sprintf("SelectSql:  %v\n",selectSql))
		sqlChan <- selectSql
		atomic.AddInt32(counter, 1)
	}

	log.Info(fmt.Sprintf("Finish:     Generate sql string finished \n"))
}

//从mysql表中取数据
func SelectFromMysql(f interface{}, in chan string, out interface{}, number chan struct{}, isUpdate bool, counter *int32, wg *sync.WaitGroup) {
	defer wg.Done()
	var (
		count               uint32
		idRegex             = regexp.MustCompile("id>=(\\d+)")
		idList              = regexp.MustCompile(" in \\((\\S*)\\)")
		upRegex             = regexp.MustCompile("update_time>(\\d+)")
		emptyMsg            = "Empty:       Get empty data from mysql\n"
		mysqlMsg            = "%6dth select from mysql error occured: %v\n"
		sqlStr, idStr       string
		upTime              int64
		fValue, resultValue reflect.Value
	)

	for {
		select {
		case sqlStr = <-in:
			if sqlStr == "" {
				return
			}
			number <- struct{}{}
			wg.Add(1)
			go func(sqlStr string, number chan struct{}) {
				defer wg.Done()
				atomic.AddUint32(&count, 1)
				if isUpdate {
					upTime, _ = strconv.ParseInt(upRegex.FindStringSubmatch(sqlStr)[1], 10, 64)
					log.Info(fmt.Sprintf("Update: %6dth get from mysql,update time: %s,there are %4d goroutines now\n", count, time.Unix(upTime, 0).In(time.FixedZone("CST", 8*3600)).Format("2006-01-02 15:04:05"), runtime.NumGoroutine()))
				} else {
					if idRegex.Match([]byte(sqlStr)) {
						idStr = idRegex.FindStringSubmatch(sqlStr)[1]
					} else {
						idStr = idList.FindStringSubmatch(sqlStr)[1]
					}
					log.Info(fmt.Sprintf("Select: %6dth get from mysql,id: %6s,there are %4d goroutines now\n", count, idStr, runtime.NumGoroutine()))
				}
				//log.Info(fmt.Sprintf("Select Sql: %v\n",sqlStr))
				fValue = reflect.ValueOf(f)
				if fValue.Kind() != reflect.Func {
					log.Panic(fmt.Sprintf("not func\n"))
				}
				switch fValue.Interface().(type) {
				case func(string) ([]interface{}, error):
					results, err := f.(func(string) ([]interface{}, error))(sqlStr)
					if err != nil {
						log.Panic(fmt.Sprintf(mysqlMsg, count, err.Error()))
					} else {
						resultValue = reflect.ValueOf(results)
						if resultValue.Kind() != reflect.Slice {
							log.Panic(fmt.Sprintf("not slice\n"))
						}
						if resultValue.Len() == 0 {
							log.Info(fmt.Sprintf(emptyMsg))
							atomic.AddInt32(counter, -1)
						} else {
							out.(chan []interface{}) <- results
						}
					}
				}
				<-number
			}(sqlStr, number)
		}
	}
}

// 查询channel频道信息
func GetChannelMap(db *sql.DB, sql string) (channelMap map[int64]interface{}, err error) {
	var (
		channels  map[int]map[string]string
		channel   *datamodels.Channel
		channelID int64
	)
	channelMap = make(map[int64]interface{})
	//查出channel
	log.Info(fmt.Sprintf("Channel:    Get channel: %s\n", sql))
	rows, err := db.Query(sql)
	if err != nil {
		log.Error(fmt.Sprintf("select channel error occured: %s; sql: %s\n", err.Error(), sql))
		return
	}
	defer rows.Close()
	channels = GetResultRows(rows)
	if len(channels) > 0 {
		for _, v := range channels {
			channel = &datamodels.Channel{}
			DataToStructByTagSql(v, channel)
			channelID, _ = strconv.ParseInt(v["channel_id"], 10, 64)
			channelMap[channelID] = channel
		}
	}
	return
}

// 正则替换成update_sql
func ReplaceToUpdate(upSqlRegex *regexp.Regexp, sqlStr, replaceStr string) string {
	// 找出 sql 中的替换子串
	var matches = make([]string, 0)
	matches = upSqlRegex.FindStringSubmatch(sqlStr)
	if len(matches) != 3 {
		return ""
	}
	return upSqlRegex.ReplaceAllString(sqlStr, "$1 "+replaceStr+" $2")
}

func DoingDict(action [3]string, in chan []interface{}, number chan struct{}, writeType WriteType, counter *int32, wg *sync.WaitGroup, rw *sync.RWMutex) {
	defer wg.Done()
	var (
		err       error
		db        *sql.DB
		count     uint32
		v         reflect.Value
		val       interface{}
		dictDatas []*datamodels.Dict
	)
	if db, err = NewMysqlConn(config.Config.Dsn[GenActionName(action)+"_dsn"]); err != nil {
		log.Panic(err.Error())
	}
	for {
		v = reflect.ValueOf(in)
		if v.Kind() != reflect.Chan {
			log.Panic(fmt.Sprintf("not chan\n"))
		}
		val = v.Interface()
		switch val.(type) {
		case chan []interface{}:
			datas := <-val.(chan []interface{})
			if len(datas) == 0 {
				return
			}
			// 将数据类型转成Dict
			if dictDatas, err = TransferToDict(datas); err != nil {
				continue
			}
			number <- struct{}{}
			atomic.AddUint32(&count, 1)
			wg.Add(1)
			go WriteMysqlDict(action, db, dictDatas, number, count, writeType, counter, wg, rw)
		}
	}
}

func WriteMysqlDict(action [3]string, db *sql.DB, datas []*datamodels.Dict, number chan struct{}, count uint32, writeType WriteType, counter *int32, wg *sync.WaitGroup, rw *sync.RWMutex) {
	defer wg.Done()
	var err error
	switch writeType {
	case Insert:
		err = InsertDict(action, db, datas, rw)
	}
	if err != nil {
		log.Error(fmt.Sprintf("%6dth write into mysql failed: %v\n", count, err.Error()))
	}
	log.Info(fmt.Sprintf("Writing:%6dth write %4d datas into mysql,there are %4d goroutines now\n", count, reflect.ValueOf(datas).Len(), runtime.NumGoroutine()))
	atomic.AddInt32(counter, -1)
	<-number
}

func InsertDict(action [3]string, db *sql.DB, dictArray []*datamodels.Dict, rw *sync.RWMutex) (err error) {
	var (
		dict         *datamodels.Dict
		res          sql.Result
		affectedRows int64
		table        = config.Config.ESConfig.Indices[GenActionName(action)]["table"]
		stmt         = "insert ignore into %s (word,update_time,type,status) values"
	)
	for _, dict = range dictArray {
		if dict.Word != "" {
			stmt = stmt + " ('" + strings.Replace(dict.Word, "'", "\\'", -1) + "'," + strconv.FormatInt(dict.UpdateTime, 10) + ",1,1),"
		}
	}
	stmt = fmt.Sprintf(strings.TrimRight(stmt, ","), table)
	// 异步写入数据库加锁
	rw.Lock()
	defer rw.Unlock()
	if res, err = db.Exec(stmt); err != nil {
		log.Error(fmt.Sprintf("insert into %s failed: %v\n", table, err.Error()))
		return
	}
	affectedRows, _ = res.RowsAffected()
	log.Info(fmt.Sprintf("insert into %s success: %v\n", table, strconv.FormatInt(affectedRows, 10)))
	return
}
