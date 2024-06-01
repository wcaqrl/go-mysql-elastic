package common

import (
	"errors"
	"fmt"
	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/schema"
	log "github.com/sirupsen/logrus"
	"go-mysql-elasticsearch/config"
	"go.uber.org/atomic"
	"regexp"
	"runtime"
	"strings"
	"sync"
	"time"
)

type TransferService struct {
	canal          *canal.Canal
	canalCfg       *canal.Config
	canalHandler   *handler
	canalEnable    atomic.Bool
	firstStart     atomic.Bool
	endpoint       Endpoint
	endpointEnable atomic.Bool
	positionDao    PositionStorage
	wg             sync.WaitGroup
	lockOfCanal    sync.Mutex
	loopStopSignal chan struct{}
}

var transferService *TransferService

func NewTransfer() (transferSrv *TransferService, err error) {
	transferSrv = &TransferService{
		loopStopSignal: make(chan struct{}, 1),
	}
	// 初始化Rules
	InitRuleConfigs()
	// 实例化canal
	if err = transferSrv.CreateCanal(); err != nil {
		return
	}
	// 组装rules
	if err = transferSrv.combineRules(); err != nil {
		return
	}
	// 添加database和table
	transferSrv.addDumpDatabaseOrTable()
	// 初始化position
	var positionDao PositionStorage
	// 初始化存储器
	if positionDao, err = NewPositionStorage(config.Config.StorageConfig); err != nil {
		return
	}
	if err = positionDao.InitPosition(); err != nil {
		return
	}
	transferSrv.positionDao = positionDao

	// 初始化终端
	var endpoint = &RedisEndpoint{}
	endpoint.client = redisDb
	transferSrv.endpoint = endpoint
	transferSrv.endpointEnable.Store(true)
	transferSrv.firstStart.Store(true)
	transferSrv.startLoop()

	transferService = transferSrv
	return
}

func (transferSrv *TransferService) CreateCanal() (err error) {
	transferSrv.canalCfg = canal.NewDefaultConfig()
	transferSrv.canalCfg.Addr = fmt.Sprintf("%s:%s", config.Config.MysqlConfig.Host, config.Config.MysqlConfig.Port)
	transferSrv.canalCfg.Password = config.Config.MysqlConfig.Password
	transferSrv.canalCfg.Charset = config.Config.MysqlConfig.Charset
	transferSrv.canalCfg.Flavor = config.Config.MysqlConfig.Flavor
	transferSrv.canalCfg.ServerID = uint32(config.Config.MysqlConfig.SlaveID)
	transferSrv.canalCfg.Dump.ExecutionPath = config.Config.MysqlConfig.DumpPath
	transferSrv.canalCfg.Dump.DiscardErr = false
	transferSrv.canalCfg.Dump.SkipMasterData = config.Config.MysqlConfig.SkipMasterData
	transferSrv.canal, err = canal.NewCanal(transferSrv.canalCfg)
	return
}

func (transferSrv *TransferService) combineRules() (err error) {
	var (
		ok                bool
		ruleKey, msg      string
		ruleCfg           *RuleMap
		tableMeta         *schema.Table
		dbs, ts           []string
		tmpIncludeRuleMap map[string]*RuleMap
		tmpExcludeRuleMap map[string]*RuleMap
		includeRuleMap    = make(map[string]*RuleMap)
		excludeRuleMap    = make(map[string]*RuleMap)
	)

	for _, ruleCfg = range RuleArray {
		if ruleCfg.Database == "" || ruleCfg.Table == "" {
			err = errors.New("database or table cannot be empty for rule")
			return
		}
		dbs = strings.Split(ruleCfg.Database, ",")
		ts = strings.Split(ruleCfg.Table, ",")
		if tmpIncludeRuleMap, err = transferSrv.arrangeRules(dbs, ts, ruleCfg, false); err != nil {
			return
		}
		dbs = strings.Split(ruleCfg.ExcludeDatabase, ",")
		ts = strings.Split(ruleCfg.ExcludeTable, ",")
		if tmpExcludeRuleMap, err = transferSrv.arrangeRules(dbs, ts, ruleCfg, true); err != nil {
			return
		}
		for ruleKey = range tmpIncludeRuleMap {
			if tmpIncludeRuleMap[ruleKey] != nil {
				includeRuleMap[ruleKey] = tmpIncludeRuleMap[ruleKey]
			}
		}
		for ruleKey = range tmpExcludeRuleMap {
			excludeRuleMap[ruleKey] = tmpExcludeRuleMap[ruleKey]
		}
	}

	// 将最后的差集添加到ruleInsMap
	for ruleKey = range includeRuleMap {
		if _, ok = excludeRuleMap[ruleKey]; !ok {
			if includeRuleMap[ruleKey] != nil {
				AddRuleIns(ruleKey, includeRuleMap[ruleKey])
				transferSrv.canalCfg.IncludeTableRegex = append(transferSrv.canalCfg.IncludeTableRegex, includeRuleMap[ruleKey].Database+"\\."+includeRuleMap[ruleKey].Table)
				log.Info(fmt.Sprintf("Receive:    Obtain changes from: %s.%s\n", includeRuleMap[ruleKey].Database, includeRuleMap[ruleKey].Table))
			}
		}
	}

	if len(ruleInsMap) == 0 {
		err = errors.New("there is no rule left")
		return
	}

	for ruleKey, ruleCfg = range ruleInsMap {
		if tableMeta, err = transferSrv.canal.GetTable(ruleCfg.Database, ruleCfg.Table); err != nil {
			return
		}
		if len(tableMeta.PKColumns) == 0 {
			// 不容忍没有主键的表
			msg = fmt.Sprintf("Careful:    Table %s.%s must have primary key", ruleCfg.Database, ruleCfg.Table)
			if !config.Config.MysqlConfig.SkipNoPkTable {
				err = errors.New(msg)
				return
			} else {
				// 清除该rule配置
				delete(ruleInsMap, ruleKey)
				log.Warn(msg)
				continue
			}
		}
		if len(tableMeta.PKColumns) > 1 {
			ruleCfg.IsCompositeKey = true // 组合主键
		}
		ruleCfg.TableInfo = tableMeta

		if err = ruleCfg.Initialize(); err != nil {
			return
		}
	}

	return
}

// 根据给定的 databases[无正则] 和 tables[包含正则] 组装 rule
func (transferSrv *TransferService) arrangeRules(databases, tables []string, ruleMap *RuleMap, isExclude bool) (rules map[string]*RuleMap, err error) {
	var (
		i                                                                                            int
		ruleKey, tmpDbName, tmpTableName, tmpFillDbName, tmpFillTName, sqlStr, schemaName, tableName string
		tmpDbMap                                                                                     map[string]bool
		result                                                                                       *mysql.Result
		newRule                                                                                      *RuleMap
	)
	rules = make(map[string]*RuleMap)
	tmpDbMap = make(map[string]bool)
	for _, tmpDbName = range databases {
		if regexp.QuoteMeta(tmpDbName) != tmpDbName {
			// 查出所有的数据库
			tmpFillDbName = tmpDbName
			if tmpDbName == "*" {
				tmpFillDbName = "." + tmpDbName
			}
			sqlStr = fmt.Sprintf("select schema_name from information_schema.schemata where schema_name rlike '%s'", tmpFillDbName)
			if result, err = transferSrv.canal.Execute(sqlStr); err != nil {
				return
			}
			for i = 0; i < result.Resultset.RowNumber(); i++ {
				if schemaName, err = result.GetString(i, 0); err != nil {
					return
				}
				tmpDbMap[schemaName] = true
			}

		} else {
			tmpDbMap[tmpDbName] = true
		}
	}

	// 遍历最终的database,获取对应的所有table
	for tmpDbName = range tmpDbMap {
		// 查询数据库下的表名
		for _, tmpTableName = range tables {
			if regexp.QuoteMeta(tmpTableName) != tmpTableName {
				tmpFillTName = tmpTableName
				if tmpTableName == "*" {
					tmpFillTName = "." + tmpTableName
				}
				sqlStr = fmt.Sprintf("select table_name from information_schema.tables where table_name rlike '%s' and table_schema = '%s'", tmpFillTName, tmpDbName)
				if result, err = transferSrv.canal.Execute(sqlStr); err != nil {
					return
				}
			} else {
				sqlStr = fmt.Sprintf("select table_name from information_schema.tables where table_name = '%s' and table_schema = '%s'", tmpTableName, tmpDbName)
				if result, err = transferSrv.canal.Execute(sqlStr); err != nil {
					return
				}
			}

			for i = 0; i < result.Resultset.RowNumber(); i++ {
				if tableName, err = result.GetString(i, 0); err != nil {
					return
				}
				if !isExclude {
					if newRule, err = RuleDeepClone(ruleMap); err != nil {
						return
					}
					newRule.Database = tmpDbName
					newRule.Table = tableName
				}
				ruleKey = RuleKey(tmpDbName, tableName)
				rules[ruleKey] = newRule
			}
		}
	}
	return
}

func (transferSrv *TransferService) addDumpDatabaseOrTable() {
	var (
		dbName, key string
		dbMap       = make(map[string]int)
		keys        []string
		tables      = make([]string, 0, RuleInsTotal())
		ruleMap     *RuleMap
	)

	for _, ruleMap = range ruleInsMap {
		dbName = ruleMap.Database // 原来有误, 写的 ruleMap.Table
		dbMap[ruleMap.Database] = 1
		tables = append(tables, ruleMap.Table)
	}
	if len(dbMap) == 1 {
		log.Info(fmt.Sprintf("Observe:    Listening %s's tables: %s\n", dbName, strings.Join(tables, ",")))
		transferSrv.canal.AddDumpTables(dbName, tables...)
	} else {
		keys = make([]string, 0, len(dbMap))
		for key = range dbMap {
			keys = append(keys, key)
		}
		log.Info(fmt.Sprintf("Observe:    Listening databases: %s\n", strings.Join(keys, ",")))
		transferSrv.canal.AddDumpDatabases(keys...)
	}
}

func (transferSrv *TransferService) updateRule(database, table string) (err error) {
	var (
		ok        bool
		msg       string
		ruleMap   *RuleMap
		tableMeta *schema.Table
	)
	ruleMap, ok = RuleIns(RuleKey(database, table))
	if ok {
		if tableMeta, err = transferSrv.canal.GetTable(database, table); err != nil {
			return
		}
		if len(tableMeta.PKColumns) == 0 {

			// 不容忍没有主键的表
			msg = fmt.Sprintf("Careful:    Table %s.%s must have primary key", database, table)
			if !config.Config.MysqlConfig.SkipNoPkTable {
				err = errors.New(msg)
				return
			} else {
				// 清除该rule配置
				delete(ruleInsMap, RuleKey(database, table))
				log.Warn(msg)
				return
			}
		}
		if len(tableMeta.PKColumns) > 1 {
			ruleMap.IsCompositeKey = true
		}

		ruleMap.TableInfo = tableMeta

		if err = ruleMap.AfterUpdateTableInfo(); err != nil {
			return err
		}
	}
	return
}

func (transferSrv *TransferService) startLoop() {
	go func() {
		var (
			err     error
			ticker  = time.NewTicker(time.Duration(config.Config.TransferInterval) * time.Second)
			sniffer = time.NewTicker(time.Duration(config.Config.SnifferInterval) * time.Second)
		)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if !transferSrv.endpointEnable.Load() {
					if err = transferSrv.endpoint.Ping(); err != nil {
						log.Error(fmt.Sprintf("endpoint not available: %s", err.Error()))
					} else {
						transferSrv.endpointEnable.Store(true)
						transferSrv.StartTransfer()
					}
				}
			case <-sniffer.C:
				log.Info(fmt.Sprintf("Sniffer:    There are %4d goroutines now\n", runtime.NumGoroutine()))
			case <-transferSrv.loopStopSignal:
				return
			}
		}
	}()
}

func (transferSrv *TransferService) StartTransfer() {
	transferSrv.lockOfCanal.Lock()
	defer transferSrv.lockOfCanal.Unlock()

	if transferSrv.firstStart.Load() {
		transferSrv.canalHandler = newHandler()
		transferSrv.canal.SetEventHandler(transferSrv.canalHandler)
		transferSrv.canalHandler.startListener()
		transferSrv.firstStart.Store(false)
		_ = transferSrv.run()
	} else {
		transferSrv.restart()
	}
}

func (transferSrv *TransferService) restart() {
	if transferSrv.canal != nil {
		transferSrv.canal.Close()
		transferSrv.wg.Wait()
	}
	log.Warn(fmt.Sprintf("Restart:  Here restart..."))
	transferSrv.CreateCanal()
	transferSrv.addDumpDatabaseOrTable()
	transferSrv.canalHandler = newHandler()
	transferSrv.canal.SetEventHandler(transferSrv.canalHandler)
	transferSrv.canalHandler.startListener()
	transferSrv.run()
}

func (transferSrv *TransferService) run() (err error) {
	var currentPos mysql.Position
	if currentPos, err = transferSrv.positionDao.Get(); err != nil {
		return
	}
	transferSrv.wg.Add(1)
	go func(p mysql.Position) {
		transferSrv.canalEnable.Store(true)
		log.Info(fmt.Sprintf("transfer run from position(%s %d)", p.Name, p.Pos))
		if err = transferSrv.canal.RunFrom(p); err != nil {
			log.Error(fmt.Sprintf("start transfer error: %v", err.Error()))
			if transferSrv.canalHandler != nil {
				transferSrv.canalHandler.stopListener()
			}
			transferSrv.canalEnable.Store(false)
		}
		log.Info("Canal is Closed")
		transferSrv.canalEnable.Store(false)
		transferSrv.canal = nil
		transferSrv.wg.Done()
	}(currentPos)

	// canal未提供回调, 停留一秒, 确保RunFrom启动成功
	time.Sleep(time.Second)
	return
}

func (transferSrv *TransferService) stopDump() {
	transferSrv.lockOfCanal.Lock()
	defer transferSrv.lockOfCanal.Unlock()

	if transferSrv.canal == nil {
		return
	}

	if !transferSrv.canalEnable.Load() {
		return
	}

	if transferSrv.canalHandler != nil {
		transferSrv.canalHandler.stopListener()
		transferSrv.canalHandler = nil
	}

	transferSrv.canal.Close()
	transferSrv.wg.Wait()

	log.Info(fmt.Sprintf("dumper stopped"))
}

func StartTransfer() {
	transferService.StartTransfer()
}

func CloseTransfer() {
	transferService.CloseTransfer()
}

func (transferSrv *TransferService) CloseTransfer() {
	transferSrv.stopDump()
	transferSrv.loopStopSignal <- struct{}{}
}

func (transferSrv *TransferService) GetPosition() (mysql.Position, error) {
	return transferSrv.positionDao.Get()
}
