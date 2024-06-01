package common

import (
	"errors"
	"fmt"
	"github.com/go-mysql-org/go-mysql/schema"
	log "github.com/sirupsen/logrus"
	"github.com/vmihailenco/msgpack"
	"go-mysql-elasticsearch/config"
	"reflect"
	"strconv"
	"strings"
	"sync"
)

var (
	RuleArray  = make([]*RuleMap, 0)
	ruleInsMap = make(map[string]*RuleMap)
	ruleRw     sync.RWMutex
)

type RuleMap struct {
	Database               string `json:"database"`
	Table                  string `json:"table"`
	ExcludeDatabase        string `json:"exclude_database"`
	ExcludeTable           string `json:"exclude_table"`
	IncludeColumns         string `json:"include_columns"`
	ExcludeColumns         string `json:"exclude_columns"`
	DateFormatter          string `json:"date_formatter"`
	DatetimeFormatter      string `json:"datetime_formatter"`
	StrategyType           string `json:"strategy_type"`
	RedisStructure         string `json:"redis_structure"`
	RedisKeyPrefix         string `json:"redis_key_prefix"`
	RedisKeyColumn         string `json:"redis_key_column"`
	RedisMasterKey         string `json:"redis_master_key"`
	RedisScoreColumn       string `json:"redis_score_column"`
	RedisListToSet         bool   `json:"redis_list_to_set"`
	RedisListToSetKey      string `json:"redis_list_to_set_key"`
	RedisListToSetLength   int64  `json:"redis_list_to_set_length"`
	RedisListToSetInterval int64  `json:"redis_list_to_set_interval"`
	Strategist             Strategy
	IsCompositeKey         bool
	RedisKeyColumnIndex    int
	RedisKeyColumnIndexs   []int
	RedisScoreColumnIndex  int
	TableInfo              *schema.Table
}

func InitRuleConfigs() {
	var (
		tmpMap     map[string]interface{}
		tmpRuleMap RuleMap
		tmpKey     string
		tmpInt64   int64
		i          int
		err        error
		ok         bool
		t          reflect.Type
		v          reflect.Value
		strMap     = map[string]string{
			"database":              "Database",
			"table":                 "Table",
			"exclude_database":      "ExcludeDatabase",
			"exclude_table":         "ExcludeTable",
			"include_columns":       "IncludeColumns",
			"exclude_columns":       "ExcludeColumns",
			"date_formatter":        "DateFormatter",
			"datetime_formatter":    "DatetimeFormatter",
			"strategy_type":         "StrategyType",
			"redis_structure":       "RedisStructure",
			"redis_key_prefix":      "RedisKeyPrefix",
			"redis_key_column":      "RedisKeyColumn",
			"redis_master_key":      "RedisMasterKey",
			"redis_score_column":    "RedisScoreColumn",
			"redis_list_to_set_key": "RedisListToSetKey",
		}
		intMap = map[string]string{
			"redis_list_to_set_length":   "RedisListToSetLength",
			"redis_list_to_set_interval": "RedisListToSetInterval",
		}
		boolMap = map[string]string{
			"redis_list_to_set": "RedisListToSet",
			//	"column_lower_case": "ColumnLowerCase",
			//	"column_upper_case": "ColumnUpperCase",
			//	"column_underscore_to_camel": "ColumnUnderscoreToCamel",
		}
	)
	for _, tmpMap = range config.Config.RuleConfig.RuleList {
		tmpRuleMap = RuleMap{}
		t = reflect.TypeOf(tmpRuleMap)
		v = reflect.ValueOf(&tmpRuleMap).Elem()
		for tmpKey = range tmpMap {
			for i = 0; i < t.NumField(); i++ {
				if t.Field(i).Name == strMap[tmpKey] {
					v.Field(i).SetString(tmpMap[tmpKey].(string))
				}
				if t.Field(i).Name == intMap[tmpKey] {
					if tmpInt64, err = strconv.ParseInt(tmpMap[tmpKey].(string), 10, 64); err != nil {
						log.Warn(fmt.Sprintf("Default:    %s will set to default value 0 as %s", tmpKey, err.Error()))
						tmpInt64 = 0
					}
					v.Field(i).SetInt(tmpInt64)
				}
				if t.Field(i).Name == boolMap[tmpKey] {
					if ok, err = strconv.ParseBool(tmpMap[tmpKey].(string)); err != nil {
						log.Warn(fmt.Sprintf("Default:    %s will set to false as %s", tmpKey, err.Error()))
					}
					v.Field(i).SetBool(ok)
				}
			}
		}
		RuleArray = append(RuleArray, &tmpRuleMap)
	}
	return
}

func RuleDeepClone(srcRule *RuleMap) (ruleMap *RuleMap, err error) {
	var bytes []byte
	if bytes, err = msgpack.Marshal(srcRule); err != nil {
		return
	}
	if err = msgpack.Unmarshal(bytes, &ruleMap); err != nil {
		return
	}
	return
}

func AddRuleIns(ruleKey string, ruleMap *RuleMap) {
	ruleRw.Lock()
	defer ruleRw.Unlock()
	ruleInsMap[ruleKey] = ruleMap
}

func DeleteRuleIns(ruleKey string) {
	ruleRw.Lock()
	defer ruleRw.Unlock()
	delete(ruleInsMap, ruleKey)
}

func RuleIns(ruleKey string) (ruleMap *RuleMap, ok bool) {
	ruleRw.RLock()
	defer ruleRw.RUnlock()
	ruleMap, ok = ruleInsMap[ruleKey]
	return
}

func RuleInsExist(ruleKey string) (ok bool) {
	ruleRw.RLock()
	defer ruleRw.RUnlock()
	_, ok = ruleInsMap[ruleKey]
	return
}

func RuleInsTotal() int {
	ruleRw.RLock()
	defer ruleRw.RUnlock()

	return len(ruleInsMap)
}

func (rm *RuleMap) Initialize() (err error) {
	if rm.DateFormatter != "" {
		rm.DateFormatter = ConvertGoFormat(rm.DateFormatter)
	}

	if rm.DatetimeFormatter != "" {
		rm.DatetimeFormatter = ConvertGoFormat(rm.DatetimeFormatter)
	}

	// 初始化策略
	if rm.Strategist, err = NewStrategy(rm.StrategyType); err != nil {
		return
	}

	err = rm.initRedisConfig()
	return
}

func (rm *RuleMap) AfterUpdateTableInfo() (err error) {
	err = rm.initRedisConfig()
	return
}

func (rm *RuleMap) TableColumn(field string) (*schema.TableColumn, int) {
	var (
		i int
		c schema.TableColumn
	)
	for i, c = range rm.TableInfo.Columns {
		if strings.ToUpper(c.Name) == strings.ToUpper(field) {
			return &c, i
		}
	}
	return nil, -1
}

func (rm *RuleMap) initRedisConfig() (err error) {
	var v, i int

	if rm.RedisStructure == "" {
		err = errors.New(fmt.Sprintf("empty redis_structure not allowed in rule"))
		return
	}

	switch strings.ToUpper(rm.RedisStructure) {
	case "STRING":
		rm.RedisStructure = "String"
	case "HASH":
		rm.RedisStructure = "Hash"
		if rm.RedisMasterKey == "" {
			err = errors.New("empty redis_master_key not allowed")
			return
		}
	case "LIST":
		rm.RedisStructure = "List"
		if rm.RedisMasterKey == "" {
			err = errors.New("empty redis_master_key not allowed in rule")
			return
		}
	case "SET":
		rm.RedisStructure = "Set"
		if rm.RedisMasterKey == "" {
			err = errors.New("empty redis_master_key not allowed in rule")
			return
		}
	case "SORTEDSET":
		rm.RedisStructure = "SortedSet"
		if rm.RedisMasterKey == "" {
			err = errors.New("empty redis_master_key not allowed in rule")
			return
		}
		// 是否设置了 score 列
		if rm.RedisScoreColumn == "" {
			rm.RedisScoreColumnIndex = -1
		} else {
			_, i = rm.TableColumn(rm.RedisScoreColumn)
			if i < 0 {
				err = errors.New("redis_score_column must be table column")
				return
			}
			rm.RedisScoreColumnIndex = i
		}
	default:
		err = errors.New("redis_structure must be in (string,hash,list,set,sortedset)")
		return
	}

	if rm.RedisKeyColumn == "" {
		if rm.IsCompositeKey {
			rm.RedisKeyColumnIndexs = []int{}
			for _, v = range rm.TableInfo.PKColumns {
				rm.RedisKeyColumnIndexs = append(rm.RedisKeyColumnIndexs, v)
			}
			rm.RedisKeyColumnIndex = -1
		} else {
			rm.RedisKeyColumnIndex = rm.TableInfo.PKColumns[0]
		}
	} else {
		_, i = rm.TableColumn(rm.RedisKeyColumn)
		if i < 0 {
			err = errors.New("redis_key_column must be table column")
			return
		}
		rm.RedisKeyColumnIndex = i
	}

	return
}
