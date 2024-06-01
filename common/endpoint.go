package common

import (
	"context"
	"errors"
	"fmt"
	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/schema"
	"github.com/go-redis/redis/v8"
	jsoniter "github.com/json-iterator/go"
	log "github.com/sirupsen/logrus"
	"strconv"
	"strings"
	"sync"
	"time"
)

var json = jsoniter.ConfigCompatibleWithStandardLibrary

const defaultDateFormatter = "2006-01-02"

type Endpoint interface {
	Connect() error
	Ping() error
	Consume([]*RowRequest) error
	Close()
}

type RedisEndpoint struct {
	isCluster bool
	client *redis.Client
	cluster *redis.ClusterClient
	retryLock sync.Mutex
}

func (rdbEpoint *RedisEndpoint) Consume(rowRequests []*RowRequest) (err error) {
	var (
		ok bool
		key, tmpKey string
		rowReq *RowRequest
		ruleMap *RuleMap
		pipe = rdbEpoint.pipe()
		rdbResp *RedisResponse
		tmpResp RedisResponse
		kvMap map[string]string
		kvArray map[string][]string
		listToSetMap = make(map[string]map[string][]string)
	)

	for _, rowReq = range rowRequests {
		if ruleMap, ok = RuleIns(rowReq.RuleKey); !ok {
			log.Warn(fmt.Sprintf("without rule %s", rowReq.RuleKey))
			continue
		}
		if len(ruleMap.TableInfo.Columns) != len(rowReq.Row) {
			log.Warn(fmt.Sprintf("%s schema mismatching", rowReq.RuleKey))
			continue
		}
		if rdbResp, err = rdbEpoint.ruleResponse(rowReq, ruleMap); err != nil {
			log.Error(fmt.Sprintf(fmt.Sprintf("rule response %s error: %s", rowReq.RuleKey, err.Error())))
			continue
		}
		// 是否需要将list数据保存到有序集合sortedset去重
		if ruleMap.RedisStructure == "List" && ruleMap.RedisListToSet {
			if kvMap, err = ruleMap.Strategist.ConvertKeyVal(rowReq, ruleMap); err != nil {
				log.Error(fmt.Sprintf("Dedup:      Convert list result to set error: %s\n", err.Error()))
				continue
			}
			if _, ok = listToSetMap[ruleMap.RedisListToSetKey]; !ok {
				listToSetMap[ruleMap.RedisListToSetKey] = make(map[string][]string)
			}
			// 判断是否有队列长度限制
			if rdbEpoint.judgeCount(ruleMap.RedisListToSetKey, ruleMap.RedisListToSetInterval, ruleMap.RedisListToSetLength) {
				for key = range kvMap {
					if kvMap[key] != "" {
						if _, ok = listToSetMap[ruleMap.RedisListToSetKey][key]; ok{
							listToSetMap[ruleMap.RedisListToSetKey][key] = append(listToSetMap[ruleMap.RedisListToSetKey][key], kvMap[key])
						} else {
							listToSetMap[ruleMap.RedisListToSetKey][key] = []string{kvMap[key]}
						}
					} else {
						log.Warn (fmt.Sprintf("Empty:      Get empty value from list: %s\n", key))
					}
				}
			}
		} else {
			rdbEpoint.preparePipe(rdbResp, pipe)
		}

		if rdbResp.Structure == "Hash" {
			log.Info(fmt.Sprintf("action: %s, structure: %s, key: %s, subKey: %s, value: %v", rdbResp.Action, rdbResp.Structure, rdbResp.Key, rdbResp.SubKey, rdbResp.Val))
		} else {
			log.Info(fmt.Sprintf("action: %s, structure: %s, key: %s, value: %v", rdbResp.Action, rdbResp.Structure, rdbResp.Key, rdbResp.Val))
		}
	}

	if len(listToSetMap) > 0 {
		for key, kvArray = range listToSetMap {
			for tmpKey = range kvArray {
				tmpResp = RedisResponse{
					Structure: "SortedSet",
					Key: key,
					Score: float64(time.Now().Unix()),
					Val: fmt.Sprintf("%s:%s", tmpKey, strings.Join(kvArray[tmpKey], ",")),
				}
				rdbEpoint.preparePipe(&tmpResp, pipe)
			}
		}
		// 需要写入elasticsearch时,检测集群cpu达到指定值时,就暂停
		if judgeCpu() {
			if _, err = pipe.Exec(context.Background()); err != nil {
				return
			}
		}
	} else {
		if _, err = pipe.Exec(context.Background()); err != nil {
			return
		}
	}

	log.Info(fmt.Sprintf("process %d rows done", len(rowRequests)))
	return
}

func convertColumnData(value interface{}, tableColumn *schema.TableColumn, ruleMap *RuleMap) interface{} {
	if value == nil {
		return nil
	}
	var (
		err error
		i int
		tmpInt64 int64
		tmpFloat64 float64
		tmpStr string
		tmpSlice []string
		in interface{}
		tmpTime time.Time
	)
	switch tableColumn.Type {
	case schema.TYPE_ENUM:
		switch value.(type) {
		case int64:
			tmpInt64 = value.(int64) - 1
			if tmpInt64 < 0 || tmpInt64 >= int64(len(tableColumn.EnumValues)) {
				// we insert invalid enum value before, so return empty
				log.Warn(fmt.Sprintf("invalid binlog enum index %d, for enum %v", tmpInt64, tableColumn.EnumValues))
				return ""
			}
			return tableColumn.EnumValues[tmpInt64]
		case string:
			return value
		case []byte:
			return string(value.([]byte))
		}
	case schema.TYPE_SET:
		switch value.(type) {
		case int64:
			tmpInt64 = value.(int64)
			tmpSlice = make([]string, 0, len(tableColumn.SetValues))
			for i, tmpStr = range tableColumn.SetValues {
				if tmpInt64&int64(1<<uint(i)) > 0 {
					tmpSlice = append(tmpSlice, tmpStr)
				}
			}
			return strings.Join(tmpSlice, ",")
		}
	case schema.TYPE_BIT:
		switch value.(type) {
		case string:
			if value.(string) == "\x01" {
				return int64(1)
			}
			return int64(0)
		}
	case schema.TYPE_STRING:
		switch value.(type) {
		case []byte:
			return string(value.([]byte)[:])
		}
	case schema.TYPE_JSON:
		switch value.(type) {
		case string:
			err = json.Unmarshal([]byte(value.(string)), &in)
		case []byte:
			err = json.Unmarshal(value.([]byte), &in)
		}
		if err == nil && in != nil {
			return in
		}
	case schema.TYPE_DATETIME, schema.TYPE_TIMESTAMP:
		switch value.(type) {
		case string:
			tmpStr = value.(string)
		case []byte:
			tmpStr = string(value.([]byte))
		}
		if ruleMap.DatetimeFormatter != "" {
			tmpTime, err = time.Parse(mysql.TimeFormat, tmpStr)
			if err != nil || tmpTime.IsZero() { // failed to parse date or zero date
				return nil
			}
			return tmpTime.Format(ruleMap.DatetimeFormatter)
		}
		return tmpStr
	case schema.TYPE_DATE:
		switch value.(type) {
		case string:
			tmpStr = value.(string)
		case []byte:
			tmpStr = string(value.([]byte))
		}
		if ruleMap.DateFormatter != "" {
			tmpTime, err = time.Parse(defaultDateFormatter, tmpStr)
			if err != nil || tmpTime.IsZero() { // failed to parse date or zero date
				return nil
			}
			return tmpTime.Format(ruleMap.DateFormatter)
		}
		return tmpStr
	case schema.TYPE_NUMBER:
		switch value.(type) {
		case string:
			if tmpInt64, err = strconv.ParseInt(value.(string), 10, 64); err != nil {
				log.Error(fmt.Sprintf("parse %v to int error: %s", value, err.Error()))
				return nil
			}
			return tmpInt64
		case []byte:
			if tmpInt64, err = strconv.ParseInt(string(value.([]byte)), 10, 64); err != nil {
				log.Error(fmt.Sprintf("parse %s to int error: %s", string(value.([]byte)), err.Error()))
				return nil
			}
			return tmpInt64
		}
	case schema.TYPE_DECIMAL, schema.TYPE_FLOAT:
		switch value.(type) {
		case string:
			if tmpFloat64, err = strconv.ParseFloat(value.(string), 64); err != nil {
				log.Error(fmt.Sprintf("parse %s to float error: %s", value.(string), err.Error()))
				return nil
			}
			return tmpFloat64
		case []byte:
			if tmpFloat64, err = strconv.ParseFloat(string(value.([]byte)), 64); err != nil {
				log.Error(fmt.Sprintf("parse %s to float error: %s", value.(string), err.Error()))
				return nil
			}
			return tmpFloat64
		}
	}

	return value
}


func rowMap(rowReq *RowRequest, ruleMap *RuleMap) (result map[string]interface{}, err error) {
	var (
		key, column string
		i,j int
		ok bool
		tableColumn schema.TableColumn
		includes []string
		excludes []string
		columnMap, excludeColumnMap map[string]bool
	)
	result = make(map[string]interface{})
	columnMap = make(map[string]bool)
	excludeColumnMap = make(map[string]bool)

	if ruleMap.IncludeColumns != "" {
		includes = strings.Split(ruleMap.IncludeColumns, ",")
	}
	if ruleMap.ExcludeColumns != "" {
		excludes = strings.Split(ruleMap.ExcludeColumns, ",")
	}
	if len(includes) > 0 {
		for _, column = range includes {
			_, i = ruleMap.TableColumn(column)
			if i < 0 {
				err = errors.New(fmt.Sprintf("include_field %s not table column", column))
				return
			}
			columnMap[column] = true
		}
	} else {
		for _, column = range excludes {
			excludeColumnMap[column] = true
		}
		for _, tableColumn = range ruleMap.TableInfo.Columns {
			if _, ok = excludeColumnMap[tableColumn.Name]; !ok {
				columnMap[tableColumn.Name] = true
			}
		}
	}

	if len(columnMap) == 0 {
		log.Warn(fmt.Sprintf("result column is empty!"))
		return
	}

	if rowReq.Action == canal.InsertAction {
		result = make(map[string]interface{}, len(columnMap))
	}

	// result必须添加主键,方便查看
	for i, tableColumn = range ruleMap.TableInfo.Columns {
		if ruleMap.IsCompositeKey {
			for _, j = range ruleMap.RedisKeyColumnIndexs {
				if i == j {
					columnMap[tableColumn.Name] = true
				}
			}
		} else {
			if i == ruleMap.RedisKeyColumnIndex {
				columnMap[tableColumn.Name] = true
			}
			break
		}
	}

	for i, tableColumn = range ruleMap.TableInfo.Columns {
		key = tableColumn.Name
		if _, ok = columnMap[key]; ok {
			// TODO 复合主键尚未验证
			if rowReq.Row[i] != nil {
				result[key] = convertColumnData(rowReq.Row[i], &tableColumn, ruleMap)
			} else if rowReq.Action == canal.UpdateAction && rowReq.Old[i] != nil {
				result[key] = convertColumnData(rowReq.Old[i], &tableColumn, ruleMap)
			}
		}
	}

	return
}

func (rdbEpoint *RedisEndpoint) Connect() error {
	return rdbEpoint.Ping()
}

func (rdbEpoint *RedisEndpoint) Ping() (err error) {
	_, err = rdbEpoint.client.Ping(context.Background()).Result()
	return
}

func (rdbEpoint *RedisEndpoint) pipe() redis.Pipeliner {
	return rdbEpoint.client.Pipeline()
}

func (rdbEpoint *RedisEndpoint) zcard(key string) (int64, error) {
	var cmd *redis.IntCmd
	cmd = rdbEpoint.client.ZCard(context.Background(), key)
	return cmd.Val(), cmd.Err()
}

func (rdbEpoint *RedisEndpoint) judgeCount(key string, interval, length int64) bool {
	// 判断是否有队列长度限制
	if interval > 0 && length > 0{
		var (
			err error
			count int64
			ticker = time.NewTicker(time.Duration(interval) * time.Second)
		)
		defer ticker.Stop()
		for {
			if count, err = rdbEpoint.zcard(key); err != nil {
				log.Info(fmt.Sprintf("Suspend:    Redis zcard error: %s", err.Error()))
			} else if count >= length {
				log.Info(fmt.Sprintf("Suspend:    Redis set count reach to %d, now let's take a break", count))
			} else {
				return true
			}
			select {
			case <-ticker.C:
			}
		}
	}
	return true
}

func (rdbEpoint *RedisEndpoint) ruleResponse(rowReq *RowRequest, ruleMap *RuleMap) (rdbResp *RedisResponse, err error) {
	var (
		kvMap map[string]interface{}
		tmpBytes []byte
		tmpKey string
		in interface{}
	)

	rdbResp = new(RedisResponse)
	rdbResp.Action = rowReq.Action
	rdbResp.Structure = ruleMap.RedisStructure
	if in, err = ruleMap.Strategist.ConvertVal(rowReq, ruleMap); err != nil {
		return
	}
	tmpKey = ConvertToString(in)
	if rdbResp.Structure == "String" {
		rdbResp.Key = tmpKey
	} else {
		// list, hash, set, sortedset 的主key
		rdbResp.Key = ruleMap.RedisMasterKey
		if rdbResp.Structure == "Hash" {
			rdbResp.SubKey = tmpKey
		}
		if rdbResp.Structure == "SortedSet" {
			rdbResp.Score = rdbEpoint.encodeScoreField(rowReq, ruleMap)
		}
	}

	// string, hash 值为变更值; list,set,sortset 值为操作类型 + 键
	if rdbResp.Structure == "String" || rdbResp.Structure == "Hash" {
		if kvMap, err = rowMap(rowReq, ruleMap); err != nil {
			return
		}
		if tmpBytes, err = json.Marshal(kvMap); err != nil {
			return
		} else {
			rdbResp.Val = string(tmpBytes)
		}
	} else {
		rdbResp.Val = tmpKey
	}
	return
}

func (rdbEpoint *RedisEndpoint) preparePipe(rdbResp *RedisResponse, pipe redis.Cmdable) {
	var (
		tmpZ *redis.Z
	)
	switch rdbResp.Structure {
	case "String":
		// 字符串情况下表示存储具体值
		if rdbResp.Action == canal.DeleteAction {
			pipe.Del(context.Background(), rdbResp.Key)
		} else {
			pipe.Set(context.Background(), rdbResp.Key, rdbResp.Val, 0)
		}
	case "Hash":
		// 哈希情况下表示存储具体值
		if rdbResp.Action == canal.DeleteAction {
			pipe.HDel(context.Background(), rdbResp.Key, rdbResp.SubKey)
		} else {
			pipe.HSet(context.Background(), rdbResp.Key, rdbResp.SubKey, rdbResp.Val)
		}
	case "List":
		// 队列情况下表示存储 操作类型+键
		pipe.RPush(context.Background(), rdbResp.Key, rdbResp.Val)
	case "Set":
		// 集合类型表示存储 操作类型+键
		pipe.SAdd(context.Background(), rdbResp.Key, rdbResp.Val)
	case "SortedSet":
		// 有序集合类型表示存储 操作类型+键
		tmpZ = &redis.Z{Score: rdbResp.Score, Member: rdbResp.Val}
		pipe.ZAdd(context.Background(), rdbResp.Key, tmpZ)
	}
}


func (rdbEpoint *RedisEndpoint) encodeScoreField(rowReq *RowRequest, ruleMap *RuleMap) float64 {
	if ruleMap.RedisScoreColumnIndex < 0 {
		return float64(time.Now().Unix())
	} else {
		var in = rowReq.Row[ruleMap.RedisScoreColumnIndex]
		if in == nil {
			return float64(time.Now().Unix())
		}
		return ToFloat64Safe(ConvertToString(in))
	}
}

func GetChangeVal(rowReq *RowRequest, i int) interface{} {
	if rowReq.Row[i] != nil {
		return rowReq.Row[i]
	} else if len(rowReq.Old) > i && rowReq.Old[i] != nil {
		return rowReq.Old[i]
	}
	return nil
}

func (rdbEpoint *RedisEndpoint) Close() {
	if rdbEpoint.client != nil {
		rdbEpoint.client.Close()
	}
}

