package common

import (
	"errors"
	"fmt"
	"github.com/go-mysql-org/go-mysql/canal"
	"strings"
)

type Strategy interface {
	ConvertVal(*RowRequest, *RuleMap) (interface{}, error)
	ConvertKeyVal(*RowRequest, *RuleMap) (map[string]string, error)
}

func NewStrategy(strategyType string) (Strategy, error) {
	var typeStr = strings.ToLower(strategyType)
	if typeStr != "" {
		if typeStr == "lemon" {
			return newLemonStrategy(), nil
		} else if typeStr == "someStrategy" {

		} else if typeStr == "otherStrategy" {

		} else {
			return nil, errors.New(fmt.Sprintf("no strategy could be apply"))
		}
	}
	return newLemonStrategy(), nil
}

type LemonStrategy struct {
}

func newLemonStrategy() *LemonStrategy {
	return &LemonStrategy{}
}

func (lemonStrategy *LemonStrategy) ConvertVal(rowReq *RowRequest, ruleMap *RuleMap) (in interface{}, err error) {
	var (
		i             int
		key           string
		dbStrSlice    []string
		tableStrSlice []string
	)

	if ruleMap.IsCompositeKey {
		for _, i = range ruleMap.RedisKeyColumnIndexs {
			key += ConvertToString(GetChangeVal(rowReq, i)) + "+"
		}
		key = fmt.Sprintf("[%s]", strings.TrimRight(key, "+"))
	} else {
		key = ConvertToString(GetChangeVal(rowReq, ruleMap.RedisKeyColumnIndex))
	}

	// 自定义业务
	dbStrSlice = strings.Split(ruleMap.Database, "_")
	if len(dbStrSlice) != 3 {
		err = errors.New(fmt.Sprintf("ruleMap's Database segment length not equal 3"))
		return
	}
	tableStrSlice = strings.Split(ruleMap.Table, "_")
	if len(tableStrSlice) < 2 {
		err = errors.New(fmt.Sprintf("ruleMap's Table segment length less than 2"))
		return
	}
	if rowReq.Action == canal.DeleteAction {
		key = fmt.Sprintf("%s_%s_%s:%s",
			strings.ToLower(dbStrSlice[1]),
			strings.ToLower(tableStrSlice[1]),
			strings.ToLower(canal.DeleteAction), key)
	} else {
		key = fmt.Sprintf("%s_%s:%s",
			strings.ToLower(dbStrSlice[1]),
			strings.ToLower(tableStrSlice[1]), key)
	}

	// string, hash 添加缓存前缀
	if ruleMap.RedisStructure == "String" || ruleMap.RedisStructure == "Hash" {
		if ruleMap.RedisKeyPrefix != "" {
			key = ruleMap.RedisKeyPrefix + key
		}
	}
	in = key
	return
}

func (lemonStrategy *LemonStrategy) ConvertKeyVal(rowReq *RowRequest, ruleMap *RuleMap) (kvMap map[string]string, err error) {
	var (
		i             int
		key           string
		dbStrSlice    []string
		tableStrSlice []string
	)
	kvMap = make(map[string]string)
	if ruleMap.IsCompositeKey {
		for _, i = range ruleMap.RedisKeyColumnIndexs {
			key += ConvertToString(GetChangeVal(rowReq, i)) + "+"
		}
		key = fmt.Sprintf("[%s]", strings.TrimRight(key, "+"))
	} else {
		key = ConvertToString(GetChangeVal(rowReq, ruleMap.RedisKeyColumnIndex))
	}

	// 自定义业务
	dbStrSlice = strings.Split(ruleMap.Database, "_")
	if len(dbStrSlice) != 3 {
		err = errors.New(fmt.Sprintf("ruleMap's Database segment length not equal 3"))
		return
	}
	tableStrSlice = strings.Split(ruleMap.Table, "_")
	if len(tableStrSlice) < 2 {
		err = errors.New(fmt.Sprintf("ruleMap's Table segment length less than 2"))
		return
	}
	if rowReq.Action == canal.DeleteAction {
		kvMap[fmt.Sprintf("%s_%s_%s",
			strings.ToLower(dbStrSlice[1]),
			strings.ToLower(tableStrSlice[1]),
			strings.ToLower(canal.DeleteAction))] = key
	} else {
		kvMap[fmt.Sprintf("%s_%s",
			strings.ToLower(dbStrSlice[1]),
			strings.ToLower(tableStrSlice[1]))] = key
	}
	return
}