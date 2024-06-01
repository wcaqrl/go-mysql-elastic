package common

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	log "github.com/sirupsen/logrus"
	"go-mysql-elasticsearch/asset"
	"go-mysql-elasticsearch/config"
	"go-mysql-elasticsearch/datamodels"
	"io/ioutil"
	"net/http"
	"reflect"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var client *elastic.Client

type FollowInfo struct {
	FollowerIndex string
	RemoteCluster string
	LeaderIndex   string
	Status        string
}

//创建elastic 连接
func NewElasticConn(cfg config.ESConfig) (cli *elastic.Client, err error) {
	var (
		esHost string
		info   *elastic.PingResult
		code   int
	)
	esHost = fmt.Sprintf("http://%s:%s", cfg.Host, cfg.Port)
	if cli, err = elastic.NewClient(
		elastic.SetURL(esHost),
		elastic.SetBasicAuth(cfg.Username, cfg.Password),
	); err != nil {
		log.Panic("Failed to Open elastic, err: " + err.Error())
	}

	if info, code, err = cli.Ping(esHost).Do(context.Background()); err != nil {
		log.Panic("Ping elastic failed, err: " + err.Error())
	}
	log.Info(fmt.Sprintf("Elasticsearch returned with code %d and version %s", code, info.Version.Number))
	client = cli
	return
}

func SendElasticRequest(method, url, username, password string, body []byte) (bodyMap map[string]interface{}, err error) {
	var (
		client http.Client
		req    *http.Request
		resp   *http.Response
	)
	bodyMap = make(map[string]interface{})
	if body == nil || len(body) == 0 {
		body = make([]byte, 0)
	}
	client = http.Client{}
	if req, err = http.NewRequest(method, url, bytes.NewBuffer(body)); err != nil {
		return
	}
	req.Header.Set("Content-Type", "application/json")
	req.SetBasicAuth(username, password)
	if resp, err = client.Do(req); err != nil {
		return
	}
	defer resp.Body.Close()
	if body, err = ioutil.ReadAll(resp.Body); err != nil {
		return
	}
	if err = json.Unmarshal(body, &bodyMap); err != nil {
		return
	}
	return
}

func GetClusterCpuPercent() (percent float64, err error) {
	var (
		clusterStatsRes *elastic.ClusterStatsResponse
		currentNanoTime = time.Now().UnixNano()
		cpuInfo         map[int64]float64
		ok              bool
		// 控制查询时间间隔为每秒
		deltaTime  int64 = 1
		oldTime    int64
		oldPercent float64
	)
	select {
	case cpuInfo, ok = <-config.Config.ESConfig.CpuChan:
		if ok {
			for oldTime, oldPercent = range cpuInfo {
				//log.Info(fmt.Sprintf("Cluster:    Time: %s, Percent: %.1f%%\n", time.Unix(oldTime/1e9,0).In(time.FixedZone("CST", 8*3600)).Format("2006-01-02 15:04:05"), oldPercent))
				if currentNanoTime-oldTime > deltaTime*1e9 {
					if clusterStatsRes, err = client.ClusterStats().Do(context.Background()); err != nil {
						log.Error(fmt.Sprintf("Failed to get cluster CPU percent, err: " + err.Error()))
						config.Config.ESConfig.CpuChan <- map[int64]float64{oldTime: oldPercent}
						return
					} else {
						percent = clusterStatsRes.Nodes.Process.CPU.Percent / float64(clusterStatsRes.Nodes.Count.Data)
						config.Config.ESConfig.CpuChan <- map[int64]float64{currentNanoTime: percent}
					}
				} else {
					percent = oldPercent
					config.Config.ESConfig.CpuChan <- map[int64]float64{oldTime: oldPercent}
				}
			}
		} else {
			config.Config.ESConfig.CpuChan <- map[int64]float64{0: 0}
		}
	default:
		config.Config.ESConfig.CpuChan <- map[int64]float64{0: 0}
	}
	return
}

func judgeCpu() bool {
	// 监控集群CPU百份比,超过指定值就暂停写入
	if config.Config.ESConfig.CpuInterval > 0 {
		var (
			percent float64
			ticker  = time.NewTicker(time.Duration(config.Config.ESConfig.CpuInterval) * time.Second)
		)
		defer ticker.Stop()
		for {
			if percent, _ = GetClusterCpuPercent(); percent >= config.Config.ESConfig.CpuPercent {
				log.Info(fmt.Sprintf("Suspend:    Cluster CPU rush to %.1f%%, now let's take a break", percent))
			} else {
				//log.Info(fmt.Sprintf("cpu percent: %.1f%%", percent))
				return true
			}
			select {
			case <-ticker.C:
			}
		}
	}
	return true
}

func CreateChunk(datas interface{}, index string) (err error) {
	var (
		disable                                                            int8
		l                                                                  int
		id, actionName, oriPrefix, prefix, msg, tmpID, placeHolder, delIds string
		actions                                                            = strings.Split(index, "_")
		addArr                                                             = make([]string, 0)
		delArr                                                             = make([]string, 0)
		val                                                                interface{}
		v                                                                  reflect.Value
		bulkService                                                        = client.Bulk()
		r                                                                  *elastic.BulkUpdateRequest
	)
	actionName = fmt.Sprintf("%s_%s", actions[0], actions[1])
	oriPrefix = GetIndexPrefix(index)
	prefix = oriPrefix
	v = reflect.ValueOf(datas)
	if v.Kind() != reflect.Slice {
		msg = "not slice"
		log.Error(msg)
		return errors.New(msg)
	}
	l = v.Len()
	for i := 0; i < l; i++ {
		val = v.Index(i).Interface()
		switch val.(type) {
		case *datamodels.VideoMix:
			disable = val.(*datamodels.VideoMix).Disable
			tmpID = strconv.FormatInt(val.(*datamodels.VideoMix).ID, 10)
		case *datamodels.Play:
			disable = val.(*datamodels.Play).Disable
			tmpID = strconv.FormatInt(val.(*datamodels.Play).ID, 10)
		case *datamodels.MediaMix:
			disable = val.(*datamodels.MediaMix).Disable
			tmpID = strconv.FormatInt(val.(*datamodels.MediaMix).ID, 10)
		case *datamodels.MediaEpisodeMix:
			disable = val.(*datamodels.MediaEpisodeMix).Disable
			tmpID = strconv.FormatInt(val.(*datamodels.MediaEpisodeMix).ID, 10)
		case *datamodels.AnchorMix:
			disable = val.(*datamodels.AnchorMix).Disable
			tmpID = strconv.FormatInt(val.(*datamodels.AnchorMix).ID, 10)
		case *datamodels.AppExternalMix:
			disable = val.(*datamodels.AppExternalMix).Disable
			tmpID = strconv.FormatInt(val.(*datamodels.AppExternalMix).ID, 10)
		case *datamodels.TopicMix:
			disable = val.(*datamodels.TopicMix).Disable
			if val.(*datamodels.TopicMix).Type == "media" {
				placeHolder = "m"
			} else if val.(*datamodels.TopicMix).Type == "video" {
				placeHolder = "v"
			}
			if placeHolder != "" {
				prefix = combinePrefix(oriPrefix, placeHolder)
			}
			tmpID = strconv.FormatInt(val.(*datamodels.TopicMix).ID, 10)
		case *datamodels.Actor:
			disable = val.(*datamodels.Actor).Disable
			tmpID = strconv.FormatInt(val.(*datamodels.Actor).ID, 10)
		case *datamodels.LogCollect:
			tmpID = val.(*datamodels.LogCollect).ID
		case *datamodels.VmsMediaMix:
			disable = val.(*datamodels.VmsMediaMix).Disable
			tmpID = strconv.FormatInt(val.(*datamodels.VmsMediaMix).ID, 10)
		}

		// 索引写入时, 全量索引保留所有disable数据, 非全量索引仅保留disable=0的数据
		if !config.Config.IsFull && disable != 0 {
			delArr = append(delArr, tmpID)
		} else {
			addArr = append(addArr, tmpID)
			id = prefix + tmpID
			r = elastic.NewBulkUpdateRequest().Index(index).Id(id).Doc(val).DocAsUpsert(true)
			bulkService.Add(r)
		}

		if len(delArr) >= int(config.Config.ESConfig.BulkNumber) {
			delIds = strings.Join(delArr, ",")
			if !DeleteByIds(index, delIds) {
				log.Error(fmt.Sprintf("delete error occured: %s", delIds))
			}
			delArr = make([]string, 0)
		}

		if bulkService.NumberOfActions() >= int(config.Config.ESConfig.BulkNumber) {
			if err = BulkDocs(bulkService, actionName, addArr); err != nil {
				return
			}
			addArr = make([]string, 0)
		}
	}

	if len(delArr) > 0 {
		delIds = strings.Join(delArr, ",")
		if !DeleteByIds(index, delIds) {
			log.Error(fmt.Sprintf("delete error occured: %s", delIds))
		}
	}

	if bulkService.NumberOfActions() > 0 {
		if err = BulkDocs(bulkService, actionName, addArr); err != nil {
			return
		}
	}
	return
}

func CreateUpdate(datas interface{}, index string) (err error) {
	var (
		disable    int8
		l          int
		tmpID, msg string
		delArr     []string
		addArr     []interface{}
		val        interface{}
		v          reflect.Value
	)
	v = reflect.ValueOf(datas)
	if v.Kind() != reflect.Slice {
		msg = "not slice"
		log.Error(msg)
		return errors.New(msg)
	}
	l = v.Len()
	for i := 0; i < l; i++ {
		val = v.Index(i).Interface()
		switch val.(type) {
		case *datamodels.VideoMix:
			disable = val.(*datamodels.VideoMix).Disable
			tmpID = strconv.FormatInt(val.(*datamodels.VideoMix).ID, 10)
		case *datamodels.Play:
			disable = val.(*datamodels.Play).Disable
			tmpID = strconv.FormatInt(val.(*datamodels.Play).ID, 10)
		case *datamodels.MediaMix:
			disable = val.(*datamodels.MediaMix).Disable
			tmpID = strconv.FormatInt(val.(*datamodels.MediaMix).ID, 10)
		case *datamodels.MediaEpisodeMix:
			disable = val.(*datamodels.MediaEpisodeMix).Disable
			tmpID = strconv.FormatInt(val.(*datamodels.MediaEpisodeMix).ID, 10)
		case *datamodels.AnchorMix:
			disable = val.(*datamodels.AnchorMix).Disable
			tmpID = strconv.FormatInt(val.(*datamodels.AnchorMix).ID, 10)
		case *datamodels.AppExternalMix:
			disable = val.(*datamodels.AppExternalMix).Disable
			tmpID = strconv.FormatInt(val.(*datamodels.AppExternalMix).ID, 10)
		case *datamodels.TopicMix:
			disable = val.(*datamodels.TopicMix).Disable
			tmpID = strconv.FormatInt(val.(*datamodels.TopicMix).ID, 10)
		case *datamodels.Actor:
			disable = val.(*datamodels.Actor).Disable
			tmpID = strconv.FormatInt(val.(*datamodels.Actor).ID, 10)
		case *datamodels.VmsMediaMix:
			disable = val.(*datamodels.VmsMediaMix).Disable
			tmpID = strconv.FormatInt(val.(*datamodels.VmsMediaMix).ID, 10)
		}
		/*
			// 索引更新时, 遇到 disable = 9 的数据都删除掉
			if disable == 9 {
				delArr = append(delArr, tmpID)
			} else {
				addArr = append(addArr, val)
			}
		*/

		// 索引写入时, 全量索引保留所有disable数据, 非全量索引仅保留disable=0的数据
		if config.Config.IsFull {
			addArr = append(addArr, val)
		} else {
			if disable != 0 {
				delArr = append(delArr, tmpID)
			} else {
				addArr = append(addArr, val)
			}
		}
	}

	//如果存在disable不为0的数据, 则从elasticsearch中删除
	if len(delArr) > 0 {
		var delIds = strings.Join(delArr, ",")
		if !DeleteByIds(index, delIds) {
			log.Error(fmt.Sprintf("delete error occured: %s", delIds))
		}
	}

	// 如果存在需要更新的数据
	if len(addArr) > 0 {
		if err = CreateChunk(addArr, index); err != nil {
			log.Error("CreateChunk for update failed: " + err.Error())
		}
	}
	return
}

func BulkDocs(bulkService *elastic.BulkService, actionName string, idArr []string) (err error) {
	var (
		retryErr                                                       error
		ok                                                             bool
		idMap                                                          = make(map[string]bool)
		failIdMap                                                      = make(map[string]bool)
		successIdArr                                                   = make([]string, 0)
		bulkResponse                                                   *elastic.BulkResponse
		responseItem                                                   *elastic.BulkResponseItem
		tmpID, tmpIndex, tmpActionName, tmpPrefix, tmpMetaID, tmpDocID string
		tmpActions                                                     []string
	)

	if bulkResponse, err = bulkService.Do(context.Background()); err != nil {
		// 索引文档报错, 则拆分成最小单元加入重试
		if len(idArr) > 0 {
			if retryErr = RetryId(actionName, idArr); retryErr != nil {
				log.Error(fmt.Sprintf("Retry:       retry index ids (%s) error: %v\n", strings.Join(idArr, ","), retryErr.Error()))
			}
		}
		log.Error(fmt.Sprintf("elasticsearch CreateChunk ids (%s) error: %v\n", strings.Join(idArr, ","), err.Error()))
		return
	} else {
		for _, tmpID = range idArr {
			idMap[tmpID] = true
		}
		// 如果响应结果中有错误, 则记录错误并追加重试
		if bulkResponse.Errors {
			for _, responseItem = range bulkResponse.Failed() {
				log.Error(fmt.Sprintf("Reason:      %v\n", responseItem.Error.Reason))
				tmpIndex = responseItem.Index
				tmpActions = strings.Split(tmpIndex, "_")
				if len(tmpActions) < 2 {
					log.Error(fmt.Sprintf("Invalid:     index name %s invalid\n", tmpIndex))
					continue
				}
				tmpPrefix = GetIndexPrefix(tmpIndex)
				tmpActionName = strings.TrimRight(tmpPrefix, "_")
				tmpMetaID = responseItem.Id
				tmpDocID = strings.TrimPrefix(tmpMetaID, tmpPrefix)
				if tmpDocID != "" && tmpActionName != "" {
					failIdMap[tmpDocID] = true
					if retryErr = RetryId(tmpActionName, []string{tmpDocID}); retryErr != nil {
						log.Error(fmt.Sprintf("Retry:       retry index ids (%s) for (%s) error: %v\n", tmpDocID, tmpActionName, retryErr.Error()))
					}
				}
			}
		}
		// 完成索引文档的id不必重试, 无论redis中是否存在,都进行删除
		for tmpID = range idMap {
			if _, ok = failIdMap[tmpID]; !ok {
				successIdArr = append(successIdArr, fmt.Sprintf("%s:%s", actionName, tmpID))
			}
		}
		if len(successIdArr) > 0 {
			Del(successIdArr)
		}
	}
	return err
}

func UpdateField(datas interface{}, index string) (err error) {
	var (
		idMap    = make(map[int64]interface{})
		v        reflect.Value
		msg      string
		l        int
		val      interface{}
		videoMix *datamodels.VideoMix
	)
	v = reflect.ValueOf(datas)
	if v.Kind() != reflect.Slice {
		msg = "not slice"
		log.Error(msg)
		return errors.New(msg)
	}
	l = v.Len()
	for i := 0; i < l; i++ {
		val = v.Index(i).Interface()
		switch val.(type) {
		case *datamodels.Dispatch:
			videoMix, _ = TransferToVideoMix(val.(*datamodels.Dispatch))
			idMap[videoMix.VideoID] = []interface{}{videoMix.InjectBrands, videoMix.VersionID}
		}

		if len(idMap) >= int(config.Config.ESConfig.BulkNumber) {
			DoUpdate(idMap, index)
			idMap = make(map[int64]interface{})
		}
	}

	if len(idMap) > 0 {
		DoUpdate(idMap, index)
		idMap = make(map[int64]interface{})
	}
	return
}

func DoUpdate(idMap map[int64]interface{}, index string) (err error) {
	var (
		script   string
		query    elastic.Query
		ids      = make([]interface{}, 0)
		paramMap = make(map[string]interface{})
		key      int64
	)
	for key, _ = range idMap {
		ids = append(ids, key)
	}
	query = elastic.NewTermsQuery("id", ids...)
	script = "ctx._source.inject_brands=params.value"
	paramMap["value"] = []string{"3265"} // 湖北移动 brand_id=3265
	if _, err = elastic.NewUpdateByQueryService(client).
		Query(query).
		Index(index).
		Script(elastic.NewScript(script).Params(paramMap).Lang("painless")).
		Type("_doc").
		IgnoreUnavailable(true).
		Do(context.Background()); err != nil {
		log.Error("update error occured: " + err.Error())
	}
	return
}

// elasticsearch 频繁compile性能跟不上,摒弃按条件更新
func DoUpdateV2(idMap map[int64]interface{}, index string) (err error) {
	var (
		script, scriptFormat string
		in                   interface{}
		query                elastic.Query
		ids                  = make([]interface{}, 0)
		paramMap             = make(map[string]interface{})
		key                  int64
	)
	scriptFormat = "if (ctx._source.id == %d) {ctx._source.inject_brands=params.inject_brands;ctx._source.version_id=params.version_id_%d} else "
	for key, in = range idMap {
		var tmpIn []interface{}
		script += fmt.Sprintf(scriptFormat, key, key)
		tmpIn = in.([]interface{})
		paramMap["version_id_"+strconv.FormatInt(key, 10)] = tmpIn[1] // 湖北移动 brand_id=3265
		ids = append(ids, key)

		if len(ids) >= 100 {
			query = elastic.NewTermsQuery("id", ids...)
			script = strings.TrimRight(script, "else ")
			// 湖北移动 brand_id=3265
			paramMap["inject_brands"] = []string{"3265"}
			if _, err = elastic.NewUpdateByQueryService(client).
				Query(query).
				Index(index).
				Script(elastic.NewScript(script).Params(paramMap).Lang("painless")).
				Type("_doc").
				IgnoreUnavailable(true).
				Do(context.Background()); err != nil {
				log.Error(script)
				//b, err := json.Marshal(paramMap)
				//log.Error(string(b))
				log.Error("update error occured: " + err.Error())
			} else {
				log.Info("update 成功")
			}
			script = ""
			ids = make([]interface{}, 0)
			paramMap = make(map[string]interface{})
		}
	}

	if len(ids) > 0 {
		query = elastic.NewTermsQuery("id", ids...)
		script = strings.TrimRight(script, "else ")
		// 湖北移动 brand_id=3265
		paramMap["inject_brands"] = []string{"3265"}
		if _, err = elastic.NewUpdateByQueryService(client).
			Query(query).
			Index(index).
			Script(elastic.NewScript(script).Params(paramMap).Lang("painless")).
			Type("_doc").
			IgnoreUnavailable(true).
			Do(context.Background()); err != nil {
			log.Error(script)
			//b, err := json.Marshal(paramMap)
			//log.Error(string(b))
			log.Error("update error occured: " + err.Error())
		} else {
			log.Info("update 成功")
		}
	}
	return
}

func IsExistsIndex(indexName string) bool {
	var (
		exists bool
		err    error
	)
	if exists, err = client.IndexExists(indexName).Do(context.Background()); err != nil {
		log.Error(fmt.Sprintf("IsExistsIndex %s error: %s\n", indexName, err.Error()))
		return false
	}
	return exists
}

func CreateIndex(indexName, mappingPath string) bool {
	var (
		err      error
		jsonMap  = make(map[string]interface{})
		tmpBytes []byte
	)

	if tmpBytes, err = asset.Asset(mappingPath); err != nil {
		log.Panic(err.Error())
	}
	// 是否配置了分片数
	if config.Config.ESConfig.Shards > 0 {
		if err = json.Unmarshal(tmpBytes, &jsonMap); err != nil {
			log.Panic(err.Error())
		}
		tmpBytes, err = json.Marshal(
			ReplaceMapValue(jsonMap, "",
				map[string]interface{}{
					"settings.index.number_of_shards": config.Config.ESConfig.Shards,
				}))
	}

	// 如果是全量索引(非日志索引), 则修改预排序字段
	if config.Config.IsFull && !strings.Contains(indexName, "_log") {
		if err = json.Unmarshal(tmpBytes, &jsonMap); err != nil {
			log.Panic(err.Error())
		}
		tmpBytes, err = json.Marshal(
			ReplaceMapValue(jsonMap, "",
				map[string]interface{}{
					"settings.index.sort.field": "version_id",
					"settings.index.sort.order": "asc",
				}))
	}
	if _, err = client.CreateIndex(indexName).BodyJson(string(tmpBytes)).Do(context.Background()); err != nil {
		log.Error(fmt.Sprintf("create index %s error: %s\n", indexName, err.Error()))
		return false
	}
	return true
}

func ExistsOrCreate(indexName, jsonFilePath string) bool {
	var exists bool
	exists = IsExistsIndex(indexName)
	if exists != true {
		if CreateIndex(indexName, jsonFilePath) != true {
			log.Panic(fmt.Sprintf("create index %s failed\n", indexName))
			return false
		}
	}
	return true
}

func SetAlias(indexName, aliasName string) bool {
	var (
		err               error
		pattern, matchStr string
		reg               *regexp.Regexp
		matches           []string
		match             bool
		indices           elastic.CatAliasesResponse
	)
	pattern = "([a-z_]*)\\d{14}"
	if config.Config.ENV == "dev" {
		pattern = "([a-z_]*)\\d{14}_dev"
	}
	reg, _ = regexp.Compile(pattern)
	matches = reg.FindStringSubmatch(indexName)
	if len(matches) != 2 {
		log.Error(fmt.Sprintf("could not match any index name for %s\n", indexName))
		return false
	}
	matchStr = matches[1] + "\\d{14}"
	if config.Config.ENV == "dev" {
		matchStr = matchStr + "_dev"
	}
	// 移除其它别名指向
	indices, _ = client.CatAliases().Alias(aliasName).Do(context.Background())
	for _, v := range indices {
		//正则匹配当前索引前缀才能执行移除别名的操作
		match, _ = regexp.MatchString(matchStr, v.Index)
		if match {
			if _, err = client.Alias().Remove(v.Index, v.Alias).Do(context.Background()); err != nil {
				log.Error(fmt.Sprintf("remove alias %s from index %s failed: %s\n", v.Alias, v.Index, err.Error()))
				return false
			}
			log.Info(fmt.Sprintf("Removing:   remove alias %s from index %s success\n", v.Alias, v.Index))
		}
	}

	// 添加别名
	if _, err = client.Alias().Add(indexName, aliasName).Do(context.Background()); err != nil {
		log.Error(fmt.Sprintf("Adding:    Add alias %s to index %s failed: %s\n", aliasName, indexName, err.Error()))
		return false
	}
	return true
}

func SetLogAlias(indexName, aliasName string) bool {
	var (
		err               error
		pattern, matchStr string
		reg               *regexp.Regexp
		matches           []string
		match             bool
		indices           elastic.CatAliasesResponse
	)
	pattern = "([a-z_]*)\\d{6}"
	if config.Config.ENV == "dev" {
		pattern = "([a-z_]*)\\d{6}_dev"
	}
	reg, _ = regexp.Compile(pattern)
	matches = reg.FindStringSubmatch(indexName)
	if len(matches) != 2 {
		log.Error(fmt.Sprintf("could not match any index name for %s\n", indexName))
		return false
	}
	matchStr = matches[1] + "\\d{6}"
	if config.Config.ENV == "dev" {
		matchStr = matchStr + "_dev"
	}
	// 移除其它别名指向
	indices, _ = client.CatAliases().Alias(aliasName).Do(context.Background())
	for _, v := range indices {
		//正则匹配当前索引前缀才能执行移除别名的操作
		match, _ = regexp.MatchString(matchStr, v.Index)
		if match {
			if _, err = client.Alias().Remove(v.Index, v.Alias).Do(context.Background()); err != nil {
				log.Error(fmt.Sprintf("remove alias %s from index %s failed: %s\n", v.Alias, v.Index, err.Error()))
				return false
			}
			log.Info(fmt.Sprintf("Removing:   remove alias %s from index %s success\n", v.Alias, v.Index))
		}
	}

	// 添加别名
	if _, err = client.Alias().Add(indexName, aliasName).Do(context.Background()); err != nil {
		log.Error(fmt.Sprintf("Adding:    Add alias %s to index %s failed: %s\n", aliasName, indexName, err.Error()))
		return false
	}
	return true
}

func GetIndexNameByAlias(hostStr, portStr, username, password, alias string) (indexName string, err error) {
	var (
		ok                                bool
		tmpIndexName, tmpAliasName        string
		client                            http.Client
		req                               *http.Request
		resp                              *http.Response
		body                              []byte
		bodyMap, indexNameMap, aliasesMap map[string]interface{}
		in, indexIn                       interface{}
	)
	client = http.Client{}
	if req, err = http.NewRequest(
		"GET",
		fmt.Sprintf("http://%s:%s/*/_alias/%s", hostStr, portStr, alias),
		bytes.NewBufferString(""),
	); err != nil {
		return
	}
	req.Header.Set("Content-Type", "application/json")
	req.SetBasicAuth(username, password)
	if resp, err = client.Do(req); err != nil {
		return
	}
	defer resp.Body.Close()
	if body, err = ioutil.ReadAll(resp.Body); err != nil {
		return
	}
	if err = json.Unmarshal(body, &bodyMap); err != nil {
		return
	}

	// 如果返回有错误, 则证明没有别名
	if _, ok = bodyMap["error"]; ok {
		err = errors.New(bodyMap["error"].(string))
		return
	}

	if len(bodyMap) > 0 {
		// 不考虑一个别名对应多条索引, 当前业务只有一个别名对应一条索引
		for tmpIndexName, in = range bodyMap {
			if indexNameMap, ok = in.(map[string]interface{}); ok {
				for _, indexIn = range indexNameMap {
					if aliasesMap, ok = indexIn.(map[string]interface{}); ok {
						for tmpAliasName, _ = range aliasesMap {
							if tmpAliasName == alias {
								indexName = tmpIndexName
								return
							}
						}
					}
				}
			}
		}
	}
	err = errors.New(fmt.Sprintf("the alias %s point nothing", alias))
	return
}

func SetReplicas(indexName string, number int8) bool {
	var (
		err  error
		body string
	)
	body = `{
		"index":{
			"number_of_replicas": "%d"
		}
	}`
	if _, err = client.IndexPutSettings().Index(indexName).BodyString(fmt.Sprintf(body, number)).Do(context.TODO()); err != nil {
		log.Error(fmt.Sprintf("put replica %d for %s failed: %v\n", number, indexName, err.Error()))
		return false
	}
	return true
}

func SetRefresh(indexName string, val interface{}) bool {
	var (
		err  error
		body string
	)
	switch val.(type) {
	case int8, int, int32, int64:
		body = fmt.Sprintf(`{"refresh_interval": %v}`, val)
	case string:
		body = fmt.Sprintf(`{"refresh_interval": "%v"}`, val)
	default:
		body = `{"refresh_interval": "1s"}`
	}
	if _, err = client.IndexPutSettings().Index(indexName).BodyString(body).Do(context.TODO()); err != nil {
		log.Error(fmt.Sprintf("put refresh_interval %v for %s failed: %v\n", val, indexName, err.Error()))
		return false
	}
	return true
}

func DeleteIndices(aliasName string) bool {
	var (
		err error
		//预定义待删除的索引数组
		deleteArr = make([]string, 0)
		dates     = make([]float64, 0)
		// 标记当前别名指向的最新索引
		currentDate float64
		// 查询出别名指向的所有索引
		aliasMap     = make(map[float64]string, 0)
		reg          *regexp.Regexp
		indices      elastic.CatAliasesResponse
		idx          elastic.CatIndicesResponse
		d, i         float64
		matches, mts []string
		tmpArr       = make([]float64, 0)
	)

	reg, _ = regexp.Compile("([a-z_].*)(\\d{14}).*")
	indices, _ = client.CatAliases().Alias(aliasName).Do(context.Background())
	for _, v := range indices {
		matches = reg.FindStringSubmatch(v.Index)
		if len(matches) >= 3 {
			i, _ = strconv.ParseFloat(matches[2], 64)
			if d < i {
				d = i
				currentDate = i
			}
			aliasMap[i] = v.Index
			dates = append(dates, i)
		}
	}

	// 匹配出其它索引名称
	mts = reg.FindStringSubmatch(aliasMap[currentDate])
	if len(mts) != 3 {
		log.Error(fmt.Sprintf("could not match any currentIndex name for %s\n", aliasMap[currentDate]))
		return false
	}

	idx, _ = client.CatIndices().Index(mts[1] + "*").Do(context.Background())
	for _, v := range idx {
		matches = reg.FindStringSubmatch(v.Index)
		if len(matches) >= 3 {
			i, _ = strconv.ParseFloat(matches[2], 64)
			if config.Config.ENV == "dev" {
				if strings.HasSuffix(v.Index, "_dev") {
					aliasMap[i] = v.Index
					dates = append(dates, i)
				}
			} else {
				aliasMap[i] = v.Index
				dates = append(dates, i)
			}
		}
	}

	// 排除当前索引
	for _, v := range dates {
		if v != currentDate {
			tmpArr = append(tmpArr, v)
		}
	}
	dates = tmpArr

	// 排除当前索引之后,如果待删除的数组长度超过1,则倒序排列之后从第2个元素开始删除
	if len(dates) > 1 {
		// 待删除的索引倒序
		sort.Sort(sort.Reverse(sort.Float64Slice(dates)))
		dates = dates[1:]
		for _, d := range dates {
			if value, ok := aliasMap[d]; ok {
				deleteArr = append(deleteArr, value)
			}
		}
	}

	if len(deleteArr) > 0 {
		if _, err = client.DeleteIndex().Index(deleteArr).Do(context.Background()); err != nil {
			log.Error(fmt.Sprintf("Delete indices failed for alias %s: %s\n", aliasName, err.Error()))
		}
		log.Info(fmt.Sprintf("Deleting:    Delete %d indices for alias %s\n", len(deleteArr), aliasName))
	} else {
		log.Info(fmt.Sprintf("Nothing:    Without any index for alias %s\n", aliasName))
	}
	return true
}

func DeleteByIds(aliasName, ids string) bool {
	var (
		err, retryErr                                                      error
		ok                                                                 bool
		actions                                                            = strings.Split(aliasName, "_")
		actionName                                                         string
		idArr                                                              = strings.Split(ids, ",")
		idMap                                                              = make(map[string]int64)
		failIdMap                                                          = make(map[string]int64)
		successIdArr                                                       = make([]string, 0)
		values                                                             = make([]interface{}, 0)
		termsQuery                                                         *elastic.TermsQuery
		delResponse                                                        *elastic.BulkIndexByScrollResponse
		tmpIndex, tmpActionName, tmpPrefix, tmpMetaID, tmpDocID, successID string
		tmpActions                                                         []string
	)
	actionName = fmt.Sprintf("%s_%s_delete", actions[0], actions[1])
	values = make([]interface{}, len(idArr))
	for i, v := range idArr {
		values[i] = v
		idMap[v] = 0
	}
	termsQuery = elastic.NewTermsQuery("id", values...)
	if delResponse, err = client.DeleteByQuery().Index(aliasName).Query(termsQuery).Do(context.Background()); err != nil {
		// 删除报错, 则拆分成最小单元加入重试
		if len(idArr) > 0 {
			if err = RetryId(actionName, idArr); err != nil {
				log.Error(fmt.Sprintf("Retry:       retry delete ids (%s) error: %v\n", ids, err.Error()))
			}
		}
		idArr = make([]string, 0)
		log.Error(fmt.Sprintf("Delete:      delete by ids (%s) error: %v\n", ids, err.Error()))
		return false
	} else {
		// 删除结果中有错误的, 则只重试错误结果
		if len(delResponse.Failures) > 0 {
			for _, failResp := range delResponse.Failures {
				log.Error(fmt.Sprintf("Retry:       retry delete ids (%s) error: %v\n", ids, err.Error()))
				tmpIndex = failResp.Index
				tmpActions = strings.Split(tmpIndex, "_")
				if len(tmpActions) < 2 {
					log.Error(fmt.Sprintf("Invalid:     index name %s invalid\n", tmpIndex))
					continue
				}
				tmpPrefix = GetIndexPrefix(tmpIndex)
				tmpActionName = fmt.Sprintf("%s_delete", strings.TrimRight(tmpPrefix, "_"))
				tmpMetaID = failResp.Id
				tmpDocID = strings.TrimPrefix(tmpMetaID, tmpPrefix)
				if tmpDocID != "" && tmpActionName != "" {
					failIdMap[tmpDocID] = 0
					if retryErr = RetryId(tmpActionName, []string{tmpDocID}); retryErr != nil {
						log.Error(fmt.Sprintf("Retry:       retry delete ids (%s) for (%s) error: %v\n", tmpDocID, tmpActionName, retryErr.Error()))
					}
				}
			}
		}
		// 写成文档删除的, id不必重试, 无论redis中是否存在,都进行删除
		for successID, _ = range idMap {
			if _, ok = failIdMap[successID]; !ok {
				successIdArr = append(successIdArr, fmt.Sprintf("%s:%s", actionName, successID))
			}
		}
		if len(successIdArr) > 0 {
			Del(successIdArr)
		}
		idMap = make(map[string]int64)
		failIdMap = make(map[string]int64)
		successIdArr = make([]string, 0)
	}
	return true
}

func GetIndexPrefix(index string) string {
	var (
		prefix    string
		r         *regexp.Regexp
		res, strs []string
	)
	r, _ = regexp.Compile("([a-z_]*).*")
	res = r.FindStringSubmatch(index)
	if len(res) > 1 {
		strs = strings.Split(res[1], "_")
		if len(strs) > 1 {
			prefix = strs[0] + "_" + strs[1]
		}
	}
	if prefix != "" {
		prefix = prefix + "_"
	}
	return prefix
}

//向elasticsearch写入数据
func WriteToES(index string, in interface{}, number chan struct{}, writeType WriteType, counter *int32, wg *sync.WaitGroup) {
	defer wg.Done()
	var (
		count uint32
		v     reflect.Value
		val   interface{}
	)
	for {
		number <- struct{}{}
		if judgeCpu() {
			v = reflect.ValueOf(in)
			if v.Kind() != reflect.Chan {
				log.Panic(fmt.Sprintf("not chan\n"))
			}
			val = v.Interface()
			switch val.(type) {
			case chan []interface{}:
				datas := <-val.(chan []interface{})
				//log.Info(fmt.Sprintf("data len %d ", len(datas)))
				if len(datas) == 0 {
					<-number
					return
				}
				atomic.AddUint32(&count, 1)
				wg.Add(1)
				go WriteIntoES(index, datas, number, count, writeType, counter, wg)
			default:
				<-number
			}
		}
	}
}

func WriteIntoES(index string, datas interface{}, number chan struct{}, count uint32, writeType WriteType, counter *int32, wg *sync.WaitGroup) {
	defer wg.Done()
	var err error
	switch writeType {
	case Insert:
		err = CreateChunk(datas, index)
	case Upsert:
		err = CreateUpdate(datas, index)
	case UpField:
		err = UpdateField(datas, index)
	}

	if err != nil {
		log.Error(fmt.Sprintf("%6dth write into elasticsearch failed: %v\n", count, err.Error()))
	}
	log.Info(fmt.Sprintf("Writing:%6dth write %4d datas into elasticsearch success,there are %4d goroutines now\n", count, reflect.ValueOf(datas).Len(), runtime.NumGoroutine()))
	atomic.AddInt32(counter, -1)
	<-number
}

func WriteEnd(counter *int32, in interface{}, wg *sync.WaitGroup) {
	defer wg.Done()
	var (
		v   reflect.Value
		val interface{}
	)
	for {
		// 不加此行,代码不会走进 *counter == 0
		_ = fmt.Sprintf("counter: %d", *counter)
		//log.Info(fmt.Sprintf("counter: %d", p))
		if *counter == 0 {
			//log.Info(fmt.Sprintf("counter 走到零 : %d", *counter))
			v = reflect.ValueOf(in)
			if v.Kind() != reflect.Chan {
				log.Panic(fmt.Sprintf("not chan\n"))
			}
			val = v.Interface()
			switch val.(type) {
			case chan []interface{}:
				val.(chan []interface{}) <- []interface{}{}
			}
			return
		}
	}
}

// 获取leader集群名称及数据传输节点列表
func GetNodesStats() (clusterName string, transAddrs []string, err error) {
	var (
		nodesStatsResp *elastic.NodesStatsResponse
		node           *elastic.NodesStatsNode
	)
	transAddrs = make([]string, 0)
	if nodesStatsResp, err = client.NodesStats().Do(context.Background()); err != nil {
		log.Error("Failed to get leader cluster nodes stats info,err: " + err.Error())
		return
	}
	clusterName = nodesStatsResp.ClusterName
	for _, node = range nodesStatsResp.Nodes {
		transAddrs = append(transAddrs, node.TransportAddress)
	}
	return
}

// 获取follow集群的_cluster/settings ->persistent.cluster.remote  []
func GetClusterSettings(hostStr, portStr, username, password string) (remotes map[string]map[string][]string, err error) {
	var (
		ok                                                 bool
		key, tmpKey                                        string
		body                                               []byte
		bodyMap, persistMap, clusterMap, remoteMap, tmpVal map[string]interface{}
		in, tmpIn, tmpSeedIn                               interface{}
		tmpSlice                                           []interface{}
		tmpSeedsSlice                                      []string
		tmpSeedsMap                                        map[string][]string
	)
	body = make([]byte, 0)
	if bodyMap, err = SendElasticRequest("GET",
		fmt.Sprintf("http://%s:%s/_cluster/settings", hostStr, portStr),
		username, password, body); err != nil {
		return
	}

	if _, ok = bodyMap["persistent"]; ok {
		if persistMap, ok = bodyMap["persistent"].(map[string]interface{}); ok {
			if _, ok = persistMap["cluster"]; ok {
				if clusterMap, ok = persistMap["cluster"].(map[string]interface{}); ok {
					if _, ok = clusterMap["remote"]; ok {
						if remoteMap, ok = clusterMap["remote"].(map[string]interface{}); ok {
							// 有远程集群配置时遍历取出
							if len(remoteMap) > 0 {
								remotes = make(map[string]map[string][]string)
								for key, in = range remoteMap {
									if tmpVal, ok = in.(map[string]interface{}); ok {
										tmpSeedsMap = make(map[string][]string)
										tmpSeedsSlice = make([]string, 0)
										for tmpKey, tmpIn = range tmpVal {
											if tmpSlice, ok = tmpIn.([]interface{}); ok {
												for _, tmpSeedIn = range tmpSlice {
													tmpSeedsSlice = append(tmpSeedsSlice, tmpSeedIn.(string))
												}
												tmpSeedsMap[tmpKey] = tmpSeedsSlice
											}
										}
										remotes[key] = tmpSeedsMap
									}
								}
							}
						}
					}
				}
			}
		}
	}
	return
}

func PutClusterSettings(hostStr, portStr, username, password string, body []byte) (err error) {
	var (
		ok, success bool
		bodyMap     map[string]interface{}
	)
	if bodyMap, err = SendElasticRequest("PUT",
		fmt.Sprintf("http://%s:%s/_cluster/settings", hostStr, portStr),
		username, password, body); err != nil {
		return
	}

	if len(bodyMap) > 0 {
		if _, ok = bodyMap["acknowledged"]; ok {
			if success, ok = bodyMap["acknowledged"].(bool); !ok || !success {
				err = errors.New("put cluster settings failed")
			}
		}
	} else {
		err = errors.New("without any response")
	}
	return
}

// 获取follow集群的_remote/info
func GetRemoteInfo(hostStr, portStr, username, password string) (remoteInfo map[string]map[string]interface{}, err error) {
	var (
		ok              bool
		key             string
		body            []byte
		bodyMap, tmpMap map[string]interface{}
		in              interface{}
	)
	body = make([]byte, 0)
	if bodyMap, err = SendElasticRequest("GET",
		fmt.Sprintf("http://%s:%s/_remote/info", hostStr, portStr),
		username, password, body); err != nil {
		return
	}

	if len(bodyMap) > 0 {
		remoteInfo = make(map[string]map[string]interface{})
		for key, in = range bodyMap {
			if tmpMap, ok = in.(map[string]interface{}); ok {
				remoteInfo[key] = tmpMap
			}
		}
	}
	return
}

// 获取指定索引名的follow信息, 只关心 错误时: error->type[index_not_found_exception]; 正确时: followr_index, remote_cluster, leader_index, status 字段
func GetFollowInfoByIndex(hostStr, portStr, username, password, indexName string) (followInfo FollowInfo, err error) {
	var (
		ok              bool
		key             string
		body            []byte
		bodyMap, tmpMap map[string]interface{}
		in, tmpIn       interface{}
		inSlice         []interface{}
		//errMap map[string]interface{}
		//errIn interface{}
	)
	body = make([]byte, 0)
	if bodyMap, err = SendElasticRequest("GET",
		fmt.Sprintf("http://%s:%s/%s/_ccr/info", hostStr, portStr, indexName),
		username, password, body); err != nil {
		return
	}

	/*if _, ok = bodyMap["error"]; ok {
		if errMap, ok = bodyMap["error"].(map[string]interface{}); ok {
			for key, errIn = range errMap {
				if key == "reason" {
					err = errors.New(errIn.(string))
					goto EndFor
				}
				//if key == "type" && errIn.(string) == "index_not_found_exception" {
				//
				//}
			}
		} else {
			err = errors.New("get follow info encouter unknown error")
		}
	EndFor:
		return
	}*/

	if _, ok = bodyMap["follower_indices"]; ok {
		if inSlice, ok = bodyMap["follower_indices"].([]interface{}); ok {
			for _, in = range inSlice {
				if tmpMap, ok = in.(map[string]interface{}); ok {
					for key, tmpIn = range tmpMap {
						if key == "leader_index" && tmpIn.(string) == indexName {
							followInfo = FollowInfo{}
							if _, ok = tmpMap["follower_index"]; ok {
								followInfo.FollowerIndex = tmpMap["follower_index"].(string)
							}
							if _, ok = tmpMap["remote_cluster"]; ok {
								followInfo.RemoteCluster = tmpMap["remote_cluster"].(string)
							}
							if _, ok = tmpMap["leader_index"]; ok {
								followInfo.LeaderIndex = tmpMap["leader_index"].(string)
							}
							if _, ok = tmpMap["status"]; ok {
								followInfo.Status = tmpMap["status"].(string)
							}
							goto EndReturn
						} else {
							continue
						}
					}
				}
			}
		}
	}
EndReturn:
	return
}

// 根据指定的索引名称关闭follow信息
func CloseFollowByIndexName(hostStr, portStr, username, password, indexName string) (err error) {
	// 步骤: pause_follow, close, unfollow, open (暂时不执行open, 用删除该索引来替代open,避免重设follow时重名冲突)
	var (
		ok, ack         bool
		key, errTypeStr string
		body            []byte
		bodyMap         map[string]interface{}
		in              interface{}
		errMap          map[string]interface{}
		followInfo      FollowInfo
	)

	// 先检查是否有此follow信息,有则继续关闭
	if followInfo, err = GetFollowInfoByIndex(hostStr, portStr, username, password, indexName); err != nil {
		return
	}

	if followInfo.LeaderIndex != indexName {
		log.Info(fmt.Sprintf("without any follow info: %s", indexName))
		return
	}

	body = make([]byte, 0)
	if bodyMap, err = SendElasticRequest("POST",
		fmt.Sprintf("http://%s:%s/%s/_ccr/pause_follow", hostStr, portStr, indexName),
		username, password, body); err != nil {
		return
	}

	// pause_follow 必须返回 acknowledged=true 再执行下一步
	if _, ok = bodyMap["error"]; ok {
		if errMap, ok = bodyMap["error"].(map[string]interface{}); ok {
			for key, in = range errMap {
				if key == "reason" {
					err = errors.New(in.(string))
					return
				}
			}
		}
	}

	if _, ok = bodyMap["acknowledged"]; ok {
		if ack, ok = bodyMap["acknowledged"].(bool); !ok || !ack {
			err = errors.New(fmt.Sprintf("pause_follow get false acknowledged: %s", indexName))
			return
		}
	} else {
		err = errors.New(fmt.Sprintf("pause_follow can not get acknowledged response"))
		return
	}

	// _close
	if bodyMap, err = SendElasticRequest("POST",
		fmt.Sprintf("http://%s:%s/%s/_close", hostStr, portStr, indexName),
		username, password, body); err != nil {
		return
	}
	// _close 必须返回 acknowledged=true 再执行下一步
	if _, ok = bodyMap["error"]; ok {
		if errMap, ok = bodyMap["error"].(map[string]interface{}); ok {
			for key, in = range errMap {
				if key == "reason" {
					err = errors.New(in.(string))
					return
				}
			}
		}
	}

	if _, ok = bodyMap["acknowledged"]; ok {
		if ack, ok = bodyMap["acknowledged"].(bool); !ok || !ack {
			err = errors.New(fmt.Sprintf("close %s get false acknowledged", indexName))
			return
		}
	} else {
		err = errors.New(fmt.Sprintf("close can not get acknowledged response"))
		return
	}

	// 解除follow
	if bodyMap, err = SendElasticRequest("POST",
		fmt.Sprintf("http://%s:%s/%s/_ccr/unfollow", hostStr, portStr, indexName),
		username, password, body); err != nil {
		return
	}

	// unfollow 必须返回 acknowledged=true 再执行下一步
	if _, ok = bodyMap["error"]; ok {
		if errMap, ok = bodyMap["error"].(map[string]interface{}); ok {
			for key, in = range errMap {
				if key == "reason" {
					err = errors.New(in.(string))
					return
				}
			}
		}
	}

	if _, ok = bodyMap["acknowledged"]; ok {
		if ack, ok = bodyMap["acknowledged"].(bool); !ok || !ack {
			err = errors.New(fmt.Sprintf("unfollow %s get false acknowledged", indexName))
			return
		}
	} else {
		err = errors.New(fmt.Sprintf("unfollow can not get acknowledged response"))
		return
	}

	/*
		// 重开索引, 当前业务不需要重开, 先写在此处预留
		urlStr = fmt.Sprintf("http://%s:%s/%s/_open", hostStr, portStr, indexName)
		req.URL, err =  url.Parse(urlStr)
		if resp, err = client.Do(req); err != nil {
			return
		}
		defer resp.Body.Close()
		if body, err = ioutil.ReadAll(resp.Body); err != nil {
			return
		}
		if err = json.Unmarshal(body, &bodyMap); err != nil {
			return
		}

		// open 必须返回 acknowledged=true 再执行下一步
		if _, ok = bodyMap["error"]; ok {
			if errMap, ok = bodyMap["error"].(map[string]interface{}); ok {
				for key, in = range errMap {
					if key == "reason" {
						err = errors.New(in.(string))
						return
					}
				}
			}
		}

		if _, ok = bodyMap["acknowledged"]; ok {
			if ack, ok = bodyMap["acknowledged"].(bool); !ok || !ack {
				err = errors.New(fmt.Sprintf("open %s get false acknowledged", indexName))
				return
			}
		} else {
			err = errors.New(fmt.Sprintf("open can not get acknowledged response"))
			return
		}
	*/

	// 删除索引
	if bodyMap, err = SendElasticRequest("DELETE",
		fmt.Sprintf("http://%s:%s/%s", hostStr, portStr, indexName),
		username, password, body); err != nil {
		return
	}

	// delete 必须返回 acknowledged=true 再执行下一步
	if _, ok = bodyMap["error"]; ok {
		if errMap, ok = bodyMap["error"].(map[string]interface{}); ok {
			if _, ok = errMap["type"]; ok {
				if errTypeStr, ok = errMap["type"].(string); ok {
					if errTypeStr != "index_not_found_exception" {
						if _, ok = bodyMap["reason"]; ok {
							err = errors.New(bodyMap["reason"].(string))
						} else {
							err = errors.New("delete occur unknown error")
						}
						return
					}
				}
			}
		}
	}

	if _, ok = bodyMap["acknowledged"]; ok {
		if ack, ok = bodyMap["acknowledged"].(bool); !ok || !ack {
			err = errors.New(fmt.Sprintf("delete %s get false acknowledged", indexName))
			return
		}
	} else {
		err = errors.New(fmt.Sprintf("delete %s can not get acknowledged response", indexName))
		return
	}

	return
}

// 根据指定索引名称、leader集群名称设置follow信息, 将follow复制的索引名称与leader所在的索引名称保持一致
func SetFollowInfoByIndex(hostStr, portStr, username, password, remoteCluster, indexName string) (err error) {
	var (
		ok              bool
		key             string
		body            []byte
		bodyMap, errMap map[string]interface{}
		in              interface{}
	)
	bodyMap = map[string]interface{}{
		"remote_cluster": remoteCluster,
		"leader_index":   indexName,
	}
	if body, err = json.Marshal(&bodyMap); err != nil {
		return
	}

	if bodyMap, err = SendElasticRequest("PUT",
		fmt.Sprintf("http://%s:%s/%s/_ccr/follow", hostStr, portStr, indexName),
		username, password, body); err != nil {
		return
	}

	if _, ok = bodyMap["error"]; ok {
		if errMap, ok = bodyMap["error"].(map[string]interface{}); ok {
			for key, in = range errMap {
				if key == "reason" {
					err = errors.New(in.(string))
					break
				}
			}
		}
	}
	return
}

func SetFollowReplicas(hostStr, portStr, username, password, indexName string, number int8) (err error) {
	var (
		ok, success     bool
		key             string
		bodyMap, errMap map[string]interface{}
		in              interface{}
	)
	if bodyMap, err = SendElasticRequest("PUT",
		fmt.Sprintf("http://%s:%s/%s/_settings", hostStr, portStr, indexName),
		username, password,
		[]byte(fmt.Sprintf(`{"number_of_replicas": "%d"}`, number))); err != nil {
		return
	}

	if _, ok = bodyMap["error"]; ok {
		if errMap, ok = bodyMap["error"].(map[string]interface{}); ok {
			for key, in = range errMap {
				if key == "reason" {
					err = errors.New(in.(string))
					return
				}
			}
		}
	}

	if _, ok = bodyMap["acknowledged"]; ok {
		if success, ok = bodyMap["acknowledged"].(bool); !ok || !success {
			err = errors.New("put replicas settings failed")
		}
	}
	return
}
