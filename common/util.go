package common

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	log "github.com/sirupsen/logrus"
	"go-mysql-elasticsearch/config"
	"go-mysql-elasticsearch/datamodels"
	"io"
	"os"
	"path"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// 动态调用函数
func CallFunc(m map[string]interface{}, name string, params ...interface{}) ([]reflect.Value, error) {
	if _, ok := m[name]; !ok {
		return nil, errors.New(fmt.Sprintf("map without %s value", name))
	}
	f := reflect.ValueOf(m[name])
	if len(params) != f.Type().NumIn() {
		return nil, errors.New("the number of input params not match")
	}
	in := make([]reflect.Value, len(params))
	for k, v := range params {
		in[k] = reflect.ValueOf(v)
	}
	return f.Call(in), nil
}

// 获取三段式操作指令
func GetActionSegments(action string) ([3]string, error) {
	var (
		actionArr [3]string
		k         int
		v         string
		strs      []string
	)
	strs = strings.Split(action, "_")
	if len(strs) > 3 || len(strs) < 2 {
		return actionArr, errors.New("action invalid")
	}
	for k, v = range strs {
		actionArr[k] = v
	}
	return actionArr, nil
}

func JudgePositiveInt(number string) bool {
	if number != "" {
		matched, err := regexp.Match("^[1-9]\\d*$", []byte(number))
		if err != nil || !matched {
			return false
		}
	}
	return true
}

// 判断ids传参是否合法
func JudgeIds(ids string) bool {
	if ids != "" {
		matched, err := regexp.Match("^\\d+(,*\\d+)*$", []byte(ids))
		if err != nil || !matched {
			return false
		}
	}
	return true
}

// 判断start_time, end_time传参是否合法
func GetTimeRange(timeRange [2]string) (timeArray [2]int64, err error) {
	var (
		key       int
		dateStr   string
		timeInt   int64
		isLegal   = true // 当遇到任意一个时间小于等于0, 或者结束时间小于等于开始时间就为false
		localTime *time.Location
		tmpTime   time.Time
	)
	timeArray = [2]int64{0, 0}
	if localTime, err = time.LoadLocation("Local"); err != nil {
		return
	}
	for key, dateStr = range timeRange {
		dateStr = strings.TrimSpace(dateStr)
		if dateStr != "" {
			if tmpTime, err = time.ParseInLocation("2006-01-02 15:04:05", dateStr, localTime); err != nil {
				return
			} else {
				timeArray[key] = tmpTime.Unix()
			}
		}
	}

	// 时间范围要么同时都为0, 要么同时都大于0并且 end_time 要大于 start_time
	if (timeArray[0] == 0) && (timeArray[1] == 0) {
		return
	}

	for _, timeInt = range timeArray {
		if timeInt <= 0 {
			isLegal = false
			break
		}
	}

	if isLegal {
		if timeArray[1] <= timeArray[0] {
			isLegal = false
		}
	}

	if !isLegal {
		err = errors.New("time must be positive integer and end_time must greater than start_time")
		return
	}

	return
}

// 判断开始时间和结束时间是否都是0
func BothZero(timeArray [2]int64) bool {
	if (timeArray[0] == 0) && (timeArray[1] == 0) {
		return true
	}
	return false
}

// 生成可支持的操作指令
func genOpts() (opts string) {
	var (
		p, i, s, line string
		prefix        = []string{}
		indices       = []string{"media"}
		suffix        = [4]string{"", "update", "delete", "ccr"}
	)
	for _, p = range prefix {
		for _, i = range indices {
			line = ""
			for _, s = range suffix {
				if s == "" {
					line += p + "_" + i + ","
				} else {
					line += p + "_" + i + "_" + s + ","
				}
			}
			opts += "\n                        " + strings.TrimRight(line, ",")
		}
	}
	return
}

// 判断是否支持操作指令
func JudgeSupportAction(actionArr [3]string) (isValid bool) {
	var (
		action, l, opt, tmpOpt string
		options, lines         []string
	)
	if actionArr[2] == "" {
		action = actionArr[0] + "_" + actionArr[1]
	} else {
		action = actionArr[0] + "_" + actionArr[1] + "_" + actionArr[2]
	}
	lines = strings.Split(genOpts(), "\n")
	for _, l = range lines {
		for _, opt = range strings.Split(l, ",") {
			tmpOpt = strings.Trim(opt, " ")
			if tmpOpt != "" {
				options = append(options, tmpOpt)
			}
		}
	}
	for _, opt = range options {
		if action == opt {
			isValid = true
			break
		}
	}
	return
}

func SetIds(actionArr [3]string, ids string) string {
	// 只有新增和删除时需要支持ids参数
	if ids != "" && (actionArr[2] == "" || actionArr[2] == "delete") {
		ids = strings.Trim(strings.ReplaceAll(strings.ReplaceAll(ids, "，", ","), " ", ""), ",")
	} else {
		ids = ""
	}
	return ids
}

func GenActionName(actionArr [3]string) string {
	return actionArr[0] + "_" + actionArr[1]
}

func GenIndexName(actionArr [3]string, ids string) string {
	var indexName = actionArr[0] + "_" + actionArr[1]
	if ids == "" {
		indexName = indexName + time.Now().In(time.FixedZone("CST", 8*3600)).Format("20060102150405")
	}
	if config.Config.ENV == "dev" {
		indexName = indexName + "_dev"
	}
	return indexName
}

// 生成日志索引名称
func GenLogIndexName(actionArr [3]string) string {
	var indexName = actionArr[0] + "_" + actionArr[1] + time.Now().In(time.FixedZone("CST", 8*3600)).Format("200601")
	if config.Config.ENV == "dev" {
		indexName = indexName + "_dev"
	}
	return indexName
}

func GenAliasName(actionArr [3]string) string {
	var indexName = actionArr[0] + "_" + actionArr[1]
	if config.Config.ENV == "dev" {
		indexName = indexName + "_dev"
	}
	return indexName
}

func GenSpecifyAlias(actionArr [3]string) string {
	var specifyAlias = actionArr[0]
	if config.Config.ENV == "dev" {
		specifyAlias = specifyAlias + "_dev"
	}
	return specifyAlias
}

func CreateFile(pathStr string) bool {
	var (
		err error
		dir string
		f   *os.File
	)
	if !Exists(pathStr) {
		dir = path.Dir(pathStr)
		// 如果目录不存在则创建目录
		if !IsDir(dir) {
			if err = os.MkdirAll(dir, 0666); err != nil {
				log.Panic(fmt.Sprintf("mkdir %s failed", dir))
			}
		}
		if f, err = os.Create(pathStr); err != nil {
			return false
		}
		defer f.Close()
		if err = os.Chmod(pathStr, 0666); err != nil {
			return false
		}
	}
	return true
}

// 判断是否目录
func IsDir(path string) bool {
	var (
		err error
		s   os.FileInfo
	)
	if s, err = os.Stat(path); err != nil {
		return false
	}
	return s.IsDir()
}

//os.Stat获取文件信息, 判断文件是否存在
func Exists(path string) bool {
	var err error
	if _, err = os.Stat(path); err != nil {
		if os.IsExist(err) {
			return true
		}
		return false
	}
	return true
}

// 替换文件名
func ReplaceFile(oldFile, newFile string) (err error) {
	var (
		msg     string
		f       *os.File
		bufRead *bufio.Reader
		line    []byte
		tmpLine string
		isNeed  = false
	)
	// 判断新文件是否存在
	if !Exists(newFile) {
		msg = fmt.Sprintf("file %s not exists", newFile)
		log.Error(msg)
		err = errors.New(msg)
		return
	}

	// 如果新文件没有内容, 则删除新文件
	if f, err = os.Open(newFile); err != nil {
		log.Error(err.Error())
		return
	}

	bufRead = bufio.NewReader(f)
	for {
		line, _, err = bufRead.ReadLine()
		tmpLine = strings.Trim(strings.TrimSpace(string(line)), ",")
		if tmpLine != "" {
			isNeed = true
			break
		}
		if err == io.EOF {
			break
		}
	}
	f.Close()

	if !isNeed {
		DeleteFile(newFile)
		msg = fmt.Sprintf("delete the empty file %s", newFile)
		log.Error(msg)
		err = errors.New(msg)
		return
	} else {
		if !DeleteFile(oldFile) {
			msg = fmt.Sprintf("delete file %s failed", oldFile)
			log.Error(msg)
			err = errors.New(msg)
			return
		} else {
			// 将新文件改名成旧文件
			if err = os.Rename(newFile, oldFile); err != nil {
				log.Error(err.Error())
			} else {
				log.Info(fmt.Sprintf("rename to %s success", oldFile))
			}
			return
		}
	}
}

// 删除文件
func DeleteFile(path string) bool {
	var err error
	if Exists(path) {
		if err = os.Remove(path); err != nil {
			log.Error(fmt.Sprintf("Deleted     file %s failed\n", path))
			return false
		} else {
			log.Info(fmt.Sprintf("Deleted     file %s success\n", path))
		}
	}
	return true
}

func SaveDict(filename string, in chan []interface{}, number chan struct{}, counter *int32, wg *sync.WaitGroup, rw *sync.RWMutex) (err error) {
	defer wg.Done()
	var (
		dictDatas []*datamodels.Dict
		v         reflect.Value
		val       interface{}
	)

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
			wg.Add(1)
			go DoSave(filename, dictDatas, number, counter, wg, rw)
		}
	}
}

func DoSave(filename string, dictDatas []*datamodels.Dict, number chan struct{}, counter *int32, wg *sync.WaitGroup, rw *sync.RWMutex) {
	defer wg.Done()
	defer func() {
		atomic.AddInt32(counter, -1)
		<-number
	}()
	var (
		err   error
		file  *os.File
		write *bufio.Writer
		dict  *datamodels.Dict
	)
	if filename == "" {
		log.Error("filename is empty")
		return
	}
	if !CreateFile(filename) {
		log.Error("create dict file failed")
		return
	}

	// 异步写入文件加锁
	rw.Lock()
	defer rw.Unlock()

	if file, err = os.OpenFile(filename, os.O_WRONLY|os.O_APPEND, 0666); err != nil {
		log.Error(fmt.Sprintf("failed to open file: %v, cause: %v\n", filename, err.Error()))
		return
	}
	//及时关闭file句柄
	defer file.Close()
	//写入文件时，使用带缓存的 *Writer
	write = bufio.NewWriter(file)
	for _, dict = range dictDatas {
		log.Info(fmt.Sprintf("get word：%s ", dict.Word))
		write.WriteString(dict.Word + "\r\n")
	}
	//Flush将缓存的文件真正写入到文件中
	write.Flush()
	return
}

func combinePrefix(prefix, placeHolder string) string {
	var (
		prefixArr  []string
		k          int
		v, destStr string
	)
	prefixArr = strings.Split(prefix, "_")
	for k, v = range prefixArr {
		if k == len(prefixArr)-2 {
			v = placeHolder + v
		}
		if k == len(prefixArr)-1 {
			destStr += v
		} else {
			destStr += v + "_"
		}
	}
	return destStr
}

func TransferToVideoMix(dispatch *datamodels.Dispatch) (videoMix *datamodels.VideoMix, err error) {
	var injectBrands = make([]string, 0)
	// 如果fd_hbcmcc_video表中禁用了该video
	if dispatch.Enable == 1 {
		// 湖北移动 brand_id=3265
		injectBrands = append(injectBrands, "3265")
	}
	videoMix = &datamodels.VideoMix{
		Video: datamodels.Video{
			VideoID:   dispatch.VideoID,
			VersionID: dispatch.VersionID,
		},
		InjectBrands: injectBrands,
	}
	return
}

func TransferToDict(datas []interface{}) (dictArray []*datamodels.Dict, err error) {
	var (
		msg, word  string
		v          reflect.Value
		l          int
		val        interface{}
		updateTime int64
		tmpVal     *datamodels.Dict
	)
	dictArray = make([]*datamodels.Dict, 0)
	v = reflect.ValueOf(datas)
	if v.Kind() != reflect.Slice {
		msg = "not slice, can not transfer to dict"
		log.Error(msg)
		err = errors.New(msg)
		return
	}
	l = v.Len()
	for i := 0; i < l; i++ {
		val = v.Index(i).Interface()
		switch val.(type) {
		case *datamodels.VideoMix:
			word = val.(*datamodels.VideoMix).Name
			updateTime = val.(*datamodels.VideoMix).UpdateTime
		case *datamodels.MediaMix:
			word = val.(*datamodels.MediaMix).Name
			updateTime = val.(*datamodels.MediaMix).UpdateTime
		case *datamodels.AnchorMix:
			word = val.(*datamodels.AnchorMix).Name
			updateTime = val.(*datamodels.AnchorMix).UpdateTime
		case *datamodels.Topic:
			word = val.(*datamodels.Topic).Name
			updateTime = val.(*datamodels.Topic).UpdateTime
		case *datamodels.Actor:
			word = val.(*datamodels.Actor).Name
			updateTime = val.(*datamodels.Actor).UpdateTime
		case *datamodels.Dict:
			word = val.(*datamodels.Dict).Word
			updateTime = val.(*datamodels.Dict).UpdateTime
		}
		tmpVal = &datamodels.Dict{
			Word:       word,
			UpdateTime: updateTime,
		}
		dictArray = append(dictArray, tmpVal)
	}
	return
}

func TransferChannel(datas map[int64]interface{}) (channelMap map[int64]*datamodels.Channel, err error) {
	var (
		k int64
		v interface{}
	)
	channelMap = make(map[int64]*datamodels.Channel)
	for k, v = range datas {
		switch v.(type) {
		case *datamodels.Channel:
			channelMap[k] = v.(*datamodels.Channel)
		}
	}
	return
}

func RetryId(action string, ids []string) (err error) {
	// 先查出redis是否有此 key=>value "操作:id" => times  如 lemon_media:24522 => 2
	// 如果没有此缓存 或者 尝试的次数不足指定次数(比如 3 次),
	// 则将此 "操作:id" 加入有序集合并给最高分数优先执行, 累加尝试次数并更新到缓存中设置有效期
	// 设置缓存有效期原因: 如果重试成功了, 缓存到期自动释放;
	// 如果重试失败再次进来重试时可以比较尝试次数是否达到上限
	var (
		key, id, numberStr, actionWithID string
		number                           int
		idMap                            = make(map[string]int)
	)
	for _, id = range ids {
		key = fmt.Sprintf("%s:%s", action, id)
		if numberStr, err = GetByKey(key); err != nil {
			// 如果是 redis: nil 表示不存在此键
			if err.Error() == "redis: nil" {
				if err = SetEX(key, 1, 3600); err != nil {
					log.Error(fmt.Sprintf("SetTimes:    set %dth retry for %s failed: %s", 1, key, err.Error()))
					continue
				}
				// 设置第 1 次重试
				idMap[key] = 1
			} else {
				log.Error(fmt.Sprintf("Redis:       get %s from redis failed: %s", key, err.Error()))
				continue
			}
		} else {
			// 如果存在则比较次数
			if number, err = strconv.Atoi(numberStr); err != nil {
				Del([]string{key})
				log.Error(fmt.Sprintf("Convert:     convert %s to integer failed: %s", numberStr, err.Error()))
				continue
			}
			if number >= config.Config.JobRetry {
				// 如果有此缓存并且缓存次数达到重试次数上限,则清除该缓存并记录错误信息
				Del([]string{key})
				log.Info(fmt.Sprintf("Exceed:      exceeds the maximum retry times: %s", key))
				continue
			} else {
				// 继续重试
				log.Info(fmt.Sprintf("Adding:      add %s to %dth retry", key, number+1))
				// 重设次数
				if err = SetEX(key, number+1, 3600); err != nil {
					log.Error(fmt.Sprintf("SetTimes:    set %dth retry for %s failed: %s", number+1, key, err.Error()))
					continue
				}
				idMap[key] = number + 1
			}
		}
	}

	// 将剩余的id赋优先级进行重试
	if len(idMap) > 0 {
		var (
			member      string
			members     = make([]*redis.Z, 0)
			memberSlice = make([]string, 0)
		)

		for actionWithID, number = range idMap {
			member = fmt.Sprintf("%s@%d", actionWithID, number)
			memberSlice = append(memberSlice, member)
			members = append(members, &redis.Z{
				Score:  0, // 赋最高优先级
				Member: member,
			})
		}
		if len(members) > 0 {
			if _, err = ZAdd(config.Config.RedisConfig.JobKey, members); err != nil {
				log.Error(fmt.Sprintf("Members:     zadd members %s failed: %s", strings.Join(memberSlice, ","), err.Error()))
			}
		}
	}
	return
}

func RuleKey(database, table string) string {
	return strings.ToLower(database + ":" + table)
}

func Uint64ToBytes(u uint64) []byte {
	var buf = make([]byte, 8)
	binary.BigEndian.PutUint64(buf, u)
	return buf
}

func ToFloat64Safe(str string) float64 {
	var (
		tmpFloat64 float64
		err        error
	)
	if tmpFloat64, err = strconv.ParseFloat(str, 64); err != nil {
		return 0
	}
	return tmpFloat64
}

func ConvertToString(value interface{}) string {
	var (
		err   error
		key   string
		bytes []byte
	)
	if value == nil {
		return key
	}

	switch value.(type) {
	case float64:
		key = strconv.FormatFloat(value.(float64), 'f', -1, 64)
	case float32:
		key = strconv.FormatFloat(float64(value.(float32)), 'f', -1, 64)
	case int:
		key = strconv.Itoa(value.(int))
	case uint:
		key = strconv.Itoa(int(value.(uint)))
	case int8:
		key = strconv.Itoa(int(value.(int8)))
	case uint8:
		key = strconv.Itoa(int(value.(uint8)))
	case int16:
		key = strconv.Itoa(int(value.(int16)))
	case uint16:
		key = strconv.Itoa(int(value.(uint16)))
	case int32:
		key = strconv.Itoa(int(value.(int32)))
	case uint32:
		key = strconv.Itoa(int(value.(uint32)))
	case int64:
		key = strconv.FormatInt(value.(int64), 10)
	case uint64:
		key = strconv.FormatUint(value.(uint64), 10)
	case string:
		key = value.(string)
	case []byte:
		key = string(value.([]byte))
	default:
		if bytes, err = json.Marshal(value); err == nil {
			key = string(bytes)
		}
	}

	return key
}

func ReplaceMapValue(value interface{}, keyDot string, replaceMap map[string]interface{}) interface{} {
	var (
		ok               bool
		key, tmpKey      string
		in               interface{}
		valueMap, newMap map[string]interface{}
	)
	if valueMap, ok = value.(map[string]interface{}); ok {
		newMap = make(map[string]interface{})
		for key, in = range valueMap {
			tmpKey = strings.TrimPrefix(fmt.Sprintf("%s.%s", keyDot, key), ".")
			if _, ok = replaceMap[tmpKey]; ok {
				newMap[key] = replaceMap[tmpKey]
			} else {
				newMap[key] = ReplaceMapValue(in, tmpKey, replaceMap)
			}
		}
		return newMap
	}

	return value
}

func Usage() {
	var consoleStr = `
	go-mysql-elasticsearch version: 1.0.0
	Usage: go-mysql-elasticsearch [-h | -op | -ids | -start_time | -end_time]
           e.g.       : ./go-mysql-elasticsearch -op=lemon_media -ids=123,456,789

           -h         : this help
           -yield     : produce from binlog
           -job       : boot up the job watcher
			-driver   : choose a job driver [rabbitmq | redis(defalut)] as per your requirement
           -ids       : input ids joined with comma, insert and delete support that
           -resume_id : input id as positive integer, insert support that
           -start_time: input start time, such as '2021-07-06 15:12:24'
           -end_time  : input end time, such as '2021-07-07 17:06:31'
           -op        : input action that you wanna perform %s
                        position: get binlog posistion or set while pass -filename and -pos 
                        clear_version: reset the increment version id to zero
`
	fmt.Fprintf(os.Stdout, fmt.Sprintf(consoleStr, genOpts()))
	//flag.PrintDefaults()
}
