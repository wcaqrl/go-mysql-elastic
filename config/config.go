package config

import (
	"database/sql"
	"fmt"
	"github.com/tietang/props/ini"
	"os"
	"path"
	"regexp"
	"runtime"
	"strings"
)

type MysqlConfig struct {
	Host           string
	Port           string
	User           string
	Password       string
	Charset        string
	SlaveID        int
	Flavor         string
	SkipNoPkTable  bool
	SkipMasterData bool
	DumpPath       string
	Dsn            map[string]string
	Perpage        int32
	SqlNumbers     int32
	ChunkNumbers   chan struct{}
	UpdateNumbers  chan struct{}
	ESMAXPAGE      int32
	MaxLifeTime    int64
	DbMap          map[string]*sql.DB
}

type ESConfig struct {
	Host            string
	Port            string
	ReplicaHost     string
	ReplicaPort     string
	Username        string
	Password        string
	ESNumbers       chan struct{}
	BulkNumber      int32
	Shards          int8
	Replicas        int8
	FollowReplicas  int8
	RefreshInterval string
	CpuChan         chan map[int64]float64
	CpuPercent      float64
	CpuInterval     int8
	Indices         map[string]map[string]string
}

type RedisConfig struct {
	Addr       string
	Db         int
	Password   string
	JobKey     string
	VersionKey string
}

type LogConfig struct {
	Path         string
	ResponseTime float64
}

type StorageConfig struct {
	FilePath string
}

type SftpConfig struct {
	User         string
	Password     string
	Host         string
	Port         int
	FullDictFile string
	AddDictFile  string
	RemoteDir    string
}

type RuleConfig struct {
	RuleList []map[string]interface{}
}

type AppConfig struct {
	ENV                string
	IP                 string
	Port               string
	Name               string
	Level              string
	LogPath            string
	RootPath           string
	ExecPath           string
	Save               int
	Refresh            int64
	TransferInterval   int
	SnifferInterval    int
	HandlerQueueLength int
	FlushInterval      int
	BulkSize           int
	Period             int64
	DictPeriod         int64
	IsFull             bool
	JobRetry           int
	JobChan            chan struct{}
	CtlProps           map[string]map[string]map[int64]interface{}
	MysqlConfig
	ESConfig
	RedisConfig
	LogConfig
	StorageConfig
	RuleConfig
	SftpConfig
}

var Config *AppConfig

func loadDsn(conf *ini.IniFileConfigSource) map[string]string {
	var (
		key, value string
		dsnMap     = make(map[string]string)
		sl         = make([]string, 0)
		dsnRegex   = regexp.MustCompile("mysql\\.(.*_dsn)")
	)
	for key, value = range conf.Values {
		if dsnRegex.Match([]byte(key)) {
			sl = strings.Split(key, ".")
			dsnMap[sl[len(sl)-1]] = value
		}
	}
	return dsnMap
}

// cfg.ESConfig.Indices 处理索引对应的mysql相关信息
func loadIndices(conf *ini.IniFileConfigSource) map[string]map[string]string {
	var (
		key        string
		ok         bool
		indicesMap = make(map[string]map[string]string)
	)
	for _, key = range conf.Keys() {
		if strings.HasPrefix(key, "index_map") {
			var strArr = strings.Split(key, ".")
			if _, ok = indicesMap[strArr[1]]; !ok {
				indicesMap[strArr[1]] = make(map[string]string)
			}
			if _, ok = indicesMap[strArr[1]][strArr[2]]; !ok {
				indicesMap[strArr[1]][strArr[2]] = ""
			}
			indicesMap[strArr[1]][strArr[2]] = conf.GetDefault(key, "")
		}
	}
	return indicesMap
}

func loadRules(conf *ini.IniFileConfigSource) []map[string]interface{} {
	var (
		ok          bool
		key, tmpKey string
		pos         int
		boolMap     = map[string]bool{
			"column_lower_case":          true,
			"column_upper_case":          true,
			"column_underscore_to_camel": true,
		}
		sectionMap = make(map[string]bool)
		keyMap     = make(map[string]bool)
		ruleMap    = make(map[string]map[string]interface{})
		tmpMap     map[string]interface{}
		ruleList   []map[string]interface{}
	)
	// 分组保存各配置
	for _, key = range conf.Keys() {
		if strings.HasPrefix(key, "rule-") {
			//取section名称作分组名
			pos = strings.Index(key, ".")
			if pos > 0 {
				if _, ok = sectionMap[key[:pos]]; !ok {
					sectionMap[key[:pos]] = true
				}
				if _, ok = keyMap[key[pos+1:]]; !ok {
					keyMap[key[pos+1:]] = true
				}
				//keyList = append(keyList, key[pos+1:])
			}
		}
	}

	for key = range sectionMap {
		if _, ok = ruleMap[key]; !ok {
			ruleMap[key] = make(map[string]interface{})
		}
		for tmpKey = range keyMap {
			if _, ok = boolMap[tmpKey]; ok {
				ruleMap[key][tmpKey] = conf.GetBoolDefault(fmt.Sprintf("%s.%s", key, tmpKey), false)
			} else {
				ruleMap[key][tmpKey] = conf.GetDefault(fmt.Sprintf("%s.%s", key, tmpKey), "")
			}
		}
	}

	ruleList = make([]map[string]interface{}, 0)
	for _, tmpMap = range ruleMap {
		ruleList = append(ruleList, tmpMap)
	}

	return ruleList
}

// 控制并发连接数
func genNumberChan(number int32) chan struct{} {
	return make(chan struct{}, number)
}

func InitConfig(configFile string) (cfg *AppConfig) {
	var (
		currentPath, ePath, file, rootPath, logPath string
		save, esMaxPage                             int
		ruleList                                    []map[string]interface{}
		indexMap                                    map[string]map[string]string
		dsnMap                                      = make(map[string]string)
		conf                                        *ini.IniFileConfigSource
		err                                         error
	)
	currentPath, _ = os.Getwd()
	//file   = kvs.GetCurrentFilePath(fmt.Sprintf("/app.ini"),2)
	if ePath, err = os.Executable(); err != nil {
		panic(err)
	}
	if strings.HasPrefix(configFile, "/") {
		file = configFile
	} else if strings.HasPrefix(configFile, "./") {
		file = fmt.Sprintf("%s/%s", path.Dir(ePath), configFile[2:])
	} else {
		file = fmt.Sprintf("%s/%s", path.Dir(ePath), configFile)
	}
	// 检查配置文件是否存在
	if _, err = os.Stat(file); err != nil && !os.IsExist(err) {
		panic(fmt.Sprintf("config file %s not exists!", file))
	}
	conf = ini.NewIniFileConfigSource(file)
	rootPath = strings.Replace(currentPath, "\\", "/", -1)
	logPath = conf.GetDefault("app.logPath", "")
	if logPath == "" {
		logPath = strings.Replace(currentPath, "\\", "/", -1) + "/logs"
	} else if strings.HasPrefix(logPath, "/") {
		if runtime.GOOS == "windows" {
			logPath = currentPath + logPath
		}
	} else {
		logPath = currentPath + "/" + logPath
	}

	save = conf.GetIntDefault("app.save", 7)

	// 取配置项page_limit,用来限制取的分页数量，0表示不限制
	esMaxPage = conf.GetIntDefault("app.page_limit", 0)
	dsnMap = loadDsn(conf)
	indexMap = loadIndices(conf)
	ruleList = loadRules(conf)
	cfg = &AppConfig{
		ENV:                conf.GetDefault("app.env", "dev"),
		IP:                 conf.GetDefault("app.server.ip", "127.0.0.1"),
		Port:               conf.GetDefault("app.server.port", "8080"),
		Name:               conf.GetDefault("app.server.name", "go-mysql-elasticsearch"),
		Level:              conf.GetDefault("app.level", "info"),
		LogPath:            logPath,
		RootPath:           rootPath,
		ExecPath:           path.Dir(ePath),
		Save:               save,
		Refresh:            int64(conf.GetIntDefault("app.refresh", 3600)),
		TransferInterval:   conf.GetIntDefault("app.transfer_interval", 1),
		SnifferInterval:    conf.GetIntDefault("app.sniffer_interval", 30),
		HandlerQueueLength: conf.GetIntDefault("app.handler_queue_length", 4096),
		FlushInterval:      conf.GetIntDefault("app.flush_interval", 1000),
		BulkSize:           conf.GetIntDefault("app.bulk_size", 1000),
		Period:             int64(conf.GetIntDefault("app.period", 3600)),
		DictPeriod:         int64(conf.GetIntDefault("app.dict_period", 86400)),
		IsFull:             conf.GetBoolDefault("app.is_full", true),
		JobRetry:           conf.GetIntDefault("app.job_retries", 3),
		JobChan:            genNumberChan(int32(conf.GetIntDefault("app.numbers", 16))),
		CtlProps:           make(map[string]map[string]map[int64]interface{}),
		MysqlConfig: MysqlConfig{
			Host:           conf.GetDefault("mysql.host", "172.17.12.100"),
			Port:           conf.GetDefault("mysql.port", "3306"),
			User:           conf.GetDefault("mysql.user", "root"),
			Password:       conf.GetDefault("mysql.password", "root"),
			Charset:        conf.GetDefault("mysql.charset", "utf8"),
			SlaveID:        conf.GetIntDefault("mysql.slave_id", 1001),
			Flavor:         conf.GetDefault("mysql.flavor", "mysql"),
			SkipNoPkTable:  conf.GetBoolDefault("mysql.skip_no_pk_table", false),
			SkipMasterData: conf.GetBoolDefault("mysql.skip_master_data", false),
			DumpPath:       conf.GetDefault("mysql.mysqldump", ""),
			Dsn:            dsnMap,
			Perpage:        int32(conf.GetIntDefault("mysql.perpage", 1000)),
			ESMAXPAGE:      int32(esMaxPage),
			SqlNumbers:     int32(conf.GetIntDefault("mysql.sql_numbers", 1000)),
			ChunkNumbers:   genNumberChan(int32(conf.GetIntDefault("mysql.numbers", 10))),
			UpdateNumbers:  genNumberChan(int32(conf.GetIntDefault("mysql.update_numbers", 5))),
			MaxLifeTime:    int64(conf.GetIntDefault("mysql.max_life_time", 150)),
			DbMap:          make(map[string]*sql.DB, 0),
		},
		ESConfig: ESConfig{
			Host:            conf.GetDefault("elastic.host", "127.0.0.1"),
			Port:            conf.GetDefault("elastic.port", "9200"),
			ReplicaHost:     conf.GetDefault("elastic.replication.host", ""),
			ReplicaPort:     conf.GetDefault("elastic.replication.port", ""),
			Username:        conf.GetDefault("elastic.username", "elastic"),
			Password:        conf.GetDefault("elastic.password", "zMuurwDPhAPuKwHXByRv"),
			ESNumbers:       genNumberChan(int32(conf.GetIntDefault("elastic.numbers", 4))),
			BulkNumber:      int32(conf.GetIntDefault("elastic.bulk_number", 1000)),
			Shards:          int8(conf.GetIntDefault("elastic.shards", 3)),
			Replicas:        int8(conf.GetIntDefault("elastic.replicas", 0)),
			FollowReplicas:  int8(conf.GetIntDefault("elastic.replication.replicas", 0)),
			RefreshInterval: conf.GetDefault("elastic.refresh_interval", "1s"),
			CpuChan:         make(chan map[int64]float64, 1),
			CpuPercent:      conf.GetFloat64Default("elastic.cpu_percent", 70),
			CpuInterval:     int8(conf.GetIntDefault("elastic.cpu_interval", 5)),
			Indices:         indexMap,
			// elasticsearch索引mysql查询信息配置 硬编码的sql语句中如有小括号,须在小括号内左右侧均留一个空格
			// select_sql 主键id 和 order by 之间的内容会被正则替换成update_sql 如 " and disable=0 " => " and update_time>%d "
		},
		RedisConfig: RedisConfig{
			Addr:       conf.GetDefault("redis.addr", "172.12.12.100:6379"),
			Password:   conf.GetDefault("redis.password", ""),
			Db:         conf.GetIntDefault("redis.db", 1000),
			JobKey:     conf.GetDefault("redis.job_key", "cms_item_update"),
			VersionKey: conf.GetDefault("redis.version_key", "version_id"),
		},
		LogConfig: LogConfig{
			Path:         conf.GetDefault("log.path", "access.log"),
			ResponseTime: conf.GetFloat64Default("log.response_time", 200),
		},
		StorageConfig: StorageConfig{
			FilePath: conf.GetDefault("storage.path", "storage/position.db"),
		},
		RuleConfig: RuleConfig{
			RuleList: ruleList,
		},
		SftpConfig: SftpConfig{
			User:         conf.GetDefault("sftp.user", "es7"),
			Password:     conf.GetDefault("sftp.password", "nihao123!"),
			Host:         conf.GetDefault("sftp.host", "172.17.12.120"),
			Port:         conf.GetIntDefault("sftp.port", 22),
			FullDictFile: conf.GetDefault("sftp.full_dict_file", "full_custom.dic"),
			AddDictFile:  conf.GetDefault("sftp.add_dict_file", "add_custom.dic"),
			RemoteDir:    conf.GetDefault("sftp.remote_dir", "/home/es7/app/elasticsearch-7.8.0/config/analysis-ik"),
		},
	}
	Config = cfg
	return Config
}
