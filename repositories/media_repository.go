package repositories

import (
	"database/sql"
	"encoding/json"
	"fmt"
	log "github.com/sirupsen/logrus"
	"go-mysql-elasticsearch/common"
	"go-mysql-elasticsearch/config"
	"go-mysql-elasticsearch/datamodels"
	"sort"
	"strconv"
	"strings"
)

type MediaManager struct {
	actionArr [3]string
	mysqlConn *sql.DB
}

func NewMediaManager(actionArr [3]string, db *sql.DB) IRepository {
	return &MediaManager{actionArr: actionArr, mysqlConn: db}
}

func (mr *MediaManager) Conn() (err error) {
	if mr.mysqlConn == nil {
		if mr.mysqlConn, err = common.NewMysqlConn(config.Config.Dsn[mr.actionArr[0]+"_"+mr.actionArr[1]+"_dsn"]); err != nil {
			return err
		}
	}
	return
}

func (mr *MediaManager) SelectFromMysql(sql string) (mediaArray []interface{}, err error) {
	var (
		ok      bool
		actName = mr.actionArr[0] + "_media"

		tags, cats, areas, directors, actors, episodes                                                      map[int]map[string]string
		tag                                                                                                 *datamodels.Tag
		cat                                                                                                 *datamodels.Category
		area                                                                                                *datamodels.Area
		director                                                                                            *datamodels.Director
		tmpSortDirectorArr                                                                                  []*datamodels.Director
		tmpSortActorArr                                                                                     []*datamodels.Actor
		length                                                                                              int
		tmpEduPriority                                                                                      = 0 // 默认教育媒体优先级 0
		directorIDSlice, actorIDSlice                                                                       []int64
		directorSlice, actorSlice                                                                           []string
		actor                                                                                               *datamodels.Actor
		episode                                                                                             *datamodels.Episode
		mediaID, tagID, catID, stdCatID, stdAreaID, uID, areaID, createTime, updateTime, oriID, tmpDuration int64

		tmpName, tmpCode, tmpVipType         string
		tNames, cNames, aNames               map[int64]string
		tmpCornerType                        []string
		tmpLanguageDisable, tmpGxAuditStatus int8
		tmpOriginalID                        interface{}

		tmpExcludeSource                                                    []string
		tmpTagMap                                                           []string
		tmpTagIDs                                                           []int64
		tmpCatMap                                                           []string
		tmpCatIDs, tmpStdCatIDs, tmpCopyrightIDs, tmpAreaIDs, tmpStdAreaIDs []int64
		tmpAreaMap                                                          []string
		tmpShowBrands, tmpHideBrands                                        []string
		tmpShowStrategies, tmpHideStrategies                                []int

		tmpDurationMap map[string]interface{}

		tmpPaymentBrands []int64
		pInfo            []map[string]interface{}

		mediaIds []string

		mediaArr []*datamodels.Media
		m        *datamodels.Media

		exSourceMap = make(map[int64][]string)

		tagIDMap = make(map[int64][]int64)
		tagMap   = make(map[int64]map[int64]string)

		catIDMap    = make(map[int64][]int64)
		catMap      = make(map[int64]map[int64]string)
		stdCatIDMap = make(map[int64][]int64)

		areaArr      []*datamodels.Area
		areaIDMap    = make(map[int64][]int64)
		areaMap      = make(map[int64]map[int64]string)
		stdAreaIDMap = make(map[int64][]int64)

		actorMap = make(map[int64][]*datamodels.Actor)

		// episodeMap 用来存放 update_time
		episodeMap = make(map[int64]int64)
		props      = make(map[string]map[string]map[int64]interface{})
	)
	mediaArray = make([]interface{}, 0)

	if err = mr.Conn(); err != nil {
		return
	}
	props = config.Config.CtlProps
	if _, ok = props[actName]; !ok {
		mr.genProps()
	}

	rows, err := mr.mysqlConn.Query(sql)
	if err != nil {
		log.Error(fmt.Sprintf("sqlselect failed--fm_media: %s", sql))
		return
	}
	defer rows.Close()
	result := common.GetResultRows(rows)
	if len(result) == 0 {
		//log.Info(fmt.Sprintf("No Data:    %s",sql))
		return
	}
	for _, v := range result {
		media := &datamodels.Media{}
		common.DataToStructByTagSql(v, media)
		media.VersionID = common.Incr(config.Config.RedisConfig.VersionKey)
		mediaArr = append(mediaArr, media)
	}

	// 取出media_ids、alternative_id
	for _, m := range mediaArr {
		mediaIds = append(mediaIds, strconv.FormatInt(m.MediaID, 10))
	}

	if len(mediaIds) > 0 {
		// 获取同名信息
		exSourceMap = mr.processAlternatives(mediaIds)
		// 查出tag表
		sql = fmt.Sprintf("select t.tag_id as id,t.tag_id,t.name,c.media_id from fm_tag t inner join fm_media_tag c on t.tag_id=c.tag_id where c.media_id in (%s)", strings.Join(mediaIds, ","))
		if rows, err = mr.mysqlConn.Query(sql); err != nil {
			log.Error(fmt.Sprintf("sqlselect failed--fm_tag: %s", sql))
			return
		}
		defer rows.Close()
		tags = common.GetResultRows(rows)
		if len(tags) > 0 {
			for _, v := range tags {
				mediaID, _ = strconv.ParseInt(v["media_id"], 10, 64)
				tagID, _ = strconv.ParseInt(v["tag_id"], 10, 64)
				tagIDMap[mediaID] = append(tagIDMap[mediaID], tagID)
				if tagMap[mediaID] == nil {
					tagMap[mediaID] = make(map[int64]string)
				}
				tagMap[mediaID][tagID] = v["name"]
				common.DataToStructByTagSql(v, tag)
			}
		}

		// 查出category表
		sql = fmt.Sprintf("select c.category_id as id,c.category_id,c.name,c.standard_cate_id,m.media_id from fm_category c inner join fm_media_category m on c.category_id=m.category_id where m.media_id in (%s)", strings.Join(mediaIds, ","))
		if rows, err = mr.mysqlConn.Query(sql); err != nil {
			log.Error(fmt.Sprintf("sqlselect failed--fm_category: %s", sql))
			return
		}
		defer rows.Close()
		cats = common.GetResultRows(rows)
		if len(cats) > 0 {
			for _, v := range cats {
				mediaID, _ = strconv.ParseInt(v["media_id"], 10, 64)
				catID, _ = strconv.ParseInt(v["category_id"], 10, 64)
				stdCatID, _ = strconv.ParseInt(v["standard_cate_id"], 10, 64)
				catIDMap[mediaID] = append(catIDMap[mediaID], catID)
				stdCatIDMap[mediaID] = append(stdCatIDMap[mediaID], stdCatID)
				if catMap[mediaID] == nil {
					catMap[mediaID] = make(map[int64]string)
				}
				catMap[mediaID][catID] = v["name"]
				common.DataToStructByTagSql(v, cat)
			}
		}

		// 查出area表
		sql = fmt.Sprintf("select a.area_id as id,a.area_id,a.name,m.media_id,a.standard_area_id from fm_area a inner join fm_media_area m on a.area_id=m.area_id where m.media_id in (%s)", strings.Join(mediaIds, ","))
		if rows, err = mr.mysqlConn.Query(sql); err != nil {
			log.Error(fmt.Sprintf("sqlselect failed--fm_area: %s", sql))
			return
		}
		defer rows.Close()
		areas = common.GetResultRows(rows)
		if len(areas) > 0 {
			for _, v := range areas {
				area = &datamodels.Area{}
				mediaID, _ = strconv.ParseInt(v["media_id"], 10, 64)
				areaID, _ = strconv.ParseInt(v["area_id"], 10, 64)
				stdAreaID, _ = strconv.ParseInt(v["standard_area_id"], 10, 64)
				areaIDMap[mediaID] = append(areaIDMap[mediaID], areaID)
				stdAreaIDMap[mediaID] = append(stdAreaIDMap[mediaID], stdAreaID)
				if areaMap[mediaID] == nil {
					areaMap[mediaID] = make(map[int64]string)
				}
				areaMap[mediaID][areaID] = v["name"]
				common.DataToStructByTagSql(v, area)
				areaArr = append(areaArr, area)
			}
		}

		// 查出director表
		sql = fmt.Sprintf("select a.actor_id as id,a.actor_id,a.name,d.media_id,d.priority,a.priority as apriority from fm_actor a inner join fm_director_media d on a.actor_id=d.actor_id where d.media_id in (%s) order by d.priority desc,a.priority desc ", strings.Join(mediaIds, ","))
		if rows, err = mr.mysqlConn.Query(sql); err != nil {
			log.Error(fmt.Sprintf("sql select failed--fm_director_media: %s", sql))
			return
		}
		defer rows.Close()
		directors = common.GetResultRows(rows)
		if len(directors) > 0 {
			for _, v := range directors {
				mediaID, _ = strconv.ParseInt(v["media_id"], 10, 64)
				common.DataToStructByTagSql(v, director)
			}
		}

		// 查出actor表
		sql = fmt.Sprintf("select a.actor_id as id,a.actor_id,a.name,m.media_id,m.priority,a.priority as apriority from fm_actor a inner join fm_actor_media m on a.actor_id=m.actor_id where m.media_id in (%s) order by m.priority desc,a.priority desc ", strings.Join(mediaIds, ","))
		if rows, err = mr.mysqlConn.Query(sql); err != nil {
			log.Error(fmt.Sprintf("sql select failed--fm_actor_media: %s", sql))
			return
		}
		defer rows.Close()
		actors = common.GetResultRows(rows)
		if len(actors) > 0 {
			for _, v := range actors {
				actor = &datamodels.Actor{}
				mediaID, _ = strconv.ParseInt(v["media_id"], 10, 64)
				common.DataToStructByTagSql(v, actor)
				actorMap[mediaID] = append(actorMap[mediaID], actor)
			}
		}

		// 查出episode表
		sql = fmt.Sprintf("select media_id,max(update_time) as update_time from fm_episode where disable=0 and media_id in (%s) group by media_id", strings.Join(mediaIds, ","))
		if rows, err = mr.mysqlConn.Query(sql); err != nil {
			log.Error(fmt.Sprintf("sql select failed--fm_episode: %s", sql))
			return
		}
		defer rows.Close()
		episodes = common.GetResultRows(rows)
		if len(episodes) > 0 {
			for _, v := range episodes {
				mediaID, _ = strconv.ParseInt(v["media_id"], 10, 64)
				uID, _ = strconv.ParseInt(v["update_time"], 10, 64)
				episodeMap[mediaID] = uID
				common.DataToStructByTagSql(v, episode)
			}
		}
	}

	// 遍历组装mediaArray *datamodels.MediaMix
	for _, m = range mediaArr {
		tmpVipType = m.VipType
		if m.VipType == "" {
			tmpVipType = "free"
		}

		tmpExcludeSource = make([]string, 0)

		if _, ok = exSourceMap[m.MediaID]; ok {
			tmpExcludeSource = mr.processExcludeSource(m.ChannelID, exSourceMap[m.MediaID])
		}

		tmpShowBrands, tmpHideBrands = GetBrandsFromExtend(m.MediaID, m.Extend)
		tmpEduPriority = getEduPriorityFromExtend(m.MediaID, m.Extend)
		tmpShowStrategies, tmpHideStrategies = GetStrategiesFromExtend(m.MediaID, m.Extend)

		pInfo = make([]map[string]interface{}, 0)
		tmpPaymentBrands = make([]int64, 0)
		if m.PaymentInfo != "" {
			if err = json.Unmarshal([]byte(m.PaymentInfo), &pInfo); err != nil {
				log.Error("payment_info transfer failed: ", err.Error())
				err = nil
			} else {
				if len(pInfo) > 0 {
					for _, v := range pInfo {
						if pment, ok := v["payment"]; ok {
							pmentVal := pment.(string)
							if pmentVal != "" && pmentVal != "free" {
								tmpPaymentBrands = append(tmpPaymentBrands, int64(v["brand"].(float64)))
							}
							// 处理 vip_type 当payment_info下存在 brand=0 时将payment值赋给vip_type
							if int64(v["brand"].(float64)) == 0 {
								tmpVipType = pmentVal
							}
						}
					}
				}
			}
		}

		// 媒体创建时间, 兼容优酷出现的创建时间小于0
		if m.CreateTime > 0 {
			createTime = m.CreateTime
		}

		// 媒体最新更新时间
		updateTime = m.UpdateTime
		if _, ok = episodeMap[m.MediaID]; ok {
			if episodeMap[m.MediaID] > updateTime {
				updateTime = episodeMap[m.MediaID]
			}
		}

		// director 和 actor
		// 按priority降序排列再按actor_id降序排列
		directorIDSlice = make([]int64, 0)
		directorSlice = make([]string, 0)

		actorIDSlice = make([]int64, 0)
		actorSlice = make([]string, 0)
		if _, ok = actorMap[m.MediaID]; ok {
			tmpSortActorArr = actorMap[m.MediaID]
			length = len(tmpSortActorArr)
			for i := 0; i < length; i++ {
				for j := 1; j < length-i; j++ {
					if tmpSortActorArr[j].Priority > tmpSortActorArr[j-1].Priority {
						tmpSortActorArr[j], tmpSortActorArr[j-1] = tmpSortActorArr[j-1], tmpSortActorArr[j]
					} else if tmpSortActorArr[j].Priority == tmpSortActorArr[j-1].Priority {
						if tmpSortActorArr[j].APriority > tmpSortActorArr[j-1].APriority {
							tmpSortActorArr[j], tmpSortActorArr[j-1] = tmpSortActorArr[j-1], tmpSortActorArr[j]
						}
					}
				}
			}

			for _, v := range tmpSortActorArr {
				actorIDSlice = append(actorIDSlice, v.ID)
				actorSlice = append(actorSlice, v.Name)
			}
		}

		// 升序排列tag_ids、category_ids、area_ids
		tmpTagIDs = make([]int64, 0)
		if _, ok := tagIDMap[m.MediaID]; ok {
			sort.Slice(tagIDMap[m.MediaID], func(i, j int) bool { return tagIDMap[m.MediaID][i] < tagIDMap[m.MediaID][j] })
			tmpTagIDs = tagIDMap[m.MediaID]
		}
		// 遍历tagMap取出对应顺序的name
		tmpTagMap = make([]string, 0)
		if tNames, ok = tagMap[m.MediaID]; ok {
			for _, v := range tagIDMap[m.MediaID] {
				if _, ok = tNames[v]; ok {
					tmpTagMap = append(tmpTagMap, tNames[v])
				}
			}
		}

		tmpCatIDs = make([]int64, 0)
		if _, ok := catIDMap[m.MediaID]; ok {
			sort.Slice(catIDMap[m.MediaID], func(i, j int) bool { return catIDMap[m.MediaID][i] < catIDMap[m.MediaID][j] })
			tmpCatIDs = catIDMap[m.MediaID]
		}

		tmpStdCatIDs = make([]int64, 0)
		if _, ok := stdCatIDMap[m.MediaID]; ok {
			sort.Slice(stdCatIDMap[m.MediaID], func(i, j int) bool { return stdCatIDMap[m.MediaID][i] < stdCatIDMap[m.MediaID][j] })
			tmpStdCatIDs = stdCatIDMap[m.MediaID]
		}

		// 遍历catMap取出对应顺序的name
		tmpCatMap = make([]string, 0)
		if cNames, ok = catMap[m.MediaID]; ok {
			for _, v := range catIDMap[m.MediaID] {
				if _, ok = cNames[v]; ok {
					tmpCatMap = append(tmpCatMap, cNames[v])
				}
			}
		}

		tmpAreaIDs = make([]int64, 0)
		if _, ok := areaIDMap[m.MediaID]; ok {
			sort.Slice(areaIDMap[m.MediaID], func(i, j int) bool { return areaIDMap[m.MediaID][i] < areaIDMap[m.MediaID][j] })
			tmpAreaIDs = areaIDMap[m.MediaID]
		}

		tmpStdAreaIDs = make([]int64, 0)
		if _, ok := stdAreaIDMap[m.MediaID]; ok {
			sort.Slice(stdAreaIDMap[m.MediaID], func(i, j int) bool { return stdAreaIDMap[m.MediaID][i] < stdAreaIDMap[m.MediaID][j] })
			tmpStdAreaIDs = stdAreaIDMap[m.MediaID]
		}

		tmpAreaMap = make([]string, 0)
		// 遍历areaMap取出对应顺序的name
		if aNames, ok = areaMap[m.MediaID]; ok {
			for _, v := range areaIDMap[m.MediaID] {
				if _, ok = aNames[v]; ok {
					tmpAreaMap = append(tmpAreaMap, aNames[v])
				}
			}
		}

		// corner_type
		tmpCornerType = make([]string, 0)
		if m.CornerType != "" {
			tmpCornerType = strings.Split(m.CornerType, ",")
		}

		// language_disable
		//默认是展示的，若关联id存在，且不与媒体id相等的话就默认是不展现的标志为1
		tmpLanguageDisable = 0
		if (m.LanguageMediaID != 0) && (m.LanguageMediaID != m.MediaID) {
			tmpLanguageDisable = 1
		}

		if oriID, err = strconv.ParseInt(m.OriginalID.(string), 10, 64); err != nil {
			log.Error(fmt.Sprintf("media_id: %d original_id format to int 64 failed", m.MediaID))
			tmpOriginalID = 0
			err = nil
		} else {
			tmpOriginalID = oriID
		}

		tmpM = &datamodels.MediaMix{
			Media: datamodels.Media{
				ID:              m.ID,
				MediaID:         m.MediaID,
				Heat:            m.Heat,
				Language:        m.Language,
				BestvID:         m.BestvID,
				Source:          m.Source,
				TotalVv:         m.TotalVv,
				Score:           m.Score,
				VipType:         tmpVipType,
				Alias:           m.Alias,
				ReleaseYear:     m.ReleaseYear,
				ReleaseDate:     m.ReleaseDate,
				Disable:         m.Disable,
				Name:            m.Name,
				Aword:           m.Aword,
				Poster:          m.Poster,
				Still:           m.Still,
				Description:     m.Description,
				DefinitionId:    m.DefinitionId,
				IsEnd:           m.IsEnd,
				TotalNum:        m.TotalNum,
				UpInfo:          m.UpInfo,
				OriginalID:      tmpOriginalID,
				ChannelID:       m.ChannelID,
				PaymentInfo:     m.PaymentInfo,
				Extend:          m.Extend,
				RecordNum:       m.RecordNum,
				BillboardId:     m.BillboardId,
				BillboardRank:   m.BillboardRank,
				EduPriority:     tmpEduPriority,
				CopyrightExpire: m.CopyrightExpire,
				CreateTime:      createTime,
				UpdateTime:      updateTime,
				VersionID:       m.VersionID,
			},
			Tags:            tmpTagMap,
			TagIDs:          tmpTagIDs,
			Categories:      tmpCatMap,
			CategoryIDs:     tmpCatIDs,
			StdCatIDs:       tmpStdCatIDs,
			Areas:           tmpAreaMap,
			AreaIDs:         tmpAreaIDs,
			StdAreaIDs:      tmpStdAreaIDs,
			DirectorIDs:     directorIDSlice,
			Director:        directorSlice,
			ActorIDs:        actorIDSlice,
			Actor:           actorSlice,
			ChannelName:     tmpName,
			ChannelCode:     tmpCode,
			LanguageDisable: tmpLanguageDisable,
			Priority:        100,
			HideBrands:      tmpHideBrands,
			ShowBrands:      tmpShowBrands,
			HideStrategies:  tmpHideStrategies,
			ShowStrategies:  tmpShowStrategies,
			PaymentBrands:   tmpPaymentBrands,
			CurrentSource:   m.Source,
			CopyrightIDs:    tmpCopyrightIDs,
			CornerType:      tmpCornerType,
			ExcludeSource:   tmpExcludeSource,
			Duration:        tmpDuration,
			GxAuditStatus:   tmpGxAuditStatus,
		}
		mediaArray = append(mediaArray, tmpM)
	}
	if len(mediaArray) == 0 {
		log.Error("mediaArray is empty")
	}
	return
}