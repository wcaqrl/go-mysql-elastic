package repositories

import (
	"encoding/json"
	log "github.com/sirupsen/logrus"
	"strconv"
	"strings"
)

type IRepository interface {
	SelectFromMysql(string) ([]interface{},error)
}

func GetBrandsFromExtend(mediaId int64, ext string) (showBrands, hideBrands []string) {
	var (
		err error
		ok bool
		tmpStr string
		tbrands []string
		tmpIn = make(map[string]interface{})
	)
	showBrands = make([]string, 0)
	hideBrands = make([]string, 0)
	if ext != "" && ext != "[]" && ext != "0"{
		if err = json.Unmarshal([]byte(ext), &tmpIn); err != nil {
			log.Errorf("media_id %d extend transfer failed: %s\n", mediaId, err.Error())
		} else {
			if len(tmpIn) >0 {
				if _, ok = tmpIn["hide_brands"]; ok {
					tbrands = strings.Split(tmpIn["hide_brands"].(string),",")
					for _, tmpStr = range tbrands {
						tstr := strings.TrimSpace(tmpStr)
						if tstr != "" {
							hideBrands = append(hideBrands, tstr)
						}
					}
				}
				if _, ok = tmpIn["show_brands"]; ok {
					tbrands = strings.Split(tmpIn["show_brands"].(string),",")
					for _, tmpStr = range tbrands {
						tstr := strings.TrimSpace(tmpStr)
						if tstr != "" {
							showBrands = append(showBrands, tstr)
						}
					}
				}
			}
		}
	}
	return
}


func GetStrategiesFromExtend(mediaId int64, ext string) (showStrategies, hideStrategies []int) {
	var (
		err error
		ok bool
		tmpStr string
		tmpInt int
		tmpStrategies []string
		tmpIn = make(map[string]interface{})
	)
	showStrategies = make([]int, 0)
	hideStrategies = make([]int, 0)
	if ext != "" && ext != "[]" && ext != "0"{
		if err = json.Unmarshal([]byte(ext), &tmpIn); err != nil {
			log.Errorf("media_id %d extend transfer failed: %s\n", mediaId, err.Error())
		} else {
			if len(tmpIn) > 0 {
				if _, ok = tmpIn["hide_strategy"]; ok {
					tmpStrategies = strings.Split(tmpIn["hide_strategy"].(string),",")
					for _, tmpStr = range tmpStrategies {
						if tstr := strings.TrimSpace(tmpStr); tstr != "" {
							tmpInt, err = strconv.Atoi(tstr)
							if err == nil {
								hideStrategies = append(hideStrategies, tmpInt)
							}
						}
					}
				}
				if _, ok = tmpIn["show_strategy"]; ok {
					tmpStrategies = strings.Split(tmpIn["show_strategy"].(string),",")
					for _, tmpStr = range tmpStrategies {
						if tstr := strings.TrimSpace(tmpStr); tstr != "" {
							tmpInt, err = strconv.Atoi(tstr)
							if err == nil {
								showStrategies = append(showStrategies, tmpInt)
							}
						}
					}
				}
			}
		}
	}
	return
}