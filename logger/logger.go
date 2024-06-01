package logger

import (
	"fmt"
	rotatelogs "github.com/lestrrat/go-file-rotatelogs"
	"github.com/rifflock/lfshook"
	log "github.com/sirupsen/logrus"
	"go-mysql-elasticsearch/config"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"
)

type writterFormatter struct{}

func InitLogger() {
	logPath := config.Config.LogPath
	save := config.Config.Save

	formatter := &log.TextFormatter{}
	formatter.FullTimestamp = true
	formatter.TimestampFormat = "2006-01-02.15:04:05.000000"
	formatter.ForceColors = true
	log.SetFormatter(formatter)

	log.SetOutput(os.Stdout)
	switch config.Config.Level {
	case "debug":
		log.SetLevel(log.DebugLevel)
	case "info":
		log.SetLevel(log.InfoLevel)
	case "warn":
		log.SetLevel(log.WarnLevel)
	case "error":
		log.SetLevel(log.ErrorLevel)
	case "fatal":
		log.SetLevel(log.FatalLevel)
	case "panic":
		log.SetLevel(log.PanicLevel)
	default:
		log.SetLevel(log.InfoLevel)
	}

	lfsHook := lfshook.NewHook(lfshook.WriterMap{
		log.InfoLevel:  writer(logPath, "info", save),
		log.ErrorLevel: writer(logPath, "error", save),
		log.FatalLevel: writer(logPath, "fatal", save),
		log.PanicLevel: writer(logPath, "panic", save),
	}, &writterFormatter{})
	log.AddHook(lfsHook)

}

func (s *writterFormatter) Format(entry *log.Entry) ([]byte, error) {
	msg := fmt.Sprintf("[%s] [%s] %s\n", time.Now().Local().Format("2006-01-02.15:04:05"), strings.ToUpper(entry.Level.String()), entry.Message)
	return []byte(msg), nil
}

func writer(logPath string, level string, save int) *rotatelogs.RotateLogs {
	logFullPath := path.Join(logPath, level)
	var location, _ = time.LoadLocation("Asia/Shanghai")
	fileSuffix := time.Now().In(location).Format("2006/01/0215") + ".logs"

	// 判断目录是否存在,不存在则创建
	absPath := filepath.Dir(logFullPath + "/" + fileSuffix)
	_, err := os.Stat(absPath)
	if err != nil {
		mkErr := os.MkdirAll(absPath, os.ModePerm)
		if mkErr != nil {
			log.Error(fmt.Sprintf("日志文件写入失败: %s\n", err.Error()))
		}
	}

	logier, err := rotatelogs.New(
		logFullPath+"/"+fileSuffix,
		rotatelogs.WithLinkName(logFullPath),
		rotatelogs.WithMaxAge(time.Duration(save*24)*time.Hour), // 文件最大保存时间
		// rotatelogs.WithRotationCount(int(save)),// 文件最大保存份数
		rotatelogs.WithRotationTime(time.Hour*24), // 日志切割时间间隔
	)

	if err != nil {
		log.Panic(fmt.Sprintf("日志文件写入失败: %s\n", err.Error()))
	}
	return logier
}
