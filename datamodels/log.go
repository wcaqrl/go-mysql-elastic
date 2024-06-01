package datamodels

import "time"

type LogCollect struct {
	ID           string     `json:"id" sql:"id"`
	TheTime      time.Time  `json:"request_time" sql:"request_time"`
	Domain       string     `json:"domain" sql:"domain"`
	HttpMethod   string     `json:"http_method" sql:"http_method"`
	RequestRoute string     `json:"request_route" sql:"request_route"`
	RequestParam string     `json:"request_param" sql:"request_param"`
	Status       int32	    `json:"http_code" sql:"http_code"`
	ResponseTime float64    `json:"response_time" sql:"response_time"`
	UserAgent    string     `json:"user_agent" sql:"user_agent"`
	RealIp       string     `json:"real_ip" sql:"real_ip"`
}
