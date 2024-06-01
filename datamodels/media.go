package datamodels

type Media struct {
	ID             int64    `json:"id" sql:"id"`
	MediaID        int64    `json:"media_id" sql:"media_id"`
	Heat           int64    `json:"heat" sql:"heat"`
	Language  	   string   `json:"language" sql:"language"`
	BestvID        string   `json:"bestv_id" sql:"bestv_id"`
	Source  	   string   `json:"source" sql:"source"`
	TotalVv        int64    `json:"total_vv" sql:"total_vv"`
	Score          float64  `json:"score" sql:"score"`
	VipType        string   `json:"vip_type" sql:"vip_type"`
	Alias  	       string   `json:"alias" sql:"alias"`
	Director  	   string   `json:"director" sql:"director"`
	ReleaseYear    int64    `json:"release_year" sql:"release_year"`
	Actor  	       string   `json:"actor" sql:"actor"`
	ReleaseDate    string   `json:"release_date" sql:"release_date"`
	Disable        int8     `json:"disable" sql:"disable"`
	CornerType     string   `json:"corner_type" sql:"corner_type"`
	Name  	       string   `json:"name" sql:"name"`
	Aword  	       string   `json:"aword" sql:"aword"`
	Poster  	   string   `json:"poster" sql:"poster"`
	Still  	       string   `json:"still" sql:"still"`
	Description    string   `json:"description" sql:"description"`
	DefinitionId   int8     `json:"definition_id" sql:"definition_id"`
	IsEnd          int8     `json:"is_end" sql:"isend"`
	TotalNum       int64    `json:"total_num" sql:"total_num"`
	UpInfo         int64    `json:"upinfo" sql:"upinfo"`
	OriginalID     interface{}  `json:"original_id" sql:"original_id"`
	ChannelID      int64    `json:"channel_id" sql:"channel_id"`
	PaymentInfo    string   `json:"payment_info" sql:"payment_info"`
	Extend         string   `json:"extend" sql:"extend"`
	LanguageMediaID int64   `json:"language_media_id" sql:"language_media_id"`
	RecordNum      string   `json:"record_num" sql:"record_num"`
	BillboardId    int64    `json:"billboard_id" sql:"billboard_id"`
	BillboardRank  int64    `json:"billboard_rank" sql:"billboard_rank"`
	EduPriority    int      `json:"edu_priority" sql:"edu_priority"`
	CopyrightExpire int8    `json:"copyright_expire" sql:"copyright_expire"`
	CreateTime     int64    `json:"create_time" sql:"create_time"`
	UpdateTime     int64    `json:"update_time" sql:"update_time"`
	VersionID      int64    `json:"version_id"`
}
