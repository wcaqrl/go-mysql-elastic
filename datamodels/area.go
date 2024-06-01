package datamodels

type Area struct {
	ID      int64  `json:"id" sql:"id"`
	AreaID  int64  `json:"area_id" sql:"area_id"`
	Name    string `json:"name" sql:"name"`
	MediaID int64  `json:"media_id" sql:"media_id"`
}
