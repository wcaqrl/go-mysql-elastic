package datamodels

type Actor struct {
	ID            int64  `json:"id" sql:"id"`
	ActorID       int64  `json:"actor_id" sql:"actor_id"`
	Name          string `json:"name" sql:"name"`
	MediaID       int64  `json:"-" sql:"media_id"`
	Region        string `json:"region" sql:"region"`
	AvailableCount  int64 `json:"available_count" sql:"available_count"`
	Priority      int64  `json:"priority" sql:"priority"`
	APriority     int64  `json:"-" sql:"apriority"`
	Alias         string `json:"alias" sql:"alias"`
	Disable       int8   `json:"disable" sql:"disable"`
	Avatar        string `json:"avatar" sql:"avatar"`
	Ranking       int64  `json:"ranking" sql:"ranking"`
	CreateTime    int64    `json:"create_time" sql:"create_time"`
	UpdateTime    int64    `json:"update_time" sql:"update_time"`
	VersionID     int64  `json:"version_id"`
}
