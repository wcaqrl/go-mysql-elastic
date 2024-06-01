package datamodels

type Dict struct {
	ID         int64   `json:"id" sql:"id"`
	Word       string  `json:"word" sql:"word"`
	UpdateTime int64   `json:"update_time" sql:"update_time"`
}
