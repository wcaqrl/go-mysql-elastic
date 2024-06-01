package services

type IService interface {
	SelectFromMysql(string) ([]interface{}, error)
}