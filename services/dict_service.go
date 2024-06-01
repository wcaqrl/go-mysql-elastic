package services

import (
	"go-mysql-elasticsearch/repositories"
)

type DictService struct {
	dictRepository repositories.IRepository
}

func NewDictService(repository repositories.IRepository) IService {
	return &DictService{repository}
}

func (d *DictService) SelectFromMysql(sql string) ([]interface{}, error) {
	return d.dictRepository.SelectFromMysql(sql)
}
