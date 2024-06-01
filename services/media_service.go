package services

import (
	"go-mysql-elasticsearch/repositories"
)

type MediaService struct {
	mediaRepository repositories.IRepository
}

func NewMediaService(repository repositories.IRepository) IService {
	return &MediaService{repository}
}

func (m *MediaService) SelectFromMysql(sql string) ([]interface{}, error) {
	return m.mediaRepository.SelectFromMysql(sql)
}
