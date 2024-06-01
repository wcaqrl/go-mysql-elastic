package services

import (
	"go-mysql-elasticsearch/repositories"
)

type ActorService struct {
	actorRepository repositories.IRepository
}

func NewActorService(repository repositories.IRepository) IService {
	return &ActorService{repository}
}

func (act *ActorService) SelectFromMysql(sql string) ([]interface{}, error) {
	return act.actorRepository.SelectFromMysql(sql)
}
