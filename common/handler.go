package common

import (
	"fmt"
	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	log "github.com/sirupsen/logrus"
	"go-mysql-elasticsearch/config"
	"time"
)

type handler struct {
	queue chan interface{}
	stop  chan struct{}
}

func newHandler() *handler {
	return &handler{
		queue: make(chan interface{}, config.Config.HandlerQueueLength),
		stop:  make(chan struct{}, 1),
	}
}

func (hdr *handler) OnRotate(e *replication.RotateEvent) error {
	hdr.queue <- PosRequest{
		Name:  string(e.NextLogName),
		Pos:   uint32(e.Position),
		Force: true,
	}
	return nil
}

func (hdr *handler) OnTableChanged(schema, table string) error {
	return transferService.updateRule(schema, table)
}

func (hdr *handler) OnDDL(nextPos mysql.Position, _ *replication.QueryEvent) (err error) {
	hdr.queue <- PosRequest{
		Name:  nextPos.Name,
		Pos:   nextPos.Pos,
		Force: true,
	}
	return
}

func (hdr *handler) OnXID(nextPos mysql.Position) (err error) {
	hdr.queue <- PosRequest{
		Name:  nextPos.Name,
		Pos:   nextPos.Pos,
		Force: false,
	}
	return
}

func (hdr *handler) OnRow(e *canal.RowsEvent) (err error) {
	var (
		i         int
		ruleKey   = RuleKey(e.Table.Schema, e.Table.Name)
		requests  []*RowRequest
		rowReq    *RowRequest
		rowsEvent []interface{}
	)
	if !RuleInsExist(ruleKey) {
		return nil
	}
	requests = make([]*RowRequest, 0, len(e.Rows))
	if e.Action == canal.UpdateAction {
		for i = 0; i < len(e.Rows); i++ {
			rowReq = new(RowRequest)
			if (i+1)%2 == 0 {
				rowReq.RuleKey = ruleKey
				rowReq.Action = e.Action
				rowReq.Timestamp = e.Header.Timestamp
				rowReq.Row = e.Rows[i]
				rowReq.Old = e.Rows[i-1]
				requests = append(requests, rowReq)
			}
		}
	} else {
		for _, rowsEvent = range e.Rows {
			rowReq = new(RowRequest)
			rowReq.RuleKey = ruleKey
			rowReq.Action = e.Action
			rowReq.Timestamp = e.Header.Timestamp
			rowReq.Row = rowsEvent
			requests = append(requests, rowReq)
		}
	}
	hdr.queue <- requests
	return
}

func (hdr *handler) OnGTID(gtid mysql.GTIDSet) (err error) {
	return
}

func (hdr *handler) OnPosSynced(pos mysql.Position, set mysql.GTIDSet, force bool) (err error) {
	return
}

func (hdr *handler) String() string {
	return "TransferHandler"
}

func (hdr *handler) startListener() {
	go func() {
		var (
			err                    error
			needFlush, needSavePos bool
			requests               []*RowRequest
			lastSavedTime, nowTime time.Time
			ticker                 = time.NewTicker(time.Duration(config.Config.FlushInterval) * time.Millisecond)
			currentPos             mysql.Position
			in                     interface{}
			posReq                 PosRequest
			rowReqs                []*RowRequest
		)
		defer ticker.Stop()

		lastSavedTime = time.Now()
		requests = make([]*RowRequest, 0, config.Config.BulkSize)
		for {
			needFlush = false
			needSavePos = false
			select {
			case in = <-hdr.queue:
				switch in.(type) {
				case PosRequest:
					posReq, _ = in.(PosRequest)
					nowTime = time.Now()
					if posReq.Force || nowTime.Sub(lastSavedTime) > 3*time.Second {
						lastSavedTime = nowTime
						needFlush = true
						needSavePos = true
						currentPos = mysql.Position{
							Name: posReq.Name,
							Pos:  posReq.Pos,
						}
					}
				case []*RowRequest:
					rowReqs, _ = in.([]*RowRequest)
					requests = append(requests, rowReqs...)
					needFlush = len(requests) >= config.Config.BulkSize
				}
			case <-ticker.C:
				needFlush = true
			case <-hdr.stop:
				return
			}

			if needFlush && len(requests) > 0 && transferService.endpointEnable.Load() {
				if err = transferService.endpoint.Consume(requests); err != nil {
					transferService.endpointEnable.Store(false)
					log.Error(fmt.Sprintf("consume error: %s", err.Error()))
					go transferService.stopDump()
				}
				requests = requests[0:0]
			}
			if needSavePos && transferService.endpointEnable.Load() {
				log.Info(fmt.Sprintf("save position %s %d", currentPos.Name, currentPos.Pos))
				if err = transferService.positionDao.Save(currentPos); err != nil {
					log.Error(fmt.Sprintf("save sync position %v error %s, close sync", currentPos, err.Error()))
					transferService.CloseTransfer()
					return
				}
			}
		}
	}()
}

func (hdr *handler) stopListener() {
	log.Println("transfer stop")
	hdr.stop <- struct{}{}
}
