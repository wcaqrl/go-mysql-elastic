package common

type WriteType int8
const (
	Insert        WriteType = 1
	Upsert        WriteType = 2
	UpField       WriteType = 3
	UpFieldUpdate WriteType = 4
	Delete        WriteType = 5
)
