package types

type Mode string

const (
	ModeWO  = Mode("WO")
	ModeRW  = Mode("RW")
	ModeERR = Mode("ERR")
)
