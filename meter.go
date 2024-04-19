package wkafka

import "time"

type Meter interface {
	Meter(start time.Time, batchSize int64, topic string, err error, isDLQ bool)
}

type meterFuncImpl struct {
	meterFunc func(time.Time, int64, string, error, bool)
}

func (m *meterFuncImpl) Meter(start time.Time, batchSize int64, topic string, err error, isDLQ bool) {
	m.meterFunc(start, batchSize, topic, err, isDLQ)
}

func NewMeter(meterFunc func(time.Time, int64, string, error, bool)) Meter {
	return &meterFuncImpl{meterFunc: meterFunc}
}

func EmptyMeter() Meter {
	return &meterFuncImpl{meterFunc: func(time.Time, int64, string, error, bool) {}}
}
