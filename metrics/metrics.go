package metrics

//CurrentOffsets defined for the current offset of each partition being processed by DMux
type CurrentOffsets map[int32]int64

//PartitionsOwned defined for the partitions owned by DMux
type PartitionsOwned map[int32]bool

//Metrics defined for representing all metrics related to observability
type Metrics struct {
	offsets CurrentOffsets
	partitions PartitionsOwned
}

type Tracker interface {
	TrackMetrics()
	InitMetrics()
}

func (m *Metrics) TrackMetrics() {

}

func (m *Metrics) InitMetrics() {
	m.partitions = PartitionsOwned{}
	m.offsets = CurrentOffsets{}
}

func (c *CurrentOffsets) updateOffsets() {

}

func (p *PartitionsOwned) updateOwnership() {

}