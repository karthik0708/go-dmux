package core

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"time"
)

//Duration type embeds time.Duration, this was added to fix JSON parsing of
//time to valid go duration
type Duration struct {
	time.Duration
}

//MarshalJSON implements encoding/json to serialize this type to json string
func (d Duration) MarshalJSON() ([]byte, error) {
	return json.Marshal(d.String())
}

//UnmarshalJSON implements encoding/json to deserialize this to Duration
func (d *Duration) UnmarshalJSON(b []byte) error {
	d.Duration = 10 * time.Nanosecond // default 10ns almost 0
	var v interface{}
	if err := json.Unmarshal(b, &v); err != nil {
		return err
	}
	switch value := v.(type) {
	case float64:
		d.Duration = time.Duration(value)
		return nil
	case string:
		var err error
		d.Duration, err = time.ParseDuration(value)
		if err != nil {
			return err
		}
		return nil
	default:
		return errors.New("invalid duration")
	}
}

//Encode function is used to convert 2d byte[] to 1d byte[]
//This function uses the following encoding scheme
// first 4 bytes = batch Size
// now for every batch
// first 4 bytes = data Size followed by data byte[]
func Encode(data [][]byte) []byte {
	var buffer []byte
	batchSizeBytes := bytesFromInt(len(data))
	buffer = append(buffer, batchSizeBytes...)
	for _, val := range data {
		dataSizeBytes := bytesFromInt(len(val))
		//TODO check is append is performant
		buffer = append(buffer, dataSizeBytes...)
		buffer = append(buffer, val...)
	}
	return buffer
}

//Encode function is used to convert 2d byte[] to 1d byte[]
//This function uses the following encoding scheme
// first 4 bytes = partition number
// next 4 bytes = batch size
func EncodeV2(partition int, data [][]byte) []byte {
	var buffer []byte
	partitionBytes := bytesFromInt(partition)
	buffer = append(buffer, partitionBytes...)
	batchSizeBytes := bytesFromInt(len(data))
	buffer = append(buffer, batchSizeBytes...)
	for _, val := range data {
		buffer = append(buffer, val...)
	}
	return buffer
}

//Encode function is used to convert payload byte[] to 1d byte[] along with the key and offset
//This function uses the following encoding scheme
// first 4 bytes = data Size
// next 8 bytes = offset
// next 4 bytes = key length
// next n byte = key
// followed by data[]
func EncodePayload(key []byte, offset int64, data []byte) []byte {
	var buffer []byte
	dataSizeBytes := bytesFromInt(len(data))
	buffer = append(buffer, dataSizeBytes...)
	offsetBytes := bytesFromInt64(offset)
	buffer = append(buffer, offsetBytes...)
	keySizeBytes := bytesFromInt(len(string(key)))
	buffer = append(buffer, keySizeBytes...)
	buffer = append(buffer, key...)
	buffer = append(buffer, data...)
	return buffer
}

func bytesFromInt(val int) []byte {
	buffer := make([]byte, 4)
	binary.BigEndian.PutUint32(buffer, uint32(val))
	return buffer
}

func bytesFromInt64(val int64) []byte {
	buffer := make([]byte, 8)
	binary.BigEndian.PutUint64(buffer, uint64(val))
	return buffer
}

//Contains method implements existence of an integer in an array
func Contains(a []int, x int) bool {
	for _, n := range a {
		if x == n {
			return true
		}
	}
	return false
}
