package core

import (
	"fmt"
	"math/rand"
	"testing"
)

func TestEncoding(t *testing.T) {
	//encode and write to file - run java code on file to decode and validate
	// data := mockDataCreate(10)
	// batchData := Encode(data)
	// fmt.Println(batchData)
	// ioutil.WriteFile("/tmp/dat1", batchData, 0777)

}

func mockDataCreate(sz int) [][]byte {
	var buffer [][]byte
	for i := 0; i < sz; i++ {
		val := mockData()
		fmt.Println(len(val))
		fmt.Println(string(val))
		buffer = append(buffer, val)
	}
	return buffer
}

func mockData() []byte {
	len := rand.Intn(100)

	bytes := make([]byte, len)
	for i := 0; i < len; i++ {
		bytes[i] = byte(65 + rand.Intn(25)) //A=65 and Z = 65+25
	}
	return bytes
}

func TestContains(t *testing.T) {
	fmt.Println("running test TestContains")
	statusCodes := []int{400, 423, 500}
	actual1 := Contains(statusCodes, 401)
	actual2 := Contains(statusCodes, 400)
	var b bool = false
	var c bool = true

	if b != actual1 {
		t.Errorf("expected %v  equal %v", b, actual1)
	}
	if c != actual2 {
		t.Errorf("expected %v  actual %v", c, actual2)
	}

}
