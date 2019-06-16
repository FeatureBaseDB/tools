package main

import (
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"io/ioutil"

	"math/bits"
	"os"
	"unsafe"
)

const headerBaseSize = 8
const MagicNumber = 12348

type interval16 struct{ start, last uint16 }

// MeowBits represents a roaring container in a possibly-novel format.
//
// The 2^16 container space is divided into 256 buckets. Each bucket has
// an index, which is the high bits of its values, and contains 0 or more
// 8-bit values, which are the corresponding low bits.
//
// For each bucket:
// * If presence bit is 0, bucket is empty and has size zero.
// * If presence bit is 1, bucket has at least some contents.
//   - if size is 0, bucket has all 256 bits, but takes no storage
//   - if size is 1..31, bucket has that many bits, and they are stored
//     as that many bytes.
//   - if size is 225..255, bucket has that many bits, and the bits it
//     doesn't have are stored as 256-size bytes.
//   - if size is 32..224, bucket has that many bits, and stores them as
//     32 bytes of bitmasks.
//
type MeowBits struct {
	presence [4]uint64
	buckets  [256]uint8
	data     [8192]uint8
}

func (m *MeowBits) SetPresence(bucket uint8) {
	m.presence[bucket>>6] |= 1 << uint64(bucket&63)
}

func (m *MeowBits) ClearPresence(bucket uint8) {
	m.presence[bucket>>6] &^= 1 << uint64(bucket&63)
}

func (m *MeowBits) Presence(bucket uint8) uint64 {
	return (m.presence[bucket>>6] >> uint64(bucket&63)) & 1
}

func (m *MeowBits) BucketSize(bucket uint8) uint16 {
	count := int(m.buckets[bucket])
	if count == 0 {
		return 0
	}
	if count < 32 {
		return uint16(count)
	}
	if 256-count < 32 {
		return uint16(256 - count)
	}
	return 32
}

func (m *MeowBits) Size() int {
	offset := m.BucketOffset(255)
	if m.Presence(255) != 0 {
		offset += m.BucketSize(255)
	}
	return int(offset) + 288
}

func (m *MeowBits) BucketOffset(bucket uint8) uint16 {
	var idx uint8
	var offset uint16
	for idx = 0; idx < bucket; idx++ {
		if m.Presence(idx) != 0 {
			offset += m.BucketSize(idx)
		}
	}
	return offset
}

func (m *MeowBits) RemoveBucket(bucket uint8) {
	if m.Presence(bucket) == 0 {
		return
	}
	offset := m.BucketOffset(bucket)
	size := m.BucketSize(bucket)
	copy(m.data[offset:], m.data[offset+size:])
	m.buckets[bucket] = 0
	m.ClearPresence(bucket)
}

// unpacks the bits set in an array into a slice of uint8s
func unpackBits(into []uint8, words [4]uint64) []uint8 {
	offset := 0
	n := 0
	for _, word := range words {
		bit := 0
		for word != 0 {
			nextBit := bits.TrailingZeros64(word)
			word >>= (uint64(nextBit) + 1)
			bit += nextBit + 1
			into[n] = uint8(bit + offset - 1)
			n++
		}
		offset += 64
	}
	return into
}

func (m *MeowBits) SetBucket(bucket uint8, vals []uint8) {
	if len(vals) == 0 {
		m.RemoveBucket(bucket)
		return
	}
	if len(vals) > 256 {
		panic("more than 256 vals for a bucket")
	}
	offset := m.BucketOffset(bucket)
	oldSize := m.BucketSize(bucket)
	m.buckets[bucket] = uint8(len(vals))
	m.SetPresence(bucket)
	newSize := m.BucketSize(bucket)
	if oldSize != newSize {
		copy(m.data[offset+oldSize:], m.data[offset+newSize:])
	}
	// 256 values: represented as nothing, because there's
	// 0 bits of additional data needed to tell us which 256
	// values.
	if len(vals) == 256 {
		return
	}
	// either we want to encode the missing bits, or the bitmap.
	// but the bitmap is the easiest way to get the missing bits...
	if len(vals) < 32 {
		copy(m.data[offset:], vals)
		return
	}
	var words [4]uint64
	for _, v := range vals {
		words[v>>6] |= 1 << uint64(v&63)
	}
	if len(vals) > 224 {
		anti := make([]uint8, 256-len(vals))
		// populate with the zero bits
		for i := 0; i < 4; i++ {
			words[i] = ^words[i]
		}
		unpackBits(anti, words)
		if len(anti) != 256-len(vals) {
			panic(fmt.Sprintf("%d vals, anti representation %d vals", len(vals), len(anti)))
		}
		copy(m.data[offset:], anti)
		return
	}
	for i, v := range words {
		binary.LittleEndian.PutUint64(m.data[offset+(8*uint16(i)):], v)
	}
}

func (m *MeowBits) GetBucket(bucket uint8, into []uint8) (n int) {
	if m.Presence(bucket) == 0 {
		return 0
	}
	n = int(m.buckets[bucket])
	if n == 0 {
		for i := 0; i < 256; i++ {
			into[i] = uint8(i)
		}
		return 256
	}
	offset := m.BucketOffset(bucket)
	if n < 32 {
		copy(into, m.data[offset:offset+uint16(n)])
		return n
	}
	if n < 224 {
		var bits [4]uint64
		for i := 0; i < 4; i++ {
			bits[i] = binary.LittleEndian.Uint64(m.data[offset+uint16(i*8):])
		}
		unpackBits(into, bits)
		return n
	}
	vals := m.data[offset : offset+uint16(256-n)]
	i := 0 // where in 0..256 we are
	j := 0 // location in "into" to write to
	for _, skip := range vals {
		for ; i < int(skip); i++ {
			into[j] = uint8(i)
			j++
		}
		i++
	}
	// copy any remaining values:
	for ; i < 256; i++ {
		into[j] = uint8(i)
		j++
	}
	if j != n {
		panic(fmt.Sprintf("tried to find %d vals in anti container, got %d", n, j))
	}
	return n
}

func ArrayToMeowBits(array []uint16) (m *MeowBits) {
	var bucket [256]uint8
	var bucketN int

	m = &MeowBits{}
	prevBucket := -1
	for _, val := range array {
		newBucket := int(val >> 8)
		if newBucket != prevBucket {
			if prevBucket >= 0 && bucketN != 0 {
				m.SetBucket(uint8(prevBucket), bucket[:bucketN])
			}
			bucketN = 0
			prevBucket = newBucket
		}
		bucket[bucketN] = uint8(val & 255)
		bucketN++
	}
	if prevBucket >= 0 && bucketN > 0 {
		m.SetBucket(uint8(prevBucket), bucket[:bucketN])
	}
	return m
}

func BitmapToMeowBits(bitmap []uint64) (m *MeowBits) {
	var bucket [256]uint8
	var bucketN int
	var words []uint64
	var n int

	m = &MeowBits{}

	for i := 0; i < 256; i++ {
		bucketN = 0
		words = bitmap[i*4 : (i+1)*4]
		offset := uint(0)
		for _, word := range words {
			for j := uint(0); j < 64; j++ {
				if (word>>j)&1 != 0 {
					bucket[bucketN] = uint8(offset + j)
					bucketN++
					n++
				}
			}
			offset += 64
		}
		if bucketN > 0 {
			m.SetBucket(uint8(i), bucket[:bucketN])
		}
	}
	return m
}

func RunToMeowBits(runs []interval16) (m *MeowBits) {
	var bitmap [1024]uint64
	for _, run := range runs {
		for i := uint64(run.start); i <= uint64(run.last); i++ {
			bitmap[i>>6] |= 1 << (i & 63)
		}
	}
	return BitmapToMeowBits(bitmap[:])
}

func (m *MeowBits) GetN() int {
	count := 0
	for i := 0; i < 256; i++ {
		if m.Presence(uint8(i)) != 0 {
			if m.buckets[i] != 0 {
				count += int(m.buckets[i])
			} else {
				count += 256
			}
		}
	}
	return count
}

func (m *MeowBits) Array() []uint16 {
	out := make([]uint16, m.GetN())
	n := 0
	var bucket [256]uint8
	for idx := 0; idx < 256; idx++ {
		bucketN := m.GetBucket(uint8(idx), bucket[:])
		if bucketN > 0 {
			for i := 0; i < bucketN; i++ {
				out[n] = (uint16(idx) << 8) | uint16(bucket[i])
				n++
			}
		}
	}
	if n != len(out) {
		panic(fmt.Sprintf("unexpected length: GetN predicted %d, got %d", len(out), n))
	}
	return out
}

func (m *MeowBits) Bitmap() []uint64 {
	out := make([]uint64, 1024)
	var bucket [256]uint8
	var bucketN int
	var words []uint64

	for i := 0; i < 256; i++ {
		words = out[i*4 : (i+1)*4]
		bucketN = m.GetBucket(uint8(i), bucket[:])
		for j := 0; j < bucketN; j++ {
			v := bucket[j]
			words[v>>6] |= 1 << (uint64(v & 63))
		}
	}
	return out
}

func ExamineRoaring(data []byte) error {
	expectedSize := 0
	if len(data) < headerBaseSize {
		return fmt.Errorf("data too small")
	}

	// Verify the first two bytes are a valid MagicNumber, and second two bytes match current storageVersion.
	fileMagic := uint32(binary.LittleEndian.Uint16(data[0:2]))
	fileVersion := uint32(data[2])
	if fileMagic != MagicNumber {
		return fmt.Errorf("invalid roaring file, magic number %v is incorrect", fileMagic)
	}

	if fileVersion != 0 {
		return fmt.Errorf("wrong roaring version, file is v%d, server requires v%d", fileVersion, 0)
	}

	// Read key count in bytes sizeof(cookie):(sizeof(cookie)+sizeof(uint32)).
	keyN := binary.LittleEndian.Uint32(data[4:8])
	postHeader := data[headerBaseSize:]
	headerDataSize := 12 * keyN
	headers := postHeader[:headerDataSize]
	offsets := postHeader[headerDataSize : headerDataSize+(keyN*4)]
	var opLog []byte = postHeader
	var nTotal int
	arrays := 0
	bitmaps := 0
	runs := 0
	var roaringArraySize, meowArraySize, roaringBitmapSize, meowBitmapSize, roaringRunSize, meowRunSize int64

	// Descriptive header section: Read container keys and cardinalities.
	fmt.Printf("container keys:\n")
done:
	for i, header, offset := 0, headers, offsets; i < int(keyN); i, header, offset = i+1, header[12:], offset[4:] {
		key := binary.LittleEndian.Uint64(header[0:8])
		// fmt.Printf("  %d\n", key)
		typ := binary.LittleEndian.Uint16(header[8:10])
		n := int(binary.LittleEndian.Uint16(header[10:12])) + 1
		nTotal += n
		offset := binary.LittleEndian.Uint32(offset[:4])
		body := data[offset:]
		var dataSize int
		switch typ {
		case 1: // array
			array := (*[1 << 16]uint16)(unsafe.Pointer(&body[0]))[:n:n]
			m := ArrayToMeowBits(array)
			a2 := m.Array()
			arrays++
			if len(array) != len(a2) {
				fmt.Printf("array mismatch, expected %d entries, got %d\n", len(array), len(a2))
				if len(array) > 5 {
					fmt.Printf("  expected: %d[...]\n", array[:5])
				} else {
					fmt.Printf("  expected: %d\n", array)
				}
				if len(a2) > 5 {
					fmt.Printf("  got: %d[...]\n", a2[:5])
				} else {
					fmt.Printf("  got: %d\n", a2)
				}
				break done
			} else {
				for bit := 0; bit < len(array); bit++ {
					if array[bit] != a2[bit] {
						fmt.Printf("array mismatch: [%d] expected %d, got %d\n", bit, array[bit], a2[bit])
						break done
					}
				}
			}
			meowArraySize += int64(m.Size())
			if n <= 5 {
				expectedSize += 32
			} else {
				expectedSize += 32 + (2 * n)
			}
			dataSize = int(n) * 2
			roaringArraySize += int64(dataSize)
		case 2: // bitmap
			bitmap := (*[1024]uint64)(unsafe.Pointer(&body[0]))[:1024:1024]
			m := BitmapToMeowBits(bitmap)
			b2 := m.Bitmap()
			meowBitmapSize += int64(m.Size())
			bitmaps++
			for word := 0; word < len(bitmap); word++ {
				if bitmap[word] != b2[word] {
					fmt.Printf("bitmap [%d] mismatch: word %d, expected %x, got %x\n",
						key, word, bitmap[word], b2[word])
					break done
				}
			}
			dataSize = 8192
			roaringBitmapSize += int64(dataSize)
			expectedSize += 8192 + 32
		case 3: //run
			count := int(binary.LittleEndian.Uint16(body[:2]))
			dataSize = 2 + (count * 4)
			expectedSize += 32 + (4 * count)
			rle := (*[2048]interval16)(unsafe.Pointer(&data[offset+2]))[:count:count]
			runs++
			b2 := RunToMeowBits(rle)
			if b2.GetN() != n {
				fmt.Printf("run of %d came out as %d\n", n, b2.GetN())
			}
			roaringRunSize += int64(dataSize)
			meowRunSize += int64(b2.Size())
		}
		// fmt.Printf("idx %d: key %d, type %d, n %d, data size %d\n", i, key, typ, n, dataSize)
		opLog = data[int(offset)+dataSize:]
	}
	fmt.Printf("%d arrays [%d vs %d], %d bitmaps [%d vs %d], %d runs [%d vs %d], total %d vs %d\n",
		arrays, roaringArraySize, meowArraySize,
		bitmaps, roaringBitmapSize, meowBitmapSize,
		runs, roaringRunSize, meowRunSize,
		roaringArraySize+roaringBitmapSize+roaringRunSize, meowArraySize+meowBitmapSize+meowRunSize)
	fmt.Printf("op log %d bytes\n", len(opLog))

	ops := opLog
	opCount := 0
	opN := 0
	keySet := make(map[uint64]struct{}, 32)

	for len(ops) > 0 {
		// Unmarshal the op and apply it.
		var opr op
		if err := opr.UnmarshalBinary(ops); err != nil {
			fmt.Printf("error unmarshalling op from %x: %v\n", ops[:1], err)
			break
		}
		opCount++
		opN += opr.count()
		opr.apply(keySet)

		// Move the buffer forward.
		ops = ops[opr.size():]
	}
	if len(keySet) > 0 {
		fmt.Printf("keys from op log:\n")
		for k := range keySet {
			fmt.Printf("  %d\n", k)
		}
	}
	fmt.Printf("%d indexes, total n %d, expected size %d [file size %d including ops], ", keyN, nTotal, expectedSize, len((data)))
	fmt.Printf("plus %d ops [%d bytes, %d values]\n", opCount, len(opLog), opN)

	return nil
}

// opType represents a type of operation.
type opType uint8

const (
	opTypeAdd         = opType(0)
	opTypeRemove      = opType(1)
	opTypeAddBatch    = opType(2)
	opTypeRemoveBatch = opType(3)
)

// op represents an operation on the bitmap.
type op struct {
	typ    opType
	value  uint64
	values []uint64
}

func (op *op) UnmarshalBinary(data []byte) error {
	if len(data) < 13 {
		return fmt.Errorf("op data out of bounds: len=%d", len(data))
	}

	op.typ = opType(data[0])
	// op.value will actually contain the length of values for batch ops
	op.value = binary.LittleEndian.Uint64(data[1:9])

	// Verify checksum.
	h := fnv.New32a()
	_, _ = h.Write(data[0:9])

	if op.typ > 1 {
		if len(data) < int(13+op.value*8) {
			return fmt.Errorf("op data truncated - expected %d, got %d", 13+op.value*8, len(data))
		}
		_, _ = h.Write(data[13 : 13+op.value*8])
		op.values = make([]uint64, op.value)
		for i := uint64(0); i < op.value; i++ {
			start := 13 + i*8
			op.values[i] = binary.LittleEndian.Uint64(data[start : start+8])
		}
		op.value = 0
	}
	if chk := binary.LittleEndian.Uint32(data[9:13]); chk != h.Sum32() {
		return fmt.Errorf("checksum mismatch: exp=%08x, got=%08x", h.Sum32(), chk)
	}

	return nil
}

func (op *op) count() int {
	switch op.typ {
	case opTypeAdd, opTypeRemove:
		return 1
	case opTypeAddBatch, opTypeRemoveBatch:
		return len(op.values)
	default:
		panic(fmt.Sprintf("unknown operation type: %d", op.typ))
	}
}

// which keys might be created by the op log?
func (op *op) apply(keySet map[uint64]struct{}) {
	switch op.typ {
	case opTypeAdd:
		keySet[op.value>>16] = struct{}{}
	case opTypeAddBatch:
		for _, k := range op.values {
			keySet[k>>16] = struct{}{}
		}
	}
}

// size returns the encoded size of the op, in bytes.
func (op *op) size() int {
	if op.typ == opTypeAdd || op.typ == opTypeRemove {
		return 1 + 8 + 4
	}
	return 1 + 8 + 4 + len(op.values)*8
}

func main() {
	for _, path := range os.Args[1:] {
		data, err := ioutil.ReadFile(path)
		if err != nil {
			fmt.Printf("error reading '%s': %v\n", path, err)
			continue
		}
		err = ExamineRoaring(data)
		if err != nil {
			fmt.Printf("error examining '%s': %v\n", path, err)
		}
	}
}
