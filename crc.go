package gocql

import "hash/crc32"

var (
	// (byte) 0xFA, (byte) 0x2D, (byte) 0x55, (byte) 0xCA
	initialCRC32Bytes = []byte{0xfa, 0x2d, 0x55, 0xca}
)

func Checksum(b []byte) uint32 {
	crc := crc32.NewIEEE()
	crc.Reset()
	crc.Write(initialCRC32Bytes)
	crc.Write(b)
	return crc.Sum32()
}

const (
	crc24Init = 0x875060
	crc24Poly = 0x1974F0B
)

func KoopmanChecksum(buf []byte) uint32 {
	crc := crc24Init
	for _, b := range buf {
		crc ^= int(b) << 16

		for i := 0; i < 8; i++ {
			crc <<= 1
			if crc&0x1000000 != 0 {
				crc ^= crc24Poly
			}
		}
	}

	return uint32(crc)
}
