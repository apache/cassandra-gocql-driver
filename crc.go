package gocql

import "hash/crc32"

var (
	// Initial CRC32 bytes: 0xFA, 0x2D, 0x55, 0xCA
	initialCRC32Bytes = []byte{0xfa, 0x2d, 0x55, 0xca}
)

// Checksum calculates the CRC32 checksum of the given byte slice.
func Checksum(b []byte) uint32 {
	crc := crc32.NewIEEE()
	crc.Reset()
	crc.Write(initialCRC32Bytes) // Include initial CRC32 bytes
	crc.Write(b)
	return crc.Sum32()
}

const (
	crc24Init = 0x875060  // Initial value for CRC24 calculation
	crc24Poly = 0x1974F0B // Polynomial for CRC24 calculation
)

// KoopmanChecksum calculates the CRC24 checksum using the Koopman polynomial.
func KoopmanChecksum(buf []byte) uint32 {
	crc := crc24Init // Initialize CRC with crc24Init value
	for _, b := range buf {
		crc ^= int(b) << 16 // XOR the byte shifted left by 16 bits with the current CRC value

		for i := 0; i < 8; i++ { // Process each bit in the byte
			crc <<= 1               // Shift CRC left by 1 bit
			if crc&0x1000000 != 0 { // If the highest bit (24th bit) is set
				crc ^= crc24Poly // XOR the CRC value with the crc24Poly
			}
		}
	}

	return uint32(crc)
}
