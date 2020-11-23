package gocql

import (
	"encoding/binary"
	"math"
	"strings"
)

// cdc partitioner

const (
	scyllaCDCPartitionerName     = "CDCPartitioner"
	scyllaCDCPartitionerFullName = "com.scylladb.dht.CDCPartitioner"

	scyllaCDCPartitionKeyLength  = 16
	scyllaCDCVersionMask         = 0x0F
	scyllaCDCMinSupportedVersion = 1
	scyllaCDCMaxSupportedVersion = 1

	scyllaCDCMinToken           = int64Token(math.MinInt64)
	scyllaCDCLogTableNameSuffix = "_scylla_cdc_log"
	scyllaCDCExtensionName      = "cdc"
)

type scyllaCDCPartitioner struct{}

var _ partitioner = scyllaCDCPartitioner{}

func (p scyllaCDCPartitioner) Name() string {
	return scyllaCDCPartitionerName
}

func (p scyllaCDCPartitioner) Hash(partitionKey []byte) token {
	if len(partitionKey) < 8 {
		// The key is too short to extract any sensible token,
		// so return the min token instead
		if gocqlDebug {
			Logger.Printf("scylla: cdc partition key too short: %d < 8", len(partitionKey))
		}
		return scyllaCDCMinToken
	}

	upperQword := binary.BigEndian.Uint64(partitionKey[0:])

	if gocqlDebug {
		// In debug mode, do some more checks

		if len(partitionKey) != scyllaCDCPartitionKeyLength {
			// The token has unrecognized format, but the first quadword
			// should be the token value that we want
			Logger.Printf("scylla: wrong size of cdc partition key: %d", len(partitionKey))
		}

		lowerQword := binary.BigEndian.Uint64(partitionKey[8:])
		version := lowerQword & scyllaCDCVersionMask
		if version < scyllaCDCMinSupportedVersion || version > scyllaCDCMaxSupportedVersion {
			// We don't support this version yet,
			// the token may be wrong
			Logger.Printf(
				"scylla: unsupported version: %d is not in range [%d, %d]",
				version,
				scyllaCDCMinSupportedVersion,
				scyllaCDCMaxSupportedVersion,
			)
		}
	}

	return int64Token(upperQword)
}

func (p scyllaCDCPartitioner) ParseString(str string) token {
	return parseInt64Token(str)
}

func scyllaIsCdcTable(session *Session, keyspaceName, tableName string) (bool, error) {
	if !strings.HasSuffix(tableName, scyllaCDCLogTableNameSuffix) {
		// Not a CDC table, use the default partitioner
		return false, nil
	}

	// Get the table metadata to see if it has the cdc partitioner set
	keyspaceMeta, err := session.KeyspaceMetadata(keyspaceName)
	if err != nil {
		return false, err
	}

	tableMeta, ok := keyspaceMeta.Tables[tableName]
	if !ok {
		return false, ErrNoMetadata
	}

	return tableMeta.Options.Partitioner == scyllaCDCPartitionerFullName, nil
}
