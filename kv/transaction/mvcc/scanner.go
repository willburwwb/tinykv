package mvcc

import (
	"bytes"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
)

// Scanner is used for reading multiple sequential key/value pairs from the storage layer. It is aware of the implementation
// of the storage layer and returns results suitable for users.
// Invariant: either the scanner is finished and cannot be used, or it is ready to return a value immediately.
type Scanner struct {
	// Your Data Here (4C).
	nextKey []byte
	txn     *MvccTxn
	iter    engine_util.DBIterator
}

// NewScanner creates a new scanner ready to read from the snapshot in txn.
func NewScanner(startKey []byte, txn *MvccTxn) *Scanner {
	// Your Code Here (4C).
	iter := txn.Reader.IterCF(engine_util.CfWrite)
	if !iter.Valid() {
		return nil
	}
	return &Scanner{
		nextKey: startKey,
		txn:     txn,
		iter:    iter,
	}
}

func (scan *Scanner) Close() {
	// Your Code Here (4C).
}

// Next returns the next key/value pair from the scanner. If the scanner is exhausted, then it will return `nil, nil, nil`.
func (scan *Scanner) Next() ([]byte, []byte, error) {
	// Your Code Here (4C).
	if !scan.iter.Valid() {
		return nil, nil, nil
	}

	scan.iter.Seek(EncodeKey(scan.nextKey, scan.txn.StartTS))
	if !scan.iter.Valid() {
		return nil, nil, nil
	}

	keyWithTs := scan.iter.Item().Key()
	key := DecodeUserKey(keyWithTs)
	dataValue, err := scan.txn.GetValue(key) // 不要用 Encode(scan.nextKey, scan.txn.StartTS)，因为不一定有value
	if err != nil {
		return nil, nil, err
	}

	scan.iter.Next()
	for scan.iter.Valid() {
		nextKey := DecodeUserKey(scan.iter.Item().Key())
		//value, _ := scan.iter.Item().Value()
		//write, _ := ParseWrite(value)
		if !bytes.Equal(nextKey, key) {
			scan.nextKey = nextKey
			break
		}
		//log.Infof("skip %v %v", nextKey, write)
		scan.iter.Next()
	}

	if dataValue == nil {
		// 跳过 key 为空的时候，直接进入下一个 key
		return scan.Next()
	}
	//log.Infof("%v %v %v", key, dataValue, err)
	return key, dataValue, err
}

func (scan *Scanner) Iter() engine_util.DBIterator {
	return scan.iter
}
