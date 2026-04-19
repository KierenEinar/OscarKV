package oscarkv

import (
	"OscarKV/kv"
)

func (db *DB) commitWorker() {

	batch := make([]*kv.Requests, 0, func(a, b int) int { if a > b { return a } ; return b }(db.writeBatchCountThreshold/4, 24))
	nextFront := make([]*kv.Requests, 0, 1)

	for front := range db.commitQueue {

loop:
		count := 0
		size := 0

		if len(nextFront) > 0 {
			batch = append(batch, nextFront[0])
			count += nextFront[0].Count
			size += nextFront[0].Size
			nextFront = nextFront[:0]
		} else {
			batch = append(batch, front)
			count += front.Count
			size += front.Size
		}

merge:
		for {
			select {
			case req, ok := <-db.commitQueue:
				if !ok {
					break merge
				}

				if req.Size >= db.writeBatchSizeThreshold {
					req.Result <- ErrTxnTooBig
				}

				if count+req.Count >= db.writeBatchCountThreshold ||
					size+req.Size >= db.writeBatchSizeThreshold {

					nextFront = append(nextFront, req)
					break merge
				}

				count += req.Count
				size += req.Size
				batch = append(batch, req)

			default:
				break merge
			}
		}

		// flush
		if len(batch) > 0 {
			db.commitBatch(batch)
			batch = batch[:0]
		}

		if len(nextFront) > 0 {
			goto loop
		}
	}

	db.commitClose <- true

}

func (db *DB) commitBatch(batch []*kv.Requests) {
	err := db.lsm.SetBatch(batch)
	for _, req := range batch {
		req.Result <- err
	}
}
