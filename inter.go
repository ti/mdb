package mdb

import (
	"gopkg.in/mgo.v2"
)

type Iter struct {
	i  *mgo.Iter
	db *Database
}



// Err returns nil if no errors happened during iteration, or the actual
// error otherwise.
//
// In case a resulting document included a field named $err or errmsg, which are
// standard ways for MongoDB to report an improper query, the returned value has
// a *QueryError type, and includes the Err message and the Code.
func (iter *Iter) Err() (err error) {
	return iter.i.Err()
}

// Close kills the server cursor used by the iterator, if any, and returns
// nil if no errors happened during iteration, or the actual error otherwise.
//
// Server cursors are automatically closed at the end of an iteration, which
// means close will do nothing unless the iteration was interrupted before
// the server finished sending results to the driver. If Close is not called
// in such a situation, the cursor will remain available at the server until
// the default cursor timeout period is reached. No further problems arise.
//
// Close is idempotent. That means it can be called repeatedly and will
// return the same result every time.
//
// In case a resulting document included a field named $err or errmsg, which are
// standard ways for MongoDB to report an improper query, the returned value has
// a *QueryError type.
func (iter *Iter) Close() (err error) {
	for i := 0; i < iter.db.MaxConnectRetries; i++ {
		err = iter.i.Close()
		if !isNetworkError(err) {
			return
		}
		iter.db.refresh()
	}
	return err
}

// Done returns true only if a follow up Next call is guaranteed
// to return false.
//
// For an iterator created with Tail, Done may return false for
// an iterator that has no more data. Otherwise it's guaranteed
// to return false only if there is data or an error happened.
//
// Done may block waiting for a pending query to verify whether
// more data is actually available or not.
func (iter *Iter) Done() bool {
	return iter.i.Done()
}

// Timeout returns true if Next returned false due to a timeout of
// a tailable cursor. In those cases, Next may be called again to continue
// the iteration at the previous cursor position.
func (iter *Iter) Timeout() bool {
	return iter.i.Timeout()
}

// Next retrieves the next document from the result set, blocking if necessary.
// This method will also automatically retrieve another batch of documents from
// the server when the current one is exhausted, or before that in background
// if pre-fetching is enabled (see the Query.Prefetch and Session.SetPrefetch
// methods).
//
// Next returns true if a document was successfully unmarshalled onto result,
// and false at the end of the result set or if an error happened.
// When Next returns false, the Err method should be called to verify if
// there was an error during iteration.
//
// For example:
//
//    iter := collection.Find(nil).Iter()
//    for iter.Next(&result) {
//        fmt.Printf("Result: %v\n", result.Id)
//    }
//    if err := iter.Close(); err != nil {
//        return err
//    }
//
func (iter *Iter) Next(result interface{}) bool {
	return iter.i.Next(result)
}

// All retrieves all documents from the result set into the provided slice
// and closes the iterator.
//
// The result argument must necessarily be the address for a slice. The slice
// may be nil or previously allocated.
//
// WARNING: Obviously, All must not be used with result sets that may be
// potentially large, since it may consume all memory until the system
// crashes. Consider building the query with a Limit clause to ensure the
// result size is bounded.
//
// For instance:
//
//    var result []struct{ Value int }
//    iter := collection.Find(nil).Limit(100).Iter()
//    err := iter.All(&result)
//    if err != nil {
//        return err
//    }
//
func (iter *Iter) All(result interface{}) (err error) {
	for i := 0; i < iter.db.MaxConnectRetries; i++ {
		err = iter.i.All(result)
		if !isNetworkError(err) {
			return
		}
		iter.db.refresh()
	}
	return err
}
