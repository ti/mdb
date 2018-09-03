package mdb

import (
	"github.com/globalsign/mgo"
	"time"
)

type Query struct {
	q  *mgo.Query
	db *Database
}

// Batch sets the batch size used when fetching documents from the database.
// It's possible to change this setting on a per-session basis as well, using
// the Batch method of Session.

// The default batch size is defined by the database itself.  As of this
// writing, MongoDB will use an initial size of min(100 docs, 4MB) on the
// first batch, and 4MB on remaining ones.
func (q *Query) Batch(n int) *Query {
	q.q = q.q.Batch(n)
	return q
}

// Prefetch sets the point at which the next batch of results will be requested.
// When there are p*batch_size remaining documents cached in an Iter, the next
// batch will be requested in background. For instance, when using this:
//
//     query.Batch(200).Prefetch(0.25)
//
// and there are only 50 documents cached in the Iter to be processed, the
// next batch of 200 will be requested. It's possible to change this setting on
// a per-session basis as well, using the SetPrefetch method of Session.
//
// The default prefetch value is 0.25.
func (q *Query) Prefetch(p float64) *Query {
	q.q = q.q.Prefetch(p)
	return q
}

// Skip skips over the n initial documents from the query results.  Note that
// this only makes sense with capped collections where documents are naturally
// ordered by insertion time, or with sorted results.
func (q *Query) Skip(n int) *Query {
	q.q = q.q.Skip(n)
	return q
}

// Limit restricts the maximum number of documents retrieved to n, and also
// changes the batch size to the same value.  Once n documents have been
// returned by Next, the following call will return ErrNotFound.
func (q *Query) Limit(n int) *Query {
	q.q = q.q.Limit(n)
	return q
}

// Select enables selecting which fields should be retrieved for the results
// found. For example, the following query would only retrieve the name field:
//
//     err := collection.Find(nil).Select(bson.M{"name": 1}).One(&result)
//
// Relevant documentation:
//
//     http://www.mongodb.org/display/DOCS/Retrieving+a+Subset+of+Fields
//
func (q *Query) Select(selector interface{}) *Query {
	q.q = q.q.Select(selector)
	return q
}

// Sort asks the database to order returned documents according to the
// provided field names. A field name may be prefixed by - (minus) for
// it to be sorted in reverse order.
//
// For example:
//
//     query1 := collection.Find(nil).Sort("firstname", "lastname")
//     query2 := collection.Find(nil).Sort("-age")
//     query3 := collection.Find(nil).Sort("$natural")
//     query4 := collection.Find(nil).Select(bson.M{"score": bson.M{"$meta": "textScore"}}).Sort("$textScore:score")
//
// Relevant documentation:
//
//     http://www.mongodb.org/display/DOCS/Sorting+and+Natural+Order
//
func (q *Query) Sort(fields ...string) *Query {
	q.q = q.q.Sort(fields...)
	return q
}

// Explain returns a number of details about how the MongoDB server would
// execute the requested query, such as the number of objects examined,
// the number of times the read lock was yielded to allow writes to go in,
// and so on.
//
// For example:
//
//     m := bson.M{}
//     err := collection.Find(bson.M{"filename": name}).Explain(m)
//     if err == nil {
//         fmt.Printf("Explain: %#v\n", m)
//     }
//
// Relevant documentation:
//
//     http://www.mongodb.org/display/DOCS/Optimization
//     http://www.mongodb.org/display/DOCS/Query+Optimizer
//
func (q *Query) Explain(result interface{}) (err error) {
	for i := 0; i < q.db.MaxConnectRetries; i++ {
		err = q.q.Explain(result)
		if err == nil {
			return
		}
		if !isNetworkError(err) {
			continue
		}
		q.db.refresh()
	}
	return err
}

// TODO: Add Collection.Explain. See https://goo.gl/1MDlvz.

// Hint will include an explicit "hint" in the query to force the server
// to use a specified index, potentially improving performance in some
// situations.  The provided parameters are the fields that compose the
// key of the index to be used.  For details on how the indexKey may be
// built, see the EnsureIndex method.
//
// For example:
//
//     query := collection.Find(bson.M{"firstname": "Joe", "lastname": "Winter"})
//     query.Hint("lastname", "firstname")
//
// Relevant documentation:
//
//     http://www.mongodb.org/display/DOCS/Optimization
//     http://www.mongodb.org/display/DOCS/Query+Optimizer
//
func (q *Query) Hint(indexKey ...string) *Query {
	q.q = q.q.Hint(indexKey...)
	return q
}

// SetMaxScan constrains the query to stop after scanning the specified
// number of documents.
//
// This modifier is generally used to prevent potentially long running
// queries from disrupting performance by scanning through too much data.
func (q *Query) SetMaxScan(n int) *Query {
	q.q = q.q.SetMaxScan(n)
	return q
}

// SetMaxTime constrains the query to stop after running for the specified time.
//
// When the time limit is reached MongoDB automatically cancels the query.
// This can be used to efficiently prevent and identify unexpectedly slow queries.
//
// A few important notes about the mechanism enforcing this limit:
//
//  - Requests can block behind locking operations on the server, and that blocking
//    time is not accounted for. In other words, the timer starts ticking only after
//    the actual start of the query when it initially acquires the appropriate lock;
//
//  - Operations are interrupted only at interrupt points where an operation can be
//    safely aborted – the total execution time may exceed the specified value;
//
//  - The limit can be applied to both CRUD operations and commands, but not all
//    commands are interruptible;
//
//  - While iterating over results, computing follow up batches is included in the
//    total time and the iteration continues until the alloted time is over, but
//    network roundtrips are not taken into account for the limit.
//
//  - This limit does not override the inactive cursor timeout for idle cursors
//    (default is 10 min).
//
// This mechanism was introduced in MongoDB 2.6.
//
// Relevant documentation:
//
//   http://blog.mongodb.org/post/83621787773/maxtimems-and-query-optimizer-introspection-in
//
func (q *Query) SetMaxTime(d time.Duration) *Query {
	q.q = q.q.SetMaxTime(d)
	return q
}

// Snapshot will force the performed query to make use of an available
// index on the _id field to prevent the same document from being returned
// more than once in a single iteration. This might happen without this
// setting in situations when the document changes in size and thus has to
// be moved while the iteration is running.
//
// Because snapshot mode traverses the _id index, it may not be used with
// sorting or explicit hints. It also cannot use any other index for the
// query.
//
// Even with snapshot mode, items inserted or deleted during the query may
// or may not be returned; that is, this mode is not a true point-in-time
// snapshot.
//
// The same effect of Snapshot may be obtained by using any unique index on
// field(s) that will not be modified (best to use Hint explicitly too).
// A non-unique index (such as creation time) may be made unique by
// appending _id to the index when creating it.
//
// Relevant documentation:
//
//     http://www.mongodb.org/display/DOCS/How+to+do+Snapshotted+Queries+in+the+Mongo+Database
//
func (q *Query) Snapshot() *Query {
	q.q = q.q.Snapshot()
	return q
}

// Comment adds a comment to the query to identify it in the database profiler output.
//
// Relevant documentation:
//
//     http://docs.mongodb.org/manual/reference/operator/meta/comment
//     http://docs.mongodb.org/manual/reference/command/profile
//     http://docs.mongodb.org/manual/administration/analyzing-mongodb-performance/#database-profiling
//
func (q *Query) Comment(comment string) *Query {
	q.q = q.q.Comment(comment)
	return q
}

// LogReplay enables an option that optimizes queries that are typically
// made on the MongoDB oplog for replaying it. This is an internal
// implementation aspect and most likely uninteresting for other uses.
// It has seen at least one use case, though, so it's exposed via the API.
func (q *Query) LogReplay() *Query {
	q.q = q.q.LogReplay()
	return q
}

// One executes the query and unmarshals the first obtained document into the
// result argument.  The result must be a struct or map value capable of being
// unmarshalled into by gobson.  This function blocks until either a result
// is available or an error happens.  For example:
//
//     err := collection.Find(bson.M{"a": 1}).One(&result)
//
// In case the resulting document includes a field named $err or errmsg, which
// are standard ways for MongoDB to return query errors, the returned err will
// be set to a *QueryError value including the Err message and the Code.  In
// those cases, the result argument is still unmarshalled into with the
// received document so that any other custom values may be obtained if
// desired.
//
func (q *Query) One(result interface{}) (err error) {
	for i := 0; i < q.db.MaxConnectRetries; i++ {
		err = q.q.One(result)
		if err == nil {
			return
		}
		if !isNetworkError(err) {
			continue
		}
		q.db.refresh()
	}
	return err
}

// Count returns the total number of documents in the result set.
func (q *Query) Count() (n int, err error) {
	return q.q.Count()
}

// Iter executes the query and returns an iterator capable of going over all
// the results. Results will be returned in batches of configurable
// size (see the Batch method) and more documents will be requested when a
// configurable number of documents is iterated over (see the Prefetch method).
func (q *Query) Iter() *Iter {
	return &Iter{
		i:  q.q.Iter(),
		db: q.db,
	}

}

// Distinct unmarshals into result the list of distinct values for the given key.
//
// For example:
//
//     var result []int
//     err := collection.Find(bson.M{"gender": "F"}).Distinct("age", &result)
//
// Relevant documentation:
//
//     http://www.mongodb.org/display/DOCS/Aggregation
//
func (q *Query) Distinct(key string, result interface{}) error {
	return q.q.Distinct(key, result)
}

// MapReduce executes a map/reduce job for documents covered by the query.
// That kind of job is suitable for very flexible bulk aggregation of data
// performed at the server side via Javascript functions.
//
// Results from the job may be returned as a result of the query itself
// through the result parameter in case they'll certainly fit in memory
// and in a single document.  If there's the possibility that the amount
// of data might be too large, results must be stored back in an alternative
// collection or even a separate database, by setting the Out field of the
// provided MapReduce job.  In that case, provide nil as the result parameter.
//
// These are some of the ways to set Out:
//
//     nil
//         Inline results into the result parameter.
//
//     bson.M{"replace": "mycollection"}
//         The output will be inserted into a collection which replaces any
//         existing collection with the same name.
//
//     bson.M{"merge": "mycollection"}
//         This option will merge new data into the old output collection. In
//         other words, if the same key exists in both the result set and the
//         old collection, the new key will overwrite the old one.
//
//     bson.M{"reduce": "mycollection"}
//         If documents exist for a given key in the result set and in the old
//         collection, then a reduce operation (using the specified reduce
//         function) will be performed on the two values and the result will be
//         written to the output collection. If a finalize function was
//         provided, this will be run after the reduce as well.
//
//     bson.M{...., "db": "mydb"}
//         Any of the above options can have the "db" key included for doing
//         the respective action in a separate database.
//
// The following is a trivial example which will count the number of
// occurrences of a field named n on each document in a collection, and
// will return results inline:
//
//     job := &mgo.MapReduce{
//             Map:      "function() { emit(this.n, 1) }",
//             Reduce:   "function(key, values) { return Array.sum(values) }",
//     }
//     var result []struct { Id int "_id"; Value int }
//     _, err := collection.Find(nil).MapReduce(job, &result)
//     if err != nil {
//         return err
//     }
//     for _, item := range result {
//         fmt.Println(item.Value)
//     }
//
// This function is compatible with MongoDB 1.7.4+.
//
// Relevant documentation:
//
//     http://www.mongodb.org/display/DOCS/MapReduce
//
func (q *Query) MapReduce(job *mgo.MapReduce, result interface{}) (info *mgo.MapReduceInfo, err error) {
	return q.q.MapReduce(job, result)
}

// Apply runs the findAndModify MongoDB command, which allows updating, upserting
// or removing a document matching a query and atomically returning either the old
// version (the default) or the new version of the document (when ReturnNew is true).
// If no objects are found Apply returns ErrNotFound.
//
// The Sort and Select query methods affect the result of Apply.  In case
// multiple documents match the query, Sort enables selecting which document to
// act upon by ordering it first.  Select enables retrieving only a selection
// of fields of the new or old document.
//
// This simple example increments a counter and prints its new value:
//
//     change := mgo.Change{
//             Update: bson.M{"$inc": bson.M{"n": 1}},
//             ReturnNew: true,
//     }
//     info, err = col.Find(M{"_id": id}).Apply(change, &doc)
//     fmt.Println(doc.N)
//
// This method depends on MongoDB >= 2.0 to work properly.
//
// Relevant documentation:
//
//     http://www.mongodb.org/display/DOCS/findAndModify+Command
//     http://www.mongodb.org/display/DOCS/Updating
//     http://www.mongodb.org/display/DOCS/Atomic+Operations
//
func (q *Query) Apply(change mgo.Change, result interface{}) (info *mgo.ChangeInfo, err error) {
	for i := 0; i < q.db.MaxConnectRetries; i++ {
		info, err = q.q.Apply(change, result)
		if err == nil {
			return
		}
		if !isNetworkError(err) {
			continue
		}
		q.db.refresh()
	}
	return info, err
}

// All works like Iter.All.
func (q *Query) All(result interface{}) (err error) {
	for i := 0; i < q.db.MaxConnectRetries; i++ {
		err = q.q.All(result)
		if err == nil {
			return
		}
		if !isNetworkError(err) {
			continue
		}
		q.db.refresh()
	}
	return err
}

// The For method is obsolete and will be removed in a future release.
// See Iter as an elegant replacement.
func (q *Query) For(result interface{}, f func() error) error {
	return q.q.For(result, f)
}
