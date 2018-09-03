package mdb

import (
	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
)

// Collection stores documents
//
// Relevant documentation:
//
//    https://docs.mongodb.com/manual/core/databases-and-collections/#collections
//
type Collection struct {
	Database *Database
	Name     string
	col      *mgo.Collection
}

// Insert inserts one or more documents in the respective collection.  In
// case the session is in safe mode (see the SetSafe method) and an error
// happens while inserting the provided documents, the returned error will
// be of type *LastError.
func (c *Collection) Insert(docs ...interface{}) (err error) {
	for i := 0; i < c.Database.MaxConnectRetries; i++ {
		err = c.col.Insert(docs...)
		if !isNetworkError(err) {
			return
		}
		c.Database.refresh()
	}
	return err
}

// Count returns the total number of documents in the collection.
func (c *Collection) Count() (n int, err error) {
	for i := 0; i < c.Database.MaxConnectRetries; i++ {
		n, err = c.col.Count()
		if !isNetworkError(err) {
			return
		}
		c.Database.refresh()
	}
	return n, err
}

// Create explicitly creates the c collection with details of info.
// MongoDB creates collections automatically on use, so this method
// is only necessary when creating collection with non-default
// characteristics, such as capped collections.
//
// Relevant documentation:
//
//     http://www.mongodb.org/display/DOCS/createCollection+Command
//     http://www.mongodb.org/display/DOCS/Capped+Collections
//
func (c *Collection) Create(info *mgo.CollectionInfo) (err error) {
	for i := 0; i < c.Database.MaxConnectRetries; i++ {
		err = c.col.Create(info)
		if !isNetworkError(err) {
			return
		}
		c.Database.refresh()
	}
	return err
}

// DropCollection removes the entire collection including all of its documents.
func (c *Collection) DropCollection() (err error) {
	for i := 0; i < c.Database.MaxConnectRetries; i++ {
		err = c.col.DropCollection()
		if !isNetworkError(err) {
			return
		}
		c.Database.refresh()
	}
	return err
}

// DropIndexName removes the index with the provided index name.
//
// For example:
//
//     err := collection.DropIndex("customIndexName")
//
func (c *Collection) DropIndexName(name string) (err error) {
	for i := 0; i < c.Database.MaxConnectRetries; i++ {
		err = c.col.DropIndexName(name)
		if !isNetworkError(err) {
			return
		}
		c.Database.refresh()
	}
	return err
}

// DropIndex drops the index with the provided key from the c collection.
//
// See EnsureIndex for details on the accepted key variants.
//
// For example:
//
//     err1 := collection.DropIndex("firstField", "-secondField")
//     err2 := collection.DropIndex("customIndexName")
//
func (c *Collection) DropIndex(key ...string) (err error) {
	for i := 0; i < c.Database.MaxConnectRetries; i++ {
		err = c.col.DropIndex(key...)
		if !isNetworkError(err) {
			return
		}
		c.Database.refresh()
	}
	return err
}

// EnsureIndex ensures an index with the given key exists, creating it with
// the provided parameters if necessary. EnsureIndex does not modify a previously
// existent index with a matching key. The old index must be dropped first instead.
//
// Once EnsureIndex returns successfully, following requests for the same index
// will not contact the server unless Collection.DropIndex is used to drop the
// same index, or Session.ResetIndexCache is called.
//
// For example:
//
//     index := Index{
//         Key: []string{"lastname", "firstname"},
//         Unique: true,
//         DropDups: true,
//         Background: true, // See notes.
//         Sparse: true,
//     }
//     err := collection.EnsureIndex(index)
//
// The Key value determines which fields compose the index. The index ordering
// will be ascending by default.  To obtain an index with a descending order,
// the field name should be prefixed by a dash (e.g. []string{"-time"}). It can
// also be optionally prefixed by an index kind, as in "$text:summary" or
// "$2d:-point". The key string format is:
//
//     [$<kind>:][-]<field name>
//
// If the Unique field is true, the index must necessarily contain only a single
// document per Key.  With DropDups set to true, documents with the same key
// as a previously indexed one will be dropped rather than an error returned.
//
// If Background is true, other connections will be allowed to proceed using
// the collection without the index while it's being built. Note that the
// session executing EnsureIndex will be blocked for as long as it takes for
// the index to be built.
//
// If Sparse is true, only documents containing the provided Key fields will be
// included in the index.  When using a sparse index for sorting, only indexed
// documents will be returned.
//
// If ExpireAfter is non-zero, the server will periodically scan the collection
// and remove documents containing an indexed time.Time field with a value
// older than ExpireAfter. See the documentation for details:
//
//     http://docs.mongodb.org/manual/tutorial/expire-data
//
// Other kinds of indexes are also supported through that API. Here is an example:
//
//     index := Index{
//         Key: []string{"$2d:loc"},
//         Bits: 26,
//     }
//     err := collection.EnsureIndex(index)
//
// The example above requests the creation of a "2d" index for the "loc" field.
//
// The 2D index bounds may be changed using the Min and Max attributes of the
// Index value.  The default bound setting of (-180, 180) is suitable for
// latitude/longitude pairs.
//
// The Bits parameter sets the precision of the 2D geohash values.  If not
// provided, 26 bits are used, which is roughly equivalent to 1 foot of
// precision for the default (-180, 180) index bounds.
//
// Relevant documentation:
//
//     http://www.mongodb.org/display/DOCS/Indexes
//     http://www.mongodb.org/display/DOCS/Indexing+Advice+and+FAQ
//     http://www.mongodb.org/display/DOCS/Indexing+as+a+Background+Operation
//     http://www.mongodb.org/display/DOCS/Geospatial+Indexing
//     http://www.mongodb.org/display/DOCS/Multikeys
//
func (c *Collection) EnsureIndex(index mgo.Index) (err error) {
	for i := 0; i < c.Database.MaxConnectRetries; i++ {
		err = c.col.EnsureIndex(index)
		if !isNetworkError(err) {
			return
		}
		c.Database.refresh()
	}
	return err
}

// Pipe prepares a pipeline to aggregate. The pipeline document
// must be a slice built in terms of the aggregation framework language.
//
// For example:
//
//     pipe := collection.Pipe([]bson.M{{"$match": bson.M{"name": "Otavio"}}})
//     iter := pipe.Iter()
//
// Relevant documentation:
//
//     http://docs.mongodb.org/manual/reference/aggregation
//     http://docs.mongodb.org/manual/applications/aggregation
//     http://docs.mongodb.org/manual/tutorial/aggregation-examples
//

func (c *Collection) Pipe(Pipe interface{}) *mgo.Pipe {
	return c.col.Pipe(Pipe)
}

// Remove finds a single document matching the provided selector document
// and removes it from the database.
// If the session is in safe mode (see SetSafe) a ErrNotFound error is
// returned if a document isn't found, or a value of type *LastError
// when some other error is detected.
//
// Relevant documentation:
//
//     http://www.mongodb.org/display/DOCS/Removing
//
func (c *Collection) Remove(selector interface{}) (err error) {
	for i := 0; i < c.Database.MaxConnectRetries; i++ {
		err = c.col.Remove(selector)
		if !isNetworkError(err) {
			return
		}
		c.Database.refresh()
	}
	return err
}

// RemoveId is a convenience helper equivalent to:
//
//     err := collection.Remove(bson.M{"_id": id})
//
// See the Remove method for more details.
func (c *Collection) RemoveId(id interface{}) error {
	return c.Remove(bson.D{{"_id", id}})
}

// Indexes returns a list of all indexes for the collection.
//
// See the EnsureIndex method for more details on indexes.
func (c *Collection) Indexes() (indexes []mgo.Index, err error) {
	for i := 0; i < c.Database.MaxConnectRetries; i++ {
		indexes, err = c.col.Indexes()
		if !isNetworkError(err) {
			return
		}
		c.Database.refresh()
	}
	return indexes, err
}

// RemoveAll finds all documents matching the provided selector document
// and removes them from the database.  In case the session is in safe mode
// (see the SetSafe method) and an error happens when attempting the change,
// the returned error will be of type *LastError.
//
// Relevant documentation:
//
//     http://www.mongodb.org/display/DOCS/Removing
//
func (c *Collection) RemoveAll(selector interface{}) (info *mgo.ChangeInfo, err error) {
	for i := 0; i < c.Database.MaxConnectRetries; i++ {
		info, err = c.col.RemoveAll(selector)
		if !isNetworkError(err) {
			return
		}
		c.Database.refresh()
	}
	return info, err
}

// UpdateId is a convenience helper equivalent to:
//
//     err := collection.Update(bson.M{"_id": id}, update)
//
// See the Update method for more details.
func (c *Collection) UpdateId(id interface{}, update interface{}) (err error) {
	return c.Update(bson.M{"_id": id}, update)
}

// Update finds a single document matching the provided selector document
// and modifies it according to the update document.
// If the session is in safe mode (see SetSafe) a ErrNotFound error is
// returned if a document isn't found, or a value of type *LastError
// when some other error is detected.
//
// Relevant documentation:
//
//     http://www.mongodb.org/display/DOCS/Updating
//     http://www.mongodb.org/display/DOCS/Atomic+Operations
//
func (c *Collection) Update(id interface{}, update interface{}) (err error) {
	for i := 0; i < c.Database.MaxConnectRetries; i++ {
		err = c.col.Update(id, update)
		if !isNetworkError(err) {
			return
		}
		c.Database.refresh()
	}
	return err
}

// UpdateAll finds all documents matching the provided selector document
// and modifies them according to the update document.
// If the session is in safe mode (see SetSafe) details of the executed
// operation are returned in info or an error of type *LastError when
// some problem is detected. It is not an error for the update to not be
// applied on any documents because the selector doesn't match.
//
// Relevant documentation:
//
//     http://www.mongodb.org/display/DOCS/Updating
//     http://www.mongodb.org/display/DOCS/Atomic+Operations
//
func (c *Collection) UpdateAll(selector interface{}, update interface{}) (info *mgo.ChangeInfo, err error) {
	for i := 0; i < c.Database.MaxConnectRetries; i++ {
		info, err = c.col.UpdateAll(selector, update)
		if !isNetworkError(err) {
			return
		}
		c.Database.refresh()
	}
	return info, err
}

// Upsert finds a single document matching the provided selector document
// and modifies it according to the update document.  If no document matching
// the selector is found, the update document is applied to the selector
// document and the result is inserted in the collection.
// If the session is in safe mode (see SetSafe) details of the executed
// operation are returned in info, or an error of type *LastError when
// some problem is detected.
//
// Relevant documentation:
//
//     http://www.mongodb.org/display/DOCS/Updating
//     http://www.mongodb.org/display/DOCS/Atomic+Operations
//
func (c *Collection) Upsert(selector interface{}, update interface{}) (info *mgo.ChangeInfo, err error) {
	for i := 0; i < c.Database.MaxConnectRetries; i++ {
		info, err = c.col.Upsert(selector, update)
		if !isNetworkError(err) {
			return
		}
		c.Database.refresh()
	}
	return info, err
}

// UpsertId is a convenience helper equivalent to:
//
//     info, err := collection.Upsert(bson.M{"_id": id}, update)
//
// See the Upsert method for more details.
func (c *Collection) UpsertId(id interface{}, update interface{}) (info *mgo.ChangeInfo, err error) {
	for i := 0; i < c.Database.MaxConnectRetries; i++ {
		info, err = c.col.UpsertId(id, update)
		if !isNetworkError(err) {
			return
		}
		c.Database.refresh()
	}
	return info, err
}

// EnsureIndexKey ensures an index with the given key exists, creating it
// if necessary.
//
// This example:
//
//     err := collection.EnsureIndexKey("a", "b")
//
// Is equivalent to:
//
//     err := collection.EnsureIndex(mgo.Index{Key: []string{"a", "b"}})
//
// See the EnsureIndex method for more details.
func (c *Collection) EnsureIndexKey(key ...string) (err error) {
	return c.EnsureIndex(mgo.Index{Key: key})
}

// FindId is a convenience helper equivalent to:
//
//     query := collection.Find(bson.M{"_id": id})
//
// See the Find method for more details.
func (c *Collection) FindId(id interface{}) *Query {
	return c.Find(bson.D{{"_id", id}})
}

// Find prepares a query using the provided document.  The document may be a
// map or a struct value capable of being marshalled with bson.  The map
// may be a generic one using interface{} for its key and/or values, such as
// bson.M, or it may be a properly typed map.  Providing nil as the document
// is equivalent to providing an empty document such as bson.M{}.
//
// Further details of the query may be tweaked using the resulting Query value,
// and then executed to retrieve results using methods such as One, For,
// Iter, or Tail.
//
// In case the resulting document includes a field named $err or errmsg, which
// are standard ways for MongoDB to return query errors, the returned err will
// be set to a *QueryError value including the Err message and the Code.  In
// those cases, the result argument is still unmarshalled into with the
// received document so that any other custom values may be obtained if
// desired.
//
// Relevant documentation:
//
//     http://www.mongodb.org/display/DOCS/Querying
//     http://www.mongodb.org/display/DOCS/Advanced+Queries
//
func (c *Collection) Find(query interface{}) *Query {
	return &Query{db: c.Database, q: c.col.Find(query)}
}

// NewIter returns a newly created iterator with the provided parameters. Using
// this method is not recommended unless the desired functionality is not yet
// exposed via a more convenient interface (Find, Pipe, etc).
//
// The optional session parameter associates the lifetime of the returned
// iterator to an arbitrary session. If nil, the iterator will be bound to c's
// session.
//
// Documents in firstBatch will be individually provided by the returned
// iterator before documents from cursorId are made available. If cursorId is
// zero, only the documents in firstBatch are provided.
//
// If err is not nil, the iterator's Err method will report it after exhausting
// documents in firstBatch.
//
// NewIter must not be called on a collection in Eventual mode, because the
// cursor id is associated with the specific server that returned it. The
// provided session parameter may be in any mode or state, though.
//
// The new Iter fetches documents in batches of the server defined default,
// however this can be changed by setting the session Batch method.
//
// When using MongoDB 3.2+ NewIter supports re-using an existing cursor on the
// server. Ensure the connection has been established (i.e. by calling
// session.Ping()) before calling NewIter.
func (c *Collection) NewIter(firstBatch []bson.Raw, cursorId int64, err error) *Iter {
	return &Iter{i: c.col.NewIter(c.Database.session, firstBatch, cursorId, err), db: c.Database}
}

// Bulk returns a value to prepare the execution of a bulk operation.
func (c *Collection) Bulk() *mgo.Bulk {
	return c.col.Bulk()
}
