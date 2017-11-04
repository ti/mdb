package mdb

import (
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

type Collection struct {
	Database *Database
	Name     string

	col      *mgo.Collection
}

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

func (c *Collection) Pipe(Pipe interface{}) *mgo.Pipe {
	return c.col.Pipe(Pipe)
}

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

func (c *Collection) RemoveId(id interface{}) error {
	return c.Remove(bson.D{{"_id", id}})
}

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

func (c *Collection) UpdateId(id interface{}, update interface{}) (err error) {
	return c.Update(bson.M{"_id": id}, update)
}

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

func (c *Collection) EnsureIndexKey(key ...string) (err error) {
	return c.EnsureIndex(mgo.Index{Key: key})
}

func (c *Collection) FindId(id interface{}) *Query {
	return c.Find(bson.D{{"_id", id}})
}

func (c *Collection) Find(query interface{}) *Query {
	return &Query{db:c.Database, q:c.col.Find(query)}
}

func (c *Collection) NewIter(firstBatch []bson.Raw, cursorId int64, err error) *Iter {
	return &Iter{i:c.col.NewIter(c.Database.session, firstBatch, cursorId, err), db: c.Database}
}

func (c *Collection) Bulk() *mgo.Bulk {
	return c.col.Bulk()
}

