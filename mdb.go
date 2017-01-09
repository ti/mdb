package mdb

import (
	"gopkg.in/mgo.v2"
	"time"
	"errors"
	"strings"
	"io"
)

var MAX_CONNECT_RETRIES = 3

type Mode int

const (
	// Relevant documentation on read preference modes:
	//
	//     http://docs.mongodb.org/manual/reference/read-preference/
	//
	Primary Mode = 2 // Default mode. All operations read from the current replica set primary.
	PrimaryPreferred Mode = 3 // Read from the primary if available. Read from the secondary otherwise.
	Secondary Mode = 4 // Read from one of the nearest secondary members of the replica set.
	SecondaryPreferred Mode = 5 // Read from one of the nearest secondaries if available. Read from primary otherwise.
	Nearest Mode = 6 // Read from one of the nearest members, irrespective of it being primary or secondary.

	// Read preference modes are specific to mgo:
	Eventual Mode = 0 // Same as Nearest, but may change servers between reads.
	Monotonic Mode = 1 // Same as SecondaryPreferred before first write. Same as Primary after first write.
	Strong Mode = 2 // Same as Primary.
)
//mgo common error is eof Closed explicitly
func isNetworkError(err error) bool {
	if err == nil {
		return false
	}
	if err == io.EOF {
		return true
	}
	e := strings.ToLower(err.Error())
	if strings.Contains(e, "close") || strings.Contains(e, "shutdown") || strings.Contains(e, "connection") {
		return true
	}
	return false
}

func Dial(url string) (*Database, error) {
	info, err := mgo.ParseURL(url)
	if err != nil {
		return nil, err
	}
	if info.Database == "" {
		return nil, errors.New("default database name requried from url")
	}
	info.Timeout = 10 * time.Second
	session, err := mgo.DialWithInfo(info)
	if err == nil {
		session.SetSyncTimeout(1 * time.Minute)
		session.SetSocketTimeout(1 * time.Minute)
	}
	return &Database{Name:info.Database, session:session}, err
}

type Database struct {
	Name       string
	session    *mgo.Session

	refreshing bool
}

func (db *Database) DB(name string) *Database {
	return &Database{session:db.session, Name:name}
}

func (db *Database) Close() {
	db.session.Close()
}


//blow is export mgo fuctions
func (db *Database) C(name string) *Collection {
	return &Collection{Database:db, Name:name, col:&mgo.Collection{&mgo.Database{db.session, db.Name}, name, db.Name + "." + name}}
}

func (db *Database) SetMode(consistency Mode, refresh bool) {
	db.session.SetMode(mgo.Mode(consistency), refresh)
}

func (db *Database) BuildInfo() (info mgo.BuildInfo, err error) {
	return db.session.BuildInfo()
}

func (db *Database) Clone() *Database {
	return &Database{session:db.session.Clone(), Name:db.Name}
}

func (db *Database) Copy() *Database {
	return &Database{session:db.session.Copy(), Name:db.Name}
}

func (db *Database) Run(cmd interface{}, result interface{}) error {
	err := db.session.DB(db.Name).Run(cmd, result)
	if err != nil && isNetworkError(err) {
		db.session.Refresh()
		return db.session.DB(db.Name).Run(cmd, result)
	}
	return err
}

func (db *Database) refresh() {
	if db.refreshing {
		time.Sleep(time.Second)
		return
	}
	db.refreshing = true
	db.session.Refresh()
	db.refreshing = false
}
