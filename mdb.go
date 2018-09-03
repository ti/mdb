package mdb

import (
	"github.com/globalsign/mgo"
	"time"
	"strings"
	"io"
	"net"
	"net/url"
	"errors"
	"strconv"
)


type Mode int

const (
	// Relevant documentation on read preference modes:
	//
	//     http://docs.mongodb.org/manual/reference/read-preference/
	//
	Primary            Mode = 2 // Default mode. All operations read from the current replica set primary.
	PrimaryPreferred   Mode = 3 // Read from the primary if available. Read from the secondary otherwise.
	Secondary          Mode = 4 // Read from one of the nearest secondary members of the replica set.
	SecondaryPreferred Mode = 5 // Read from one of the nearest secondaries if available. Read from primary otherwise.
	Nearest            Mode = 6 // Read from one of the nearest members, irrespective of it being primary or secondary.

	// Read preference modes are specific to mgo:
	Eventual  Mode = 0 // Same as Nearest, but may change servers between reads.
	Monotonic Mode = 1 // Same as SecondaryPreferred before first write. Same as Primary after first write.
	Strong    Mode = 2 // Same as Primary.
)
// Dial establishes a new session to the cluster identified by the given seed
// server(s). The session will enable communication with all of the servers in
// the cluster, so the seed servers are used only to find out about the cluster
// topology.
//
// The following connection options are supported after the question mark:
//
//    maxRetries  : max retries time  when network is error, default is 2
//    db          : database name when your connection string and database name is diffrent
//
//    use mongo official connection string + &db=dbname to connect
//
//    exp: mongodb://user:pass@192.168.1.1:27017?dbname=test
//
// Relevant documentation:
//
//     http://docs.mongodb.org/manual/reference/connection-string/
//
func Dial(mgoUrl string) (*Database, error) {
	uri, err := url.Parse(mgoUrl)
	if err != nil {
		return nil, err
	}
	query := uri.Query()
	var dbName string
	if db := query.Get("db"); db != "" {
		dbName = db
		query.Del("db")
	} else if len(uri.Path) > 1 {
		dbName = uri.Path[1:]
	} else {
		return nil, errors.New("please use mongodb://***/dbName or  mongodb://***?db=dbName to config default dbName")
	}
	maxRetries := 2
	if n := query.Get("maxRetries"); n != "" {
		tryn, err := strconv.Atoi(n)
		if err != nil {
			return nil, err
		}
		maxRetries = tryn
		query.Del("maxRetries")
	}
	uri.RawQuery = query.Encode()
	mgoUrl = uri.String()
	session, err := mgo.Dial(mgoUrl)
	return &Database{ Name:dbName, MaxConnectRetries: maxRetries, session:session}, err
}


//isNetworkError mgo common error is eof Closed explicitly
func isNetworkError(err error)  bool {
	if err == nil {
		return false
	}
	if err == io.EOF {
		return true
	}
	if _, ok := err.(*net.OpError); ok {
		return true
	}
	e := strings.ToLower(err.Error())
	if strings.HasPrefix(e, "closed") || strings.HasSuffix(e,"closed"){
		return true
	}
	return false
}



type Database struct {
	Name    string
	MaxConnectRetries int
	session *mgo.Session
	refreshing bool
}

func (db *Database) DB(name string) *Database {
	return &Database{session:db.session, Name:name, MaxConnectRetries: db.MaxConnectRetries,}
}

func (db *Database) Close(){
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
	return  db.session.BuildInfo()
}

func (db *Database) Clone() *Database {
	return &Database{session:db.session.Clone(), Name:db.Name}
}

func (db *Database) Copy() *Database {
	return &Database{session:db.session.Copy(), Name:db.Name}
}

func (db *Database) Run(cmd interface{}, result interface{}) error {
	err := db.session.DB(db.Name).Run(cmd, result)
	if err != nil && isNetworkError(err){
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
