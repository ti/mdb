# mdb

A rich mongodb driver based on mgo and auto refresh when "Closed explicitly" or "EOF"

# feature

* do not need `copy := session.Clone; defter copy.Close();`
* use db instance in project
* less tcp connections
* auto refresh

# why this one

you do not need `copy := session.Clone; defter copy.Close(); copy.DB("dbname).C("col").Find(...)` to use mgo, this is not safe when open too may files.

mgo will "Closed explicitly" or "EOF"

# quick start

```go
package main

import (
	"log"
	"gopkg.in/mgo.v2/bson"
	"fmt"
	"github.com/ti/mdb"
)

type Person struct {
	Name string
	Phone string
}


func main() {
    //the test is default db, you can use db.DB(dbname) to other
	db, err := mdb.Dial("mongodb://127.0.0.1:27017/test")
	if err != nil {
		panic(err)
	}
	defer db.Close()

	// Optional. Switch the session to a monotonic behavior.
	db.SetMode(mdb.Monotonic, true)
	
	c := db.C("people")
	err = c.Insert(&Person{"Ale", "+55 53 8116 9639"},
		&Person{"Cla", "+55 53 8402 8510"})
	if err != nil {
		log.Fatal(err)
	}

	result := Person{}
	err = c.Find(bson.M{"name": "Ale"}).One(&result)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Phone:", result.Phone)

}

```

## TODO

add mdb.v3 to move all mgo branch
