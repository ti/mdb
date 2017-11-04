# mdb

A rich mongodb driver based on mgo and auto refresh when "Closed explicitly" or "EOF"

# feature

* do not need `copy := session.Clone; defter copy.Close();`
* use db instance in project
* less tcp connections
* auto refresh connections when connection is break
* more simple

# why this one

if you use  `copy := session.Clone; defter copy.Close(); copy.DB("dbname).C("col").Find(...)` 

you may got "Closed explicitly" or "EOF"  when in high concurrency

# quick start

```go
type Person struct {
	Name string
	Phone string
}

func main() {
    //the test is default db
	db, err := mdb.Dial("mongodb://127.0.0.1:27017/test")
	if err != nil {
		panic(err)
	}
	defer db.Close()
  
	c := db.C("people")
	err = c.Insert(&Person{"Ale", "+55 53 8116 9639"},&Person{"Cla", "+55 53 8402 8510"})
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
# when mongo connection string and database name is different?

use [mongo-connection-string](https://docs.mongodb.com/manual/reference/connection-string/) + `&db={db_name}` use  to config your db name

example:

```go
db, err := mdb.Dial("mongodb://username:password@192.168.31.5:27017?db=test")
```

when username is not an administrator

```go
db, err := mdb.Dial("mongodb://username:password@127.0.0.1:27017/test")
//when you have to connect another db first
db, err := mdb.Dial("mongodb://username:password@127.0.0.1:27017/db_for_connect?db=test")
```

