package mdb

import (
	"testing"
	"log"
	"time"
)

func TestRefresh(t *testing.T) {

	//the test is default db, you can use db.DB(dbname) to other
	db, err := Dial("mongodb://127.0.0.1:27017/test")
	if err != nil {
		panic(err)
	}
	defer db.Close()

	// Optional. Switch the session to a monotonic behavior.
	db.SetMode(Monotonic, true)

	type Person struct {
		Name  string
		Phone string
	}

	c := db.C("people")
	err = c.Insert(&Person{"Ale", "+55 53 8116 9639"},
		&Person{"Cla", "+55 53 8402 8510"})
	if err != nil {
		log.Fatal(err)
	}

	for i := 0; i < 100; i++ {

		go func() {
			c := db.C("people")
			for i := 0; i < 900; i++ {
				err = c.Insert(&Person{"Ale", "+55 53 8116 9639"},
					&Person{"Cla", "+55 53 8402 8510"})
				if err != nil {
					log.Println(err)
					continue
				}
				time.Sleep(500 * time.Millisecond)
				err = c.Insert(&Person{"Ale", "+55 53 8116 9639"},
					&Person{"Cla", "+55 53 8402 8510"})
				if err != nil {
					log.Println(err)
					continue
				}
				log.Println("insert people", i)
			}

		}()
	}

	for {
		time.Sleep(10 * time.Second)
	}

}

