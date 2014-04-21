package main

import (
	"database/sql"
	"fmt"
	_ "github.com/mattn/go-sqlite3"
	"github.com/saintfish/orm.go"
	"os"
	"time"
)

type Int int
type Data struct {
	Id      Int
	private string
	S       string
	B       bool
	Ptr     *int
	D       time.Time
}

var dataSpec = orm.NewStructSpecBuilder(Data{}).
	GenericOtherFields().
	SetPrimaryKey("Id").
	Build()

func (d *Data) TableSpec() orm.TableSpec {
	return dataSpec
}

func main() {
	os.Remove("foo.db")
	db, err := sql.Open("sqlite3", "foo.db")
	if err != nil {
		panic(err)
	}
	o := orm.New(db)
	defer o.Close()
	err = o.CreateTable(&Data{}, true)
	if err != nil {
		panic(err)
	}
	d := &Data{
		Id:      100,
		private: "priv",
		S:       "hello",
		B:       true,
		Ptr:     nil,
		D:       time.Now(),
	}
	err = o.Insert(d)
	if err != nil {
		panic(err)
	}
	i := 10
	d = &Data{
		Id:  200,
		S:   "world",
		B:   false,
		Ptr: &i,
		D:   time.Now(),
	}
	err = o.Insert(d)
	if err != nil {
		panic(err)
	}
	dd := &Data{}
	err = o.Select().
		Where("S = ?", "hello").
		Find(dd)
	if err != nil {
		panic(err)
	}
	fmt.Println(dd)

	var array []Data
	err = o.Select().FindAll(&array)
	if err != nil {
		panic(err)
	}
	fmt.Println(array)

	dd = &Data{Id: dd.Id}
	err = o.FindByPrimaryKey(dd)
	if err != nil {
		panic(err)
	}
	fmt.Println(dd)

	dd.S = "WORLD"
	err = o.Update(dd, "Id = ?", dd.Id)
	if err != nil {
		panic(err)
	}

	d.S = "GoLang"
	err = o.UpdateByPrimaryKey(d)
	if err != nil {
		panic(err)
	}

	err = o.DeleteByPrimaryKey(d)
	if err != nil {
		panic(err)
	}
}
