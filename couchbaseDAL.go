package couchbase

import (
	"github.com/couchbaselabs/go-couchbase"
	"errors"
)



type CouchbaseDal struct {
	location string
}

func Get(dbLocation string, key string, value interface{}) (err error) {
	c, err := couchbase.Connect("http://localhost:8091/")
	if (err != nil) { return }

	pool, err := c.GetPool("default")
	if (err != nil) { return }

	bucket, err := pool.GetBucket("SecretThing")
	if (err != nil) { return }
	defer bucket.Close()

	err = bucket.Get(key, value)
	if (err != nil) { return }

	return
}

func Remove(dbLocation string, key string) (err error) {
	c, err := couchbase.Connect(dbLocation)
	if (err != nil) { return }

	pool, err := c.GetPool("default")
	if (err != nil) { return }

	bucket, err := pool.GetBucket("SecretThing")
	if (err != nil) { return }
	defer bucket.Close()

	err = bucket.Delete(key)

	bucket.Close()

	return
}

func Set(dbLocation string, key string, value interface{}) (err error) { 
	c, err := couchbase.Connect(dbLocation)
	if (err != nil) { return }

	pool, err := c.GetPool("default")
	if (err != nil) { return }

	bucket, err := pool.GetBucket("SecretThing")
	if (err != nil) { return }
	defer bucket.Close()

	added, err := bucket.Add(key, 0, value)
	if (err != nil) { return }

	if !added { 
		err = errors.New("not added")
	}
	return
}