package etcd

import (
	"encoding/json"
	"log"
	"strconv"
	"time"

	etcd "github.com/coreos/etcd/client"
	"github.com/fanzhangio/cloudruntime/stores"
	"golang.org/x/net/context"
)

type bucket struct {
	prefix string
	s      *Store
}

type bucketEnum struct {
	name   string
	cursor int
	count  int
	store  *Store
	end    bool
}

func (b *bucket) Enumerate(opts stores.EnumOptions) stores.Enumerator {
	return &bucketEnum{
		name:   b.prefix + strconv.Itoa(opts.Partition),
		cursor: 0,
		count:  opts.PageSize,
		store:  b.s,
	}
}

func (e *bucketEnum) Next() ([]stores.Value, error) {
	if e.end {
		return nil, nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	api := etcd.NewKeysAPI(*e.store.Client)
	getOp := etcd.GetOptions{
		Sort:   true,
		Quorum: true,
	}

	resp, err := api.Get(ctx, e.name, &getOp)
	cancel()

	if err != nil {
		log.Fatal(err)
	}

	respNode := resp.Node
	if respNode.Dir {
		nodes := respNode.Nodes
		vals := make([]stores.Value, 0, len(nodes))
		counter := 0

		// capture the slice defined by cursor and pageSize
		for index, node := range nodes {
			encoded, _ := json.Marshal(node.Value)

			if (index >= e.cursor) && (counter < e.count) {
				vals = append(vals, &value{
					data: string(encoded),
					ttl:  stores.NoTTL,
				})
				counter++
			} else if counter >= e.count {
				break
			} else if ((index + 1) == len(nodes)) && (counter <= e.count) {
				e.end = true
			}
		}

		if e.end {
			e.cursor = 0
		}

		if counter < e.count {
			e.cursor = 0
		} else {
			e.cursor = e.cursor + e.count
		}

		return vals, nil
	}

	return nil, nil
}

func (b *bucket) Put(key string, value interface{}, ttl time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	api := etcd.NewKeysAPI(*b.s.Client)

	encoded, err := json.Marshal(value)
	if err != nil {
		log.Fatal(err)
		return err
	}
	key = b.mapKey(key)
	setOp := etcd.SetOptions{
		TTL:     ttl,
		Refresh: true,
	}
	_, err = api.Set(ctx, key, string(encoded), &setOp)
	cancel()

	if err != nil {
		log.Fatal(err)
	}

	return err
}

func (b *bucket) Get(key string) (val stores.Value, err error) {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	api := etcd.NewKeysAPI(*b.s.Client)
	key = b.mapKey(key)
	getOp := etcd.GetOptions{
		Quorum: true,
	}

	resp, err := api.Get(ctx, key, &getOp)
	cancel()
	if err != nil {
		log.Fatal(err)
	}

	node := resp.Node

	if !node.Dir {
		encoded, _ := json.Marshal(node.Value)
		val = &value{
			data: string(encoded),
			ttl:  time.Duration(node.TTL) * time.Second,
		}
	}

	return nil, err
}

func (b *bucket) Expire(key string, ttl time.Duration) (err error) {
	log.Println("Expire method not supported in bucket")
	return nil
}

func (b *bucket) Remove(key string) (val stores.Value, err error) {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	api := etcd.NewKeysAPI(*b.s.Client)
	key = b.mapKey(key)

	delOp := etcd.DeleteOptions{
		PrevValue: "",
	}

	resp, err := api.Delete(ctx, key, &delOp)
	cancel()
	if err != nil {
		log.Fatal(err)
	}

	node := resp.PrevNode

	if !node.Dir {
		encoded, _ := json.Marshal(node.Value)
		val = &value{
			data: string(encoded),
			ttl:  time.Duration(node.TTL) * time.Second, // todo: get ttl of specific key
		}
	}
	return
}

func (b *bucket) mapKey(key string) string {
	return b.prefix + strconv.Itoa(stores.Partition(key)) + "/" + key
}
