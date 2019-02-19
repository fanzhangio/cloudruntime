package etcd

import (
	"encoding/json"
	"log"
	"time"

	etcd "github.com/coreos/etcd/client"
	"github.com/fanzhangio/cloudruntime/stores"
	"golang.org/x/net/context"
)

type orderedList struct {
	name string
	s    *Store
}

type orderedListEnum struct {
	name   string
	cursor int
	count  int
	end    bool
	store  *Store
}

func (l *orderedList) Enumerate(opts stores.EnumOptions) stores.Enumerator {
	return &orderedListEnum{
		name:   l.name,
		cursor: 0,
		count:  opts.PageSize,
		store:  l.s,
	}
}

func (e *orderedListEnum) Next() ([]stores.Value, error) {
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

func (l *orderedList) Set(id string, exist bool) (err error) {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	api := etcd.NewKeysAPI(*l.s.Client)
	if exist {
		// add new value to list
		createOp := etcd.CreateInOrderOptions{
			TTL: time.Duration(-1),
		}
		_, err = api.CreateInOrder(ctx, l.name, id, &createOp)
		cancel()
	} else {
		getOp := etcd.GetOptions{
			Sort:   true,
			Quorum: true,
		}
		resp, err := api.Get(ctx, l.name, &getOp)
		cancel()
		if err != nil {
			log.Fatal(err)
		}

		node := resp.Node
		if node.Dir {
			nodes := node.Nodes
			for _, node = range nodes {
				if node.Value == id {
					break
				}
			}
		} else {
			// something goes wrong here
		}

		delOp := etcd.DeleteOptions{
			Dir: false,
		}
		_, err = api.Delete(ctx, node.Key, &delOp)
		cancel()
	}
	if err != nil {
		log.Fatal(err)
	}

	return err
}

func (l *orderedList) Has(id string) (bool, error) {
	api := etcd.NewKeysAPI(*l.s.Client)
	getOp := etcd.GetOptions{
		Sort:   true,
		Quorum: true,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	resp, err := api.Get(ctx, l.name, &getOp)
	cancel()
	if err != nil {
		log.Fatal(err)
	}

	node := resp.Node
	if node.Dir {
		nodes := node.Nodes
		for _, node = range nodes {
			if node.Value == id {
				return true, nil
			}
		}
	} else {
		return false, nil
	}

	return false, err
}
