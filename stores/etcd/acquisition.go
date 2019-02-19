package etcd

import (
	"log"
	"time"

	etcd "github.com/coreos/etcd/client"
	"golang.org/x/net/context"
)

type acquisition struct {
	name    string
	owner   string
	ownedBy string
	ttl     time.Duration
	s       *Store
}

func (a *acquisition) Acquired() bool {
	return a.owner == a.ownedBy
}

func (a *acquisition) Owner() string {
	return a.ownedBy
}

func (a *acquisition) TTL() time.Duration {
	return a.ttl
}

func (a *acquisition) Refresh(ttl time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	api := etcd.NewKeysAPI(*a.s.Client)

	key := a.key()
	setOp := etcd.SetOptions{
		PrevValue: a.owner,
		TTL:       ttl,
		Refresh:   true,
	}

	_, err := api.Set(ctx, key, a.owner, &setOp)
	cancel()

	if err != nil {
		log.Fatal(err)
	}

	return err
}

func (a *acquisition) Release() error {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	api := etcd.NewKeysAPI(*a.s.Client)
	key := a.key()

	delOp := etcd.DeleteOptions{
		PrevValue: a.owner,
	}

	_, err := api.Delete(ctx, key, &delOp)
	cancel()

	if err != nil {
		log.Fatal(err)
	}

	return err
}

func (a *acquisition) tryAcquire() error {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	api := etcd.NewKeysAPI(*a.s.Client)
	key := a.key()

	setOp := etcd.SetOptions{
		PrevExist: "PrevNoExist",
	}

	resp, err := api.Set(ctx, key, a.owner, &setOp)
	cancel()

	if err != nil {
		log.Fatal(err)
	}

	respNode := resp.Node
	if !respNode.Dir {
		a.ownedBy = respNode.Value
		a.ttl = time.Duration(respNode.TTL) * time.Second
	}

	return err
}

func (a *acquisition) key() string {
	return a.name + "/" + a.name
}
