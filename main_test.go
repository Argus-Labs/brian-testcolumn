package main_test

import (
	"errors"
	"log"
	"testing"

	"github.com/kelindar/column"
	"github.com/zeebo/assert"
)

func TestTransaction(t *testing.T) {
	players := column.NewCollection()
	err := errors.Join(players.CreateColumn("name", column.ForString()),
		players.CreateColumn("class", column.ForString()),
		players.CreateColumn("balance", column.ForFloat64()),
		players.CreateColumn("age", column.ForInt16()))
	if err != nil {
		log.Fatalf("failed to create columns")
	}

	err = players.Query(func(tx *column.Txn) error {
		for i := 0; i < 20; i++ {
			tx.Insert(func(r column.Row) error {
				r.SetString("name", "merlin")
				r.SetString("class", "mage")
				r.SetFloat64("balance", 99.95)
				r.SetInt16("age", 107)
				return nil
			})
		}
		return nil
	})
	assert.Nil(t, err)
	assert.Equal(t, players.Count(), 20)

	err = players.Query(func(tx *column.Txn) error {
		for i := 0; i < 20; i++ {
			tx.Insert(func(r column.Row) error {
				r.SetString("name", "merlin")
				r.SetString("class", "mage")
				r.SetFloat64("balance", 99.95)
				r.SetInt16("age", 107)
				return nil
			})
		}
		return errors.New("SHOULD NOT PASS")
	})

	assert.Error(t, err)                 //should be error.
	assert.Equal(t, players.Count(), 20) //transaction failed should still be 20.
}
