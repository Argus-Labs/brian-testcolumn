package main_test

import (
	"errors"
	"fmt"
	"log"
	"strconv"
	"testing"

	"github.com/kelindar/column"
	"github.com/zeebo/assert"
)

func TestTransactionInTransaction(t *testing.T) {
	players := column.NewCollection()
	err := errors.Join(players.CreateColumn("name", column.ForString()),
		players.CreateColumn("class", column.ForString()),
		players.CreateColumn("balance", column.ForFloat64()),
		players.CreateColumn("age", column.ForInt16()))
	if err != nil {
		log.Fatalf("failed to create columns")
	}

	printRows := func() {
		err = players.Query(func(tx *column.Txn) error {
			names := tx.String("name")
			var i int = 0
			err = tx.WithInt("age", func(v int64) bool {
				return true
			}).Range(func(idx uint32) {
				name, _ := names.Get()
				fmt.Println("names: " + name + strconv.Itoa(i))
				i++
			})
			if err != nil {
				return err
			}
			return nil
		})
	}

	addPlayers := func() {
		err = players.Query(func(tx *column.Txn) error {

			//inner tx
			err = players.Query(func(tx *column.Txn) error {
				for i := 0; i < 20; i++ {
					_, err := tx.Insert(func(r column.Row) error {
						r.SetString("name", "merlin")
						r.SetString("class", "mage")
						r.SetFloat64("balance", 99.95)
						r.SetInt16("age", 107)
						return nil
					})
					if err != nil {
						return err
					}
				}
				return errors.New("FAIL!!!!!!!!!!!!!!!!!!")
			})

			for i := 0; i < 20; i++ {
				_, err := tx.Insert(func(r column.Row) error {
					r.SetString("name", "merlin")
					r.SetString("class", "mage")
					r.SetFloat64("balance", 99.95)
					r.SetInt16("age", 107)
					return nil
				})
				if err != nil {
					return err
				}
			}
			return nil
		})
	}

	addPlayers()
	printRows()

}

func TestTransaction(t *testing.T) {
	players := column.NewCollection()
	err := errors.Join(players.CreateColumn("name", column.ForString()),
		players.CreateColumn("class", column.ForString()),
		players.CreateColumn("balance", column.ForFloat64()),
		players.CreateColumn("age", column.ForInt16()))
	if err != nil {
		log.Fatalf("failed to create columns")
	}
	addPlayers := func() {
		err = players.Query(func(tx *column.Txn) error {
			for i := 0; i < 20; i++ {
				_, err := tx.Insert(func(r column.Row) error {
					r.SetString("name", "merlin")
					r.SetString("class", "mage")
					r.SetFloat64("balance", 99.95)
					r.SetInt16("age", 107)
					return nil
				})
				if err != nil {
					return err
				}
			}
			return nil
		})
	}

	addPlayers()
	assert.Nil(t, err)
	assert.Equal(t, players.Count(), 20)

	addPlayersError := func() error {
		err = players.Query(func(tx *column.Txn) error {
			for i := 0; i < 20; i++ {
				_, err = tx.Insert(func(r column.Row) error {
					r.SetString("name", "merlin")
					r.SetString("class", "mage")
					r.SetFloat64("balance", 99.95)
					r.SetInt16("age", 107)
					return nil
				})
				if err != nil {
					return err
				}
			}
			return errors.New("SHOULD NOT PASS")
		})
		return err
	}

	//printRows := func() {
	//	err = players.Query(func(tx *column.Txn) error {
	//		names := tx.String("name")
	//		var i int = 0
	//		err = tx.WithInt("age", func(v int64) bool {
	//			return true
	//		}).Range(func(idx uint32) {
	//			name, _ := names.Get()
	//			fmt.Println("names: " + name + strconv.Itoa(i))
	//			i++
	//		})
	//		if err != nil {
	//			return err
	//		}
	//		return nil
	//	})
	//}

	err = addPlayersError()
	//printRows() //prints correctly even though count is 40. Uncomment to see.
	assert.Error(t, err)                 //should be error.
	assert.Equal(t, players.Count(), 20) //transaction failed should still be 20.
}
