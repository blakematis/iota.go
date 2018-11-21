package main

import (
	"fmt"
	"github.com/iotaledger/iota.go/api"
	"github.com/iotaledger/iota.go/pow"
	"github.com/iotaledger/iota.go/x/account"
	"os"
)

func must(err error) {
	if err != nil {
		panic(err)
	}
}

const seed = "XRESYFHDADERN9FJEWHKIQQTIMHFU9UJIYCDCYCIZXCI9GLCLSZKNBPWQEAMOUSZXACZABHNYQDPREPNJ"
const target = "NGWYMGNWZMZOCISYWFTSOYNIQUBNPJKEE9QQLSGHXAZHJK9M9ST9GXMPTOITE9HQBVJDTCNEZMVN9GZFUBVYVZIGGA"

func main() {
	_, proofOfWorkFunc := pow.GetFastestProofOfWorkImpl()
	iotaAPI, err := api.ComposeAPI(api.HTTPClientSettings{
		URI:                  "https://node.iota-tangle.io:14265",
		LocalProofOfWorkFunc: proofOfWorkFunc,
	})
	must(err)

	os.Mkdir("/tmp/acc", os.ModePerm)
	store, err := account.NewBadgerStorage("/tmp/acc")
	must(err)

	acc, err := account.NewAccount(seed, store, iotaAPI)
	must(err)

	bndl, err := acc.Send(target, 0)
	must(err)

	isNew, err := acc.IsNew()
	must(err)

	if isNew {
		fmt.Println("the account is new")
	}

	fmt.Println("tail tx", bndl[0].Hash)
	fmt.Println("bundle hash", bndl[0].Bundle)
}
