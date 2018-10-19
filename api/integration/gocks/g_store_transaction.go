package gocks

import (
	. "github.com/iotaledger/iota.go/api"
	. "github.com/iotaledger/iota.go/api/integration/samples"
	. "github.com/iotaledger/iota.go/trinary"
	"gopkg.in/h2non/gock.v1"
)

var TrytesToStore = "ZXM999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999XATZTVVXYMRFJC9DGUXMGO9LJBFYDCJGJVFTKDWWLMLZHLJYOXFTBLGJBHQRQYTVCKXOYGTDYBAUDADYS999999999999999999999999999MRIYREZPWRDN999999999999999RMJEHZD99999999999999999999XIWKKPBVNDVCAZHCTCYYABOKZTSQASOKCILTCBAIKZKSJYFGGCXJDENVJXCCCNPNLYF9UTVDLJLCXZ9PDDLYMKXVA9BJEVOGXHJCOIMWOLGRXUVCHYXDXQPCTOMWWHXEZXBCLGBNOWX9FBNZVSHIVSPDYKI9SA99999BDF9XOPXTSQWEEOE99MYCIYVMOYKXK9LXFROQSALINFOCBKHPQJLMMAGUGRIDTQEEGWS9LLYFFRA9999IJHYREZPWRDN999999999999999LPAOSFELE999999999MMMMMMMMMEAAJHCNEUXGKOMYQCJF9MROSOCF"

func init() {
	gock.New(DefaultLocalIRIURI).
		Persist().
		Post("/").
		MatchType("json").
		JSON(StoreTransactionsCommand{
			Command: StoreTransactionsCmd,
			Trytes:  []Trytes{TrytesToStore},
		}).
		Reply(200)

	gock.New(DefaultLocalIRIURI).
		Persist().
		Post("/").
		MatchType("json").
		JSON(StoreTransactionsCommand{
			Command: StoreTransactionsCmd,
			Trytes:  BundleTrytes,
		}).
		Reply(200)
}