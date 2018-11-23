package account_test

import (
	"github.com/iotaledger/iota.go/experimental/account"
	"github.com/iotaledger/iota.go/transaction"
	"github.com/iotaledger/iota.go/trinary"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"os"
)

const badgerDBDir = "./account_badger_test"

var _ = Describe("BadgerDB", func() {

	removeBadgerDir := func() {
		if _, err := os.Stat(badgerDBDir); err == nil {
			if err := os.RemoveAll(badgerDBDir); err != nil {
				panic(err)
			}
		}
	}

	var zeroValBundleTrytes = []trinary.Trytes{
		"VDD999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999FNXCXZCECVIXHIBZKIFOPPWRKBC9BSN9B9QOTREKIJSBVSBLSETYPQOQSGTVWDDKHIWITNFUDWFFXUXTA999999999999999999999999999GHY9QLLL9XNY999999999999999ZI9FN9D99999999999999999999FPBVYDWIHZHJLBRQVZBTWEXIOBGTDYIUPQPBI9HIVVWGIDADHGFQOO9OPWYJVXJBIDHIHIPOCKHUQUCF9RDGYDVOGMSDQCBXLTLONMBVRLATCCLKPCCQRVFGPTVVRJPAITAKTFS9MLUKPPDCGJSPROOAYXKPO99999CEXUIXWFMUDXDNVGWIPCEDQD99LDAYNNYVUXLEZECXLBPLYAIKGWLCYEYPAXGKEW9REOOVFHEB9PA9999SHY9QLLL9XNY999999999999999ROUSUMMLE999999999MMMMMMMMMXRGQHAZWFAEVOAZ9VGEDQHRNZMC",
	}
	tx, err := transaction.AsTransactionObject(zeroValBundleTrytes[0])
	if err != nil {
		panic(err)
	}
	zeroValBundleHash := "FPBVYDWIHZHJLBRQVZBTWEXIOBGTDYIUPQPBI9HIVVWGIDADHGFQOO9OPWYJVXJBIDHIHIPOCKHUQUCF9"

	removeBadgerDir()
	defer removeBadgerDir()

	if err := os.Mkdir("./account_badger_test", os.ModePerm); err != nil {
		panic(err)
	}

	store, err := account.NewBadgerStore(badgerDBDir)
	if err != nil {
		panic(err)
	}

	var state *account.AccountState
	It("loads correctly an empty account", func() {
		var err error
		state, err = store.LoadAccount(id)
		Expect(err).ToNot(HaveOccurred())
		Expect(state.IsNew()).To(BeTrue())
	})

	Context("addresses", func() {
		It("marks addresses", func() {
			By("deposit", func() {
				err := store.MarkDepositAddresses(id, 1)
				Expect(err).ToNot(HaveOccurred())
				state, err = store.LoadAccount(id)
				Expect(err).ToNot(HaveOccurred())
				Expect(state.UsedAddresses).To(Equal([]int64{-1}))
			})

			By("spent", func() {
				err := store.MarkSpentAddresses(id, 1)
				Expect(err).ToNot(HaveOccurred())
				state, err = store.LoadAccount(id)
				Expect(err).ToNot(HaveOccurred())
				Expect(state.UsedAddresses).To(Equal([]int64{1}))
			})
		})
	})

	Context("AddPendingTransfer()", func() {
		It("adds the pending transfer to the store", func() {
			err := store.AddPendingTransfer(id, zeroValBundleTrytes)
			Expect(err).ToNot(HaveOccurred())
		})
	})

	Context("GetPendingTransfer()", func() {
		It("returns the pending transfer given the bundle hash", func() {
			bndl, err := store.GetPendingTransfer(id, zeroValBundleHash)
			Expect(err).ToNot(HaveOccurred())
			Expect(bndl[0].Address).To(Equal(tx.Address))
		})
	})

	Context("GetPendingTransfers()", func() {
		It("returns all pending transfers", func() {
			bndls, err := store.GetPendingTransfers(id)
			Expect(err).ToNot(HaveOccurred())
			Expect(bndls[0][0].Address).To(Equal(tx.Address))
		})
	})

	Context("AddTailHash()", func() {
		It("adds the given tail hash", func() {
			err := store.AddTailHash(id, zeroValBundleHash, tx.Hash)
			Expect(err).ToNot(HaveOccurred())
			state, err = store.LoadAccount(id)
			Expect(err).ToNot(HaveOccurred())
			Expect(state.PendingTransfers[0].Tails[0]).To(Equal(tx.Hash))
		})
	})

	Context("RemovePendingTransfer()", func() {
		It("returns the given transfer", func() {
			err := store.RemovePendingTransfer(id, zeroValBundleHash)
			Expect(err).ToNot(HaveOccurred())
			bndls, err := store.GetPendingTransfers(id)
			Expect(err).ToNot(HaveOccurred())
			Expect(len(bndls)).To(Equal(0))
		})
	})
})