package account_test

import (
	"github.com/iotaledger/iota.go/experimental/account"
	"github.com/iotaledger/iota.go/transaction"
	"github.com/iotaledger/iota.go/trinary"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"strings"
)

var emptyAddr = strings.Repeat("9", 81)

var _ = Describe("InMemory", func() {

	var zeroValBundleTrytes = []trinary.Trytes{
		"VDD999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999FNXCXZCECVIXHIBZKIFOPPWRKBC9BSN9B9QOTREKIJSBVSBLSETYPQOQSGTVWDDKHIWITNFUDWFFXUXTA999999999999999999999999999GHY9QLLL9XNY999999999999999ZI9FN9D99999999999999999999FPBVYDWIHZHJLBRQVZBTWEXIOBGTDYIUPQPBI9HIVVWGIDADHGFQOO9OPWYJVXJBIDHIHIPOCKHUQUCF9RDGYDVOGMSDQCBXLTLONMBVRLATCCLKPCCQRVFGPTVVRJPAITAKTFS9MLUKPPDCGJSPROOAYXKPO99999CEXUIXWFMUDXDNVGWIPCEDQD99LDAYNNYVUXLEZECXLBPLYAIKGWLCYEYPAXGKEW9REOOVFHEB9PA9999SHY9QLLL9XNY999999999999999ROUSUMMLE999999999MMMMMMMMMXRGQHAZWFAEVOAZ9VGEDQHRNZMC",
	}
	tx, err := transaction.AsTransactionObject(zeroValBundleTrytes[0])
	if err != nil {
		panic(err)
	}
	zeroValBundleHash := "FPBVYDWIHZHJLBRQVZBTWEXIOBGTDYIUPQPBI9HIVVWGIDADHGFQOO9OPWYJVXJBIDHIHIPOCKHUQUCF9"

	store := account.NewInMemoryStore()

	var state *account.AccountState
	It("loads correctly an empty account", func() {
		var err error
		state, err = store.LoadAccount(id)
		Expect(err).ToNot(HaveOccurred())
		Expect(state.IsNew()).To(BeTrue())
	})

	Context("addresses", func() {
		It("marks deposit addresses", func() {
			err := store.MarkDepositAddresses(id, 1)
			Expect(err).ToNot(HaveOccurred())
			state, err = store.LoadAccount(id)
			Expect(err).ToNot(HaveOccurred())
			Expect(state.UsedAddresses).To(Equal([]int64{-1}))
		})
	})

	Context("AddPendingTransfer()", func() {
		It("adds the pending zero value transfer to the store", func() {
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
		It("removes the given transfer", func() {
			err := store.RemovePendingTransfer(id, zeroValBundleHash)
			Expect(err).ToNot(HaveOccurred())
			state, err = store.LoadAccount(id)
			Expect(err).ToNot(HaveOccurred())
			Expect(len(state.PendingTransfers)).To(Equal(0))
			bndls, err := store.GetPendingTransfers(id)
			Expect(err).ToNot(HaveOccurred())
			Expect(len(bndls)).To(Equal(0))
		})
	})
})
