package api_test

import (
	"fmt"
	. "github.com/iotaledger/iota.go/api"
	"github.com/iotaledger/iota.go/trinary"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/pkg/errors"
	"gopkg.in/h2non/gock.v1"
)

type fakereqres struct {
	Command
	Val int `json:"val"`
}

var _ = Describe("Quorum", func() {

	const nodesCount = 4
	nodes := make([]string, nodesCount)
	for i := 0; i < nodesCount; i++ {
		nodes[i] = fmt.Sprintf("http:/%d", i)
	}

	Context("voting()", func() {
		It("throws an error when quorum couldn't be reached 0%", func() {
			provider, _ := NewQuorumHTTPClient(QuorumHTTPClientSettings{
				Nodes:     nodes,
				Threshold: 1,
			})
			defer gock.Flush()
			// every node gives a different answer
			for i, node := range nodes {
				gock.New(node).
					Post("/").
					MatchType("json").
					JSON(fakereqres{Val: 0}).
					Reply(200).
					JSON(fakereqres{Val: i})
			}
			err := provider.Send(&fakereqres{Val: 0}, nil)
			Expect(errors.Cause(err)).To(Equal(ErrQuorumNotReached))
		})

		It("throws an error when quorum couldn't be reached 50%", func() {
			provider, _ := NewQuorumHTTPClient(QuorumHTTPClientSettings{
				Nodes:     nodes,
				Threshold: 1,
			})
			defer gock.Flush()
			// 50%
			for i, node := range nodes {
				var res int
				if i%2 == 0 {
					res = 1
				} else {
					res = 0
				}
				gock.New(node).
					Post("/").
					MatchType("json").
					JSON(fakereqres{Val: 0}).
					Reply(200).
					JSON(fakereqres{Val: res})
			}

			err := provider.Send(&fakereqres{Val: 0}, nil)
			Expect(errors.Cause(err)).To(Equal(ErrQuorumNotReached))
		})

		It("returns the optional defined value when quorum couldn't be reached", func() {
			defVal := true
			provider, _ := NewQuorumHTTPClient(QuorumHTTPClientSettings{
				Nodes:     nodes[:2],
				Threshold: 1,
				Defaults: &QuorumDefaults{
					WereAddressesSpentFrom: &defVal,
				},
			})

			req := &WereAddressesSpentFromCommand{
				Addresses: trinary.Hashes{"bla", "alb", "lab"},
				Command:   Command{WereAddressesSpentFromCmd},
			}
			defer gock.Flush()
			// 50%, 100% not reached
			for i := 0; i < 2; i++ {
				var answer bool
				if i == 1 {
					answer = true
				}
				gock.New(nodes[i]).
					Post("/").
					MatchType("json").
					JSON(req).
					Reply(200).
					JSON(WereAddressesSpentFromResponse{States: []bool{answer}})
			}
			res := &WereAddressesSpentFromResponse{}
			err := provider.Send(req, res)
			Expect(err).ToNot(HaveOccurred())
			Expect(res.States[0]).To(Equal(true))
		})

		It("returns the response when a quorum was reached", func() {
			provider, _ := NewQuorumHTTPClient(QuorumHTTPClientSettings{
				Nodes:     nodes,
				Threshold: 1,
			})
			const resVal = 1
			defer gock.Flush()
			for _, node := range nodes {
				gock.New(node).
					Post("/").
					MatchType("json").
					JSON(fakereqres{Val: 0}).
					Reply(200).
					JSON(fakereqres{Val: resVal})
			}
			res := &fakereqres{}
			err := provider.Send(&fakereqres{Val: 0}, res)
			Expect(err).ToNot(HaveOccurred())
			Expect(res.Val).To(Equal(resVal))
		})

		It("returns the response when a quorum was reached (at threshold 75%)", func() {
			provider, _ := NewQuorumHTTPClient(QuorumHTTPClientSettings{
				Nodes:     nodes,
				Threshold: 0.75,
			})
			defer gock.Flush()
			// one node gives another answer
			for i, node := range nodes {
				var resVal int
				if i == len(nodes)-1 {
					resVal = 1
				}
				gock.New(node).
					Post("/").
					MatchType("json").
					JSON(fakereqres{Val: 0}).
					Reply(200).
					JSON(fakereqres{Val: resVal})
			}
			res := &fakereqres{}
			err := provider.Send(&fakereqres{Val: 0}, res)
			Expect(err).ToNot(HaveOccurred())
			Expect(res.Val).To(Equal(0))
		})

		It("returns the error response when the quorum forms it", func() {
			provider, _ := NewQuorumHTTPClient(QuorumHTTPClientSettings{
				Nodes:     nodes,
				Threshold: 1,
			})
			type errorresp struct {
				Error string `json:"error"`
			}
			const errorMsg = "Command [getBanana] is unknown"
			const resVal = 1
			defer gock.Flush()
			for _, node := range nodes {
				gock.New(node).
					Post("/").
					MatchType("json").
					JSON(fakereqres{Val: 0}).
					Reply(400).
					JSON(errorresp{errorMsg})
			}
			res := &fakereqres{}
			err := provider.Send(&fakereqres{Val: 0}, res)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(Equal(errorMsg))
		})

	})

})
