package api

import (
	"bytes"
	"encoding/json"
	"github.com/cespare/xxhash"
	. "github.com/iotaledger/iota.go/consts"
	"github.com/iotaledger/iota.go/pow"
	"github.com/pkg/errors"
	"io/ioutil"
	"math/rand"
	"net/http"
	"net/url"
	"sync"
)

type QuorumLevel float64

const (
	QuorumHigh   = 0.95
	QuorumMedium = 0.75
	QuorumLow    = 0.60
)

var ErrInvalidQuorumThreshold = errors.New("quorum threshold is set too low, must be >0.5")
var ErrQuorumNotReached = errors.New("the quorum didn't reach a satisfactory result")

const MinimumQuorumThreshold = 0.5

// NewQuorumHTTPClient creates a new quorum based Http Provider.
func NewQuorumHTTPClient(settings interface{}) (Provider, error) {
	client := &quorumhttpclient{}
	if err := client.SetSettings(settings); err != nil {
		return nil, err
	}
	return client, nil
}

// QuorumHTTPClientSettings defines a set of settings for when constructing a new Http Provider.
type QuorumHTTPClientSettings struct {
	// The threshold/majority percentage which must be reached in the responses
	// to form a quorum.
	QuorumThreshold float64
	// For certain commands for which a quorum doesn't make sense
	// this node will be used. For example GetTransactionsToApprove
	// would always fail when queried via a quorum.
	// If no PrimaryNode is set, then a node is randomly selected
	// for executing calls for which no quorum can be done.
	PrimaryNode *string
	// The nodes to which the client connects to.
	Nodes []string
	// The underlying HTTPClient to use. Defaults to http.DefaultClient.
	Client HTTPClient
	// The Proof-of-Work implementation function. Defaults to use the AttachToTangle IRI API call.
	LocalProofOfWorkFunc pow.ProofOfWorkFunc
}

// ProofOfWorkFunc returns the defined Proof-of-Work function.
func (hcs QuorumHTTPClientSettings) ProofOfWorkFunc() pow.ProofOfWorkFunc {
	return hcs.LocalProofOfWorkFunc
}

// QuorumHTTPClient defines an object being able to do Http calls.
type QuorumHTTPClient interface {
	Do(req *http.Request) (*http.Response, error)
}

type quorumhttpclient struct {
	primary     Provider
	randClients []Provider
	client      HTTPClient
	nodesCount  int
	settings    *QuorumHTTPClientSettings
}

// ignore
func (hc *quorumhttpclient) SetSettings(settings interface{}) error {
	quSettings, ok := settings.(QuorumHTTPClientSettings)
	if !ok {
		return errors.Wrapf(ErrInvalidSettingsType, "expected %T", QuorumHTTPClientSettings{})
	}

	// verify the urls of all given nodes
	for i := range quSettings.Nodes {
		if _, err := url.Parse(quSettings.Nodes[i]); err != nil {
			return errors.Wrap(ErrInvalidURI, quSettings.Nodes[i])
		}
	}

	// set default client
	if quSettings.Client != nil {
		hc.client = quSettings.Client
	} else {
		hc.client = http.DefaultClient
	}

	// verify that the quorum threshold makes sense
	if quSettings.QuorumThreshold != 0 {
		if quSettings.QuorumThreshold <= MinimumQuorumThreshold {
			return ErrInvalidQuorumThreshold
		}
	} else {
		quSettings.QuorumThreshold = QuorumHigh
	}

	// set the primary node of our provider
	if quSettings.PrimaryNode != nil {
		// initialize the primary client
		httpSettings := HTTPClientSettings{
			URI:                  *quSettings.PrimaryNode,
			Client:               quSettings.Client,
			LocalProofOfWorkFunc: quSettings.LocalProofOfWorkFunc,
		}

		httpProvider, err := NewHTTPClient(httpSettings)
		if err != nil {
			return err
		}
		hc.primary = httpProvider
	} else {
		// instantiate a new provider for each single node
		randClients := make([]Provider, len(quSettings.Nodes))
		for i := range quSettings.Nodes {
			httpSettings := HTTPClientSettings{
				URI:                  quSettings.Nodes[i],
				Client:               quSettings.Client,
				LocalProofOfWorkFunc: quSettings.LocalProofOfWorkFunc,
			}
			httpProvider, err := NewHTTPClient(httpSettings)
			if err != nil {
				return err
			}
			randClients[i] = httpProvider
		}
		hc.randClients = randClients
	}
	hc.nodesCount = len(quSettings.Nodes)
	hc.settings = &quSettings
	return nil
}

var nonQuorumCommands = map[IRICommand]struct{}{
	"getNodeInfo":              {},
	"getNeighbors":             {},
	"addNeighbors":             {},
	"removeNeighbors":          {},
	"getTips":                  {},
	"getTransactionsToApprove": {},
	"attachToTangle":           {},
	"interruptAttachToTangle":  {},
	"broadcastTransactions":    {},
	"storeTransactions":        {},
}

type quorumresponse struct {
	data   []byte
	status int
}

// ignore
func (hc *quorumhttpclient) Send(cmd interface{}, out interface{}) error {
	command, ok := cmd.(Commander)
	if !ok {
		panic("non Commander interface passed into Send()")
	}

	// execute non quorum command on the primary or random node
	if _, ok := nonQuorumCommands[command.Cmd()]; ok {
		// randomly pick up as no primary is defined
		if hc.primary == nil {
			provider := hc.randClients[rand.Int()%hc.nodesCount]
			return provider.Send(cmd, out)
		}
		// use primary node
		return hc.primary.Send(cmd, out)
	}

	// serialize
	b, err := json.Marshal(cmd)
	if err != nil {
		return err
	}

	// for any error which occurred during sending the request in the quorum
	var anyError error

	// holds the count of same responses
	mu := sync.Mutex{}
	votes := map[uint64]float64{}
	responses := map[uint64]quorumresponse{}

	wg := sync.WaitGroup{}
	wg.Add(len(hc.settings.Nodes))

	// query each not in parallel
	for i := range hc.settings.Nodes {
		go func(i int) {
			defer wg.Done()
			rd := bytes.NewReader(b)
			req, err := http.NewRequest("POST", hc.settings.Nodes[i], rd)
			if err != nil {
				anyError = err
				return
			}

			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("X-IOTA-API-Version", "1")
			resp, err := hc.client.Do(req)
			if err != nil {
				anyError = err
				return
			}
			defer resp.Body.Close()

			bs, err := ioutil.ReadAll(resp.Body)
			if err != nil {
				anyError = err
				return
			}

			// cut out duration parameter from response
			var indexOfLastField int
			for i := len(bs) - 1; i > 0; i-- {
				// comma
				if bs[i] == 44 {
					indexOfLastField = i
					break
				}
			}

			// only grab part until last field and add closing bracket
			bs = append(bs[:indexOfLastField], 125)
			hash := xxhash.Sum64(bs)

			mu.Lock()
			votes[hash]++
			_, ok := responses[hash]
			if !ok {
				responses[hash] = quorumresponse{data: bs, status: resp.StatusCode}
			}
			mu.Unlock()
		}(i)
	}
	wg.Wait()

	// if any error occurred and a node couldn't get a response,
	// we simply return the error instead of forming a quorum.
	// lib users should use good/alive nodes
	if anyError != nil {
		return err
	}

	var mostVotes float64
	var selected uint64
	for key, v := range votes {
		if mostVotes < v {
			mostVotes = v
			selected = key
		}
	}

	// check whether quorum is over threshold
	percentage := mostVotes / float64(hc.nodesCount)
	if percentage < hc.settings.QuorumThreshold {
		return errors.Wrapf(ErrQuorumNotReached, "%0.2f of needed %0.2f reached", percentage, hc.settings.QuorumThreshold)
	}

	quorumResult := responses[selected]
	resultData := quorumResult.data

	if quorumResult.status != http.StatusOK {
		errResp := &ErrorResponse{}
		err = json.Unmarshal(resultData, errResp)
		return handleError(errResp, err, errors.Wrapf(ErrNonOKStatusCodeFromAPIRequest, "http code %d", quorumResult.status))
	}

	if bytes.Contains(resultData, []byte(`"error"`)) || bytes.Contains(resultData, []byte(`"exception"`)) {
		errResp := &ErrorResponse{}
		err = json.Unmarshal(resultData, errResp)
		return handleError(errResp, err, ErrUnknownErrorFromAPIRequest)
	}

	if out == nil {
		return nil
	}
	return json.Unmarshal(resultData, out)
}
