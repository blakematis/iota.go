package api

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	. "github.com/iotaledger/iota.go/consts"
	"github.com/iotaledger/iota.go/pow"
	"github.com/pkg/errors"
	"io/ioutil"
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
	PrimaryNode string
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
	primary  Provider
	client   HTTPClient
	settings *QuorumHTTPClientSettings
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

	// initialize the primary client
	httpSettings := HTTPClientSettings{
		URI:                  quSettings.PrimaryNode,
		Client:               quSettings.Client,
		LocalProofOfWorkFunc: quSettings.LocalProofOfWorkFunc,
	}

	httpProvider, err := NewHTTPClient(httpSettings)
	if err != nil {
		return err
	}
	hc.primary = httpProvider

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

// ignore
func (hc *quorumhttpclient) Send(cmd interface{}, out interface{}) error {
	b, err := json.Marshal(cmd)
	if err != nil {
		return err
	}

	// execute non quorum command on the primary node
	command, ok := cmd.(Commander)
	if !ok {
		panic("non Commander interface passed into Send()")
	}
	if _, ok := nonQuorumCommands[command.Cmd()]; ok {
		return hc.primary.Send(cmd, out)
	}

	type quresp struct {
		data   []byte
		status int
	}

	// for any error which occured during sending the request
	// of any node in the quorum
	var anyError error

	// holds the count of same responses gathered
	mu := sync.Mutex{}
	votes := map[string]float64{}
	responses := map[string]quresp{}
	var totalVotes float64
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

			// the total count is only determined by votes
			// which actually fulfilled the request
			totalVotes++

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

			mu.Lock()
			rawHash := sha256.Sum256(bs)
			hash := string(rawHash[:])
			votes[hash]++
			_, ok := responses[hash]
			if !ok {
				responses[hash] = quresp{data: bs, status: resp.StatusCode}
			}
			mu.Unlock()
		}(i)
	}
	wg.Wait()

	// if any error occurred and a node couldn't
	// get a response, we simply return the error
	// instead of forming a quorum.
	// lib users should use good/alive nodes
	if anyError != nil {
		return err
	}

	var highestVotes float64
	var highestKey string
	for key, v := range votes {
		if highestVotes < v {
			highestVotes = v
			highestKey = key
		}
	}

	// check whether quorum is over threshold
	percentage := highestVotes / totalVotes
	if percentage < hc.settings.QuorumThreshold {
		return errors.Wrapf(ErrQuorumNotReached, "%0.2f of needed %0.2f reached", percentage, hc.settings.QuorumThreshold)
	}

	quorumResult := responses[highestKey]
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
