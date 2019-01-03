package api

import (
	"bytes"
	"encoding/json"
	"github.com/cespare/xxhash"
	. "github.com/iotaledger/iota.go/consts"
	"github.com/iotaledger/iota.go/pow"
	"github.com/pkg/errors"
	"io/ioutil"
	"math"
	"math/rand"
	"net/http"
	"net/url"
	"strconv"
	"sync"
)

// QuorumLevel defines the percentage needed for a quorum.
type QuorumLevel float64

// package quorum levels
const (
	QuorumHigh   = 0.95
	QuorumMedium = 0.75
	QuorumLow    = 0.60
)

// errors produced by the quorum http provider
var (
	ErrInvalidQuorumThreshold      = errors.New("quorum threshold is set too low, must be >0.5")
	ErrQuorumNotReached            = errors.New("the quorum didn't reach a satisfactory result")
	ErrExceededNoResponseTolerance = errors.New("exceeded no-response tolerance for quorum")
	ErrNotEnoughNodesForQuorum     = errors.New("at least 2 nodes must be defined for quorum")
	ErrNoLatestSolidSubtangleInfo  = errors.New("no latest solid subtangle info found")
)

// MinimumQuorumThreshold is the minimum threshold the quorum settings
// must have.
const MinimumQuorumThreshold = 0.5

// NewQuorumHTTPClient creates a new quorum based Http Provider.
func NewQuorumHTTPClient(settings interface{}) (Provider, error) {
	client := &quorumhttpclient{}
	if err := client.SetSettings(settings); err != nil {
		return nil, err
	}
	return client, nil
}

// QuorumDefaults defines optional default values when a quorum couldn't be reached.
type QuorumDefaults struct {
	WereAddressesSpentFrom *bool
	GetInclusionStates     *bool
	GetBalances            *uint64
}

// QuorumHTTPClientSettings defines a set of settings for when constructing a new Http Provider.
type QuorumHTTPClientSettings struct {
	// The threshold/majority percentage which must be reached in the responses
	// to form a quorum. Define the threshold as 0<x<1, i.e. 0.8 => 80%.
	// A threshold of 1 would mean that all nodes must give the same response.
	Threshold float64

	// Defines the max percentage of nodes which can fail to give a response
	// when a quorum is built. For example, if 4 nodes are specified and
	// a NoResponseTolerance of 0.25/25% is set, then 1 node of those 4
	// is tolerated to fail to give a response and the quorum is built.
	NoResponseTolerance float64

	// For certain commands for which a quorum doesn't make sense
	// this node will be used. For example GetTransactionsToApprove
	// would always fail when queried via a quorum.
	// If no PrimaryNode is set, then a node is randomly selected from Nodes
	// for executing calls for which no quorum can be done.
	// The primary node is not used for forming the quorum and must be
	// explicitly set in the Nodes field a second time.
	PrimaryNode *string

	// The nodes to which the client connects to.
	Nodes []string

	// The underlying HTTPClient to use. Defaults to http.DefaultClient.
	Client HTTPClient

	// The Proof-of-Work implementation function. Defaults to use the AttachToTangle IRI API call.
	LocalProofOfWorkFunc pow.ProofOfWorkFunc

	// A list of commands which will be executed in quorum even though they are not
	// particularly made for such scenario. Good candidates are 'BroadcastTransactionsCmd'
	// or 'StoreTransactionsCmd'
	ForceQuorumSend map[IRICommand]struct{}

	// Whether to execute BroadcastTransactions() on all nodes
	SpreadBroadcastTransaction bool

	// Default values which are returned when no quorum could be reached
	// for certain types of calls.
	Defaults *QuorumDefaults
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

	if len(quSettings.Nodes) < 2 {
		return ErrNotEnoughNodesForQuorum
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
	if quSettings.Threshold != 0 {
		if quSettings.Threshold <= MinimumQuorumThreshold {
			return ErrInvalidQuorumThreshold
		}
	} else {
		quSettings.Threshold = QuorumHigh
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

var subMileTag = [30]byte{34, 108, 97, 116, 101, 115, 116, 83, 111, 108, 105, 100, 83, 117, 98, 116, 97, 110, 103, 108, 101, 77, 105, 108, 101, 115, 116, 111, 110, 101}
var subMileIndexTag = [34]byte{108, 97, 116, 101, 115, 116, 83, 111, 108, 105, 100, 83, 117, 98, 116, 97, 110, 103, 108, 101, 77, 105, 108, 101, 115, 116, 111, 110, 101, 73, 110, 100, 101, 120}

const commaAsciiVal = 44

func reduceToLatestSolidSubtangleData(data []byte) ([]byte, error) {
	indexOfSubtangleHashKey := bytes.Index(data, subMileTag[:])
	if indexOfSubtangleHashKey == -1 {
		return nil, ErrNoLatestSolidSubtangleInfo
	}
	indexOfSubtangleIndexKey := bytes.Index(data, subMileIndexTag[:])
	if indexOfSubtangleIndexKey == -1 {
		return nil, ErrNoLatestSolidSubtangleInfo
	}
	commaIndex := bytes.Index(data[indexOfSubtangleIndexKey:], []byte{commaAsciiVal})
	if commaIndex == -1 {
		return nil, ErrNoLatestSolidSubtangleInfo
	}
	if indexOfSubtangleIndexKey+commaIndex > len(data) {
		return nil, ErrNoLatestSolidSubtangleInfo
	}
	part := data[indexOfSubtangleHashKey : indexOfSubtangleIndexKey+commaIndex]
	// add opening/closing bracket
	part = append([]byte{123}, part...)
	part = append(part, 125)
	return part, nil
}

// injects the optional default set data into the response
func (hc *quorumhttpclient) injectDefault(cmd interface{}, out interface{}) bool {
	// use defaults for non quorum results
	if hc.settings.Defaults == nil {
		return false
	}
	switch x := cmd.(type) {
	case *WereAddressesSpentFromCommand:
		if hc.settings.Defaults.WereAddressesSpentFrom != nil {
			states := make([]bool, len(x.Addresses))
			for i := range states {
				states[i] = *hc.settings.Defaults.WereAddressesSpentFrom
			}
			out.(*WereAddressesSpentFromResponse).States = states
			return true
		}
	case *GetInclusionStatesCommand:
		if hc.settings.Defaults.GetInclusionStates != nil {
			states := make([]bool, len(x.Transactions))
			for i := range states {
				states[i] = *hc.settings.Defaults.GetInclusionStates
			}
			out.(*GetInclusionStatesResponse).States = states
			return true
		}
	case *GetBalancesCommand:
		if hc.settings.Defaults.GetBalances != nil {
			balances := make([]string, len(x.Addresses))
			for i := range balances {
				balances[i] = strconv.Itoa(int(*hc.settings.Defaults.GetBalances))
			}
			out.(*GetBalancesResponse).Balances = balances
			return true
		}
	}
	return false
}

const closingCurlyBraceAscii = 125

var durationKey = [11]byte{34, 100, 117, 114, 97, 116, 105, 111, 110, 34, 58}

// removes the duration value from the response
// as multiple nodes will always return a different one
func removeDurationField(data []byte) []byte {
	indexOfDurationField := bytes.Index(data, durationKey[:])
	if indexOfDurationField == -1 {
		return data
	}
	curlyBraceIndex := bytes.Index(data[indexOfDurationField:], []byte{closingCurlyBraceAscii})
	return append(data[:indexOfDurationField-1], data[indexOfDurationField+curlyBraceIndex:]...)
}

// ignore
func (hc *quorumhttpclient) Send(cmd interface{}, out interface{}) error {
	comm, ok := cmd.(Commander)
	if !ok {
		panic("non Commander interface passed into Send()")
	}

	// check whether we are specifically asking for the latest solid subtangle
	_, isLatestSolidSubtangleQuery := cmd.(*GetLatestSolidSubtangleMilestoneCommand)

	if !isLatestSolidSubtangleQuery {
		// execute non quorum command on the primary or random node
		command := comm.Cmd()
		_, forced := hc.settings.ForceQuorumSend[command]
		if _, ok := nonQuorumCommands[command]; ok && !forced {
			// randomly pick up as no primary is defined
			if hc.primary == nil {
				provider := hc.randClients[rand.Int()%hc.nodesCount]
				return provider.Send(cmd, out)
			}
			// use primary node
			return hc.primary.Send(cmd, out)
		}
	}

	// serialize
	b, err := json.Marshal(cmd)
	if err != nil {
		return err
	}

	// for any errors which occurred during sending the request
	errMu := sync.Mutex{}
	anyErrors := []error{}

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
			// add the error which occurred during this call
			var anyError error
			defer func() {
				if anyError != nil {
					errMu.Lock()
					anyErrors = append(anyErrors, anyError)
					errMu.Unlock()
				}
			}()

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

			// extract only latest solid subtangle data from get node info
			// call to be able to form a quorum around that response
			if isLatestSolidSubtangleQuery {
				reducedData, err := reduceToLatestSolidSubtangleData(bs)
				if err != nil {
					anyError = err
					return
				}
				bs = reducedData
			}

			// remove the duration field from the response
			// as multiple nodes will always give a different answer
			bs = removeDurationField(bs)

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

	// check how many nodes failed to give a response
	// and then check whether we violated the no-response tolerance
	errorCount := len(anyErrors)
	percOfFailedResp := float64(errorCount) / float64(hc.nodesCount)
	if percOfFailedResp > hc.settings.NoResponseTolerance {
		perc := math.Round(percOfFailedResp * 100)
		return errors.Wrapf(ErrExceededNoResponseTolerance, "%d%% of nodes failed to give a response, first error '%v'", int(perc), anyErrors[0].Error())
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
	percentage := mostVotes / float64(hc.nodesCount-errorCount)
	if percentage < hc.settings.Threshold {
		// automatically inject the default value set by the library user
		// in case no quorum was reached. If no defaults are set, then
		// the default error is returned indicating that no quorum was reached
		if hc.injectDefault(cmd, out) {
			return nil
		}
		return errors.Wrapf(ErrQuorumNotReached, "%0.2f of needed %0.2f reached", percentage, hc.settings.Threshold)
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
