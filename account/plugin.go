package account

// Plugin is a component which extends the account's functionality.
type Plugin interface {
	// Starts the given component and passes in the account object.
	Start(acc Account) error
	// Shutdown instructs the component to terminate.
	Shutdown() error
}
