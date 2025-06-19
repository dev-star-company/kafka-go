package topics

type Topic string

const (
	SyncUsers     Topic = "sync.users"
	SyncPhones    Topic = "sync.phones"
	SyncEmails    Topic = "sync.emails"
	SyncAddresses Topic = "sync.addresses"
	SyncRoles     Topic = "sync.roles"
)
