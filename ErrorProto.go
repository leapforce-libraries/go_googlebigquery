package googlebigquery

type ErrorProto struct {
	Reason    string `json:"reason"`
	Location  string `json:"location"`
	DebugInfo string `json:"debugInfo"`
	Message   string `json:"message"`
}
