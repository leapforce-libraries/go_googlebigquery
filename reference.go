package googlebigquery

type DatasetReference struct {
	ProjectID string `json:"projectId"`
	DatasetID string `json:"datasetId"`
}

type TableReference struct {
	ProjectID string `json:"projectId"`
	DatasetID string `json:"datasetId"`
	TableID   string `json:"tableId"`
}

type RoutineReference struct {
	ProjectID string `json:"projectId"`
	DatasetID string `json:"datasetId"`
	RoutineID string `json:"routineId"`
}

type JobReference struct {
	ProjectID string `json:"projectId"`
	JobID     string `json:"jobId"`
	Location  string `json:"location"`
}

type ModelReference struct {
	ProjectID string `json:"projectId"`
	DatasetID string `json:"datasetId"`
	ModelID   string `json:"modelId"`
}
