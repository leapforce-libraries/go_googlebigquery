package googlebigquery

import (
	"fmt"
	"net/http"
	"net/url"

	errortools "github.com/leapforce-libraries/go_errortools"
	go_http "github.com/leapforce-libraries/go_http"
	go_types "github.com/leapforce-libraries/go_types"
)

type JobsResponse struct {
	Kind          string  `json:"kind"`
	Etag          string  `json:"etag"`
	NextPageToken *string `json:"nextPageToken"`
	Jobs          []Job   `json:"jobs"`
}

type Job struct {
	Kind          string           `json:"kind"`
	Etag          string           `json:"etag"`
	ID            string           `json:"id"`
	SelfLink      string           `json:"selfLink"`
	UserEmail     string           `json:"user_email"`
	Configuration JobConfiguration `json:"configuration"`
	JobReference  JobReference     `json:"jobReference"`
	Statistics    JobStatistics    `json:"statistics"`
	Status        JobStatus        `json:"status"`
}

type JobConfiguration struct {
	JobType      string                     `json:"jobType"`
	Query        *JobConfigurationQuery     `json:"query"`
	Load         *JobConfigurationLoad      `json:"load"`
	Copy         *JobConfigurationTableCopy `json:"copy"`
	Extract      *JobConfigurationExtract   `json:"extract"`
	DryRun       *bool                      `json:"dryRun"`
	JobTimeoutMS *go_types.Int64String      `json:"jobTimeoutMs"`
	Labels       *map[string]string         `json:"labels"`
}

type JobConfigurationQuery struct {
	Query                              string                               `json:"query"`
	DestinationTable                   *TableReference                      `json:"destinationTable"`
	TableDefinitions                   map[string]ExternalDataConfiguration `json:"tableDefinitions"`
	UserDefinedFunctionResources       []UserDefinedFunctionResource        `json:"userDefinedFunctionResources"`
	CreateDisposition                  *string                              `json:"createDisposition"`
	WriteDisposition                   *string                              `json:"writeDisposition"`
	DefaultDataset                     *DatasetReference                    `json:"defaultDataset"`
	Priority                           *string                              `json:"priority"`
	AllowLargeResults                  *bool                                `json:"allowLargeResults"`
	UseQueryCache                      *bool                                `json:"useQueryCache"`
	FlattenResults                     *bool                                `json:"flattenResults"`
	MaximumBillingTier                 *int64                               `json:"maximumBillingTier"`
	MaximumBytesBilled                 *go_types.Int64String                `json:"maximumBytesBilled"`
	UseLegacySQL                       bool                                 `json:"useLegacySql"`
	ParameterMode                      *string                              `json:"parameterMode"`
	QueryParameters                    *[]QueryParameter                    `json:"queryParameters"`
	SchemaUpdateOptions                []string                             `json:"schemaUpdateOptions"`
	TimePartitioning                   *TimePartitioning                    `json:"timePartitioning"`
	RangePartitioning                  *RangePartitioning                   `json:"rangePartitioning"`
	Clustering                         *Clustering                          `json:"clustering"`
	DestinationEncryptionConfiguration *EncryptionConfiguration             `json:"destinationEncryptionConfiguration"`
	ScriptOptions                      *ScriptOptions                       `json:"scriptOptions"`
	ConnectionProperties               *[]ConnectionProperty                `json:"connectionProperties"`
}

type QueryParameter struct {
	Name           *string             `json:"name"`
	ParameterType  QueryParameterType  `json:"parameterType"`
	ParameterValue QueryParameterValue `json:"parameterValue"`
}

type QueryParameterType struct {
	Type        string              `json:"type"`
	ArrayType   *QueryParameterType `json:"arrayType"`
	StructTypes *[]struct {
		Name        *string            `json:"name"`
		Type        QueryParameterType `json:"type"`
		Description *string            `json:"description"`
	} `json:"structTypes"`
}

type QueryParameterValue struct {
	Value        *string                         `json:"value"`
	ArrayValues  *[]QueryParameterValue          `json:"arrayValues"`
	StructValues *map[string]QueryParameterValue `json:"structValues"`
}

type ScriptOptions struct {
	StatementTimeoutMS  *string `json:"statementTimeoutMs"`
	StatementByteBudget *string `json:"statementByteBudget"`
	KeyResultStatement  *string `json:"keyResultStatement"`
}

type ConnectionProperty struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type JobConfigurationLoad struct {
	SourceURIs                         []string                    `json:"sourceUris"`
	Schema                             *TableSchema                `json:"schema"`
	DestinationTable                   TableReference              `json:"destinationTable"`
	DestinationTableProperties         *DestinationTableProperties `json:"destinationTableProperties"`
	CreateDisposition                  *string                     `json:"createDisposition"`
	WriteDisposition                   *string                     `json:"writeDisposition"`
	NullMarker                         *string                     `json:"nullMarker"`
	FieldDelimiter                     *string                     `json:"fieldDelimiter"`
	SkipLeadingRows                    *int64                      `json:"skipLeadingRows"`
	Encoding                           *string                     `json:"encoding"`
	Quote                              *string                     `json:"quote"`
	MaxBadRecords                      *int64                      `json:"maxBadRecords"`
	AllowQuotedNewlines                bool                        `json:"allowQuotedNewlines"`
	SourceFormat                       *string                     `json:"sourceFormat"`
	AllowJaggedRows                    *bool                       `json:"allowJaggedRows"`
	IgnoreUnknownValues                *bool                       `json:"ignoreUnknownValues"`
	ProjectionFields                   *[]string                   `json:"projectionFields"`
	Autodetect                         *bool                       `json:"autodetect"`
	SchemaUpdateOptions                *[]string                   `json:"schemaUpdateOptions"`
	TimePartitioning                   *TimePartitioning           `json:"timePartitioning"`
	RangePartitioning                  *RangePartitioning          `json:"rangePartitioning"`
	Clustering                         *Clustering                 `json:"clustering"`
	DestinationEncryptionConfiguration *EncryptionConfiguration    `json:"destinationEncryptionConfiguration"`
	UseAvroLogicalTypes                *bool                       `json:"useAvroLogicalTypes"`
	HivePartitioningOptions            *HivePartitioningOptions    `json:"hivePartitioningOptions"`
	DecimalTargetTypes                 *[]string                   `json:"decimalTargetTypes"`
	ParquetOptions                     *ParquetOptions             `json:"parquetOptions"`
}

type DestinationTableProperties struct {
	FriendlyName *string            `json:"friendlyName"`
	Description  *string            `json:"description"`
	Labels       *map[string]string `json:"labels"`
}

type JobConfigurationTableCopy struct {
	SourceTable                        TableReference           `json:"sourceTable"`
	SourceTables                       []TableReference         `json:"sourceTables"`
	DestinationTable                   TableReference           `json:"destinationTable"`
	CreateDisposition                  *string                  `json:"createDisposition"`
	WriteDisposition                   *string                  `json:"writeDisposition"`
	DestinationEncryptionConfiguration *EncryptionConfiguration `json:"destinationEncryptionConfiguration"`
	OperationType                      *string                  `json:"operationType"`
	DestinationExpirationTime          *go_types.Int64String    `json:"destinationExpirationTime"`
}

type JobConfigurationExtract struct {
	DestinationURIs     []string        `json:"destinationUris"`
	PrintHeader         *bool           `json:"printHeader"`
	FieldDelimiter      *string         `json:"fieldDelimiter"`
	DestinationFormat   *string         `json:"destinationFormat"`
	Compression         *string         `json:"compression"`
	UseAvroLogicalTypes *bool           `json:"useAvroLogicalTypes"`
	SourceTable         *TableReference `json:"sourceTable"`
	SourceModel         *ModelReference `json:"sourceModel"`
}

type JobStatistics struct {
	CreationTime        go_types.Int64String  `json:"creationTime"`
	StartTime           *go_types.Int64String `json:"startTime"`
	EndTime             *go_types.Int64String `json:"endTime"`
	TotalBytesProcessed *go_types.Int64String `json:"totalBytesProcessed"`
	CompletionRatio     *float64              `json:"completionRatio"`
	QuotaDeferments     *[]string             `json:"quotaDeferments"`
	//Query                      *JobStatistics2             `json:"query"`
	//Load                       *JobStatistics3             `json:"load"`
	//Extract                    *JobStatistics4             `json:"extract"`
	TotalSlotMS *go_types.Int64String `json:"totalSlotMs"`
	//ReservationUsage           *[]ReservationUsage         `json:"reservationUsage"`
	ReservationID *string               `json:"reservation_id"`
	NumChildJobs  *go_types.Int64String `json:"numChildJobs"`
	ParentJobID   *string               `json:"parentJobId"`
	//ScriptStatistics           *ScriptStatistics           `json:"scriptStatistics"`
	//RowLevelSecurityStatistics *RowLevelSecurityStatistics `json:"rowLevelSecurityStatistics"`
	//TransactionInfo            *TransactionInfo            `json:"transactionInfo"`
}

type JobStatus struct {
	ErrorResult *ErrorProto   `json:"errorResult"`
	Errors      *[]ErrorProto `json:"errors"`
	State       string        `json:"state"`
}

type GetJobsConfig struct {
	ProjectID       string
	AllUsers        *bool
	MaxResults      *int
	MinCreationTime *int64
	MaxCreationTime *int64
	PageToken       *string
	Projection      *JobProjection
	StateFilter     *[]JobState
	ParentJobID     *string
}

type JobProjection string

const (
	JobProjectionFull    JobProjection = "FULL"
	JobProjectionMinimal JobProjection = "MINIMAL"
)

type JobState string

const (
	JobStateDone    JobState = "DONE"
	JobStatePending JobState = "PENDING"
	JobStateRunning JobState = "RUNNING"
)

func (service *Service) GetJobs(config *GetJobsConfig) (*[]Job, *errortools.Error) {
	if config == nil {
		return nil, errortools.ErrorMessage("GetJobsConfig must not be a nil pointer")
	}

	values := url.Values{}

	if config.AllUsers != nil {
		values.Set("allUsers", fmt.Sprintf("%v", *config.AllUsers))
	}
	if config.MaxResults != nil {
		values.Set("maxResults", fmt.Sprintf("%v", *config.MaxResults))
	}
	if config.MinCreationTime != nil {
		values.Set("minCreationTime", fmt.Sprintf("%v", *config.MinCreationTime))
	}
	if config.MaxCreationTime != nil {
		values.Set("maxCreationTime", fmt.Sprintf("%v", *config.MaxCreationTime))
	}
	if config.Projection != nil {
		values.Set("projection", string(*config.Projection))
	}
	if config.StateFilter != nil {
		for _, stateFilter := range *config.StateFilter {
			values.Set("stateFilter", string(stateFilter))
		}
	}
	pageToken := config.PageToken

	jobs := []Job{}

	for {
		if pageToken != nil {
			values.Set("pageToken", *pageToken)
		}

		jobsReponse := JobsResponse{}

		requestConfig := go_http.RequestConfig{
			Method:        http.MethodGet,
			Url:           service.url(fmt.Sprintf("projects/%s/jobs?%s", config.ProjectID, values.Encode())),
			ResponseModel: &jobsReponse,
		}
		_, _, e := service.googleService.HttpRequest(&requestConfig)
		if e != nil {
			return nil, e
		}

		jobs = append(jobs, jobsReponse.Jobs...)

		if config.PageToken != nil {
			break
		}
		if jobsReponse.NextPageToken == nil {
			break
		}

		pageToken = jobsReponse.NextPageToken
	}

	return &jobs, nil
}

type GetJobConfig struct {
	ProjectID string
	JobID     string
}

func (service *Service) GetJob(config *GetJobConfig) (*Job, *errortools.Error) {
	if config == nil {
		return nil, errortools.ErrorMessage("GetJobsConfig must not be a nil pointer")
	}

	job := Job{}

	requestConfig := go_http.RequestConfig{
		Method:        http.MethodGet,
		Url:           service.url(fmt.Sprintf("projects/%s/jobs/%s", config.ProjectID, config.JobID)),
		ResponseModel: &job,
	}
	_, _, e := service.googleService.HttpRequest(&requestConfig)
	if e != nil {
		return nil, e
	}

	return &job, nil
}
