package googlebigquery

import (
	"encoding/json"
	"fmt"
	"net/url"
	"time"

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
	Labels       *json.RawMessage           `json:"labels"`
}

type JobConfigurationQuery struct {
	Query string `json:"query"`
	//...
}

type JobConfigurationLoad struct {
	SourceURIs []string `json:"sourceUris"`
	//...
}

type JobConfigurationTableCopy struct {
	SourceTable TableReference `json:"sourceTable"`
	//...
}

type JobConfigurationExtract struct {
	DestinationURI string `json:"destinationUri"`
	//...
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
	MinCreationTime *time.Time
	MaxCreationTime *time.Time
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
		values.Set("minCreationTime", fmt.Sprintf("%v", config.MinCreationTime.Unix()*1000))
	}
	if config.MaxCreationTime != nil {
		values.Set("maxCreationTime", fmt.Sprintf("%v", config.MaxCreationTime.Unix()*1000))
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
			URL:           service.url(fmt.Sprintf("projects/%s/jobs?%s", config.ProjectID, values.Encode())),
			ResponseModel: &jobsReponse,
		}
		_, _, e := service.googleService.Get(&requestConfig)
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
		URL:           service.url(fmt.Sprintf("projects/%s/jobs/%s", config.ProjectID, config.JobID)),
		ResponseModel: &job,
	}
	_, _, e := service.googleService.Get(&requestConfig)
	if e != nil {
		return nil, e
	}

	return &job, nil
}
