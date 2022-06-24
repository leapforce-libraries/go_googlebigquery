package googlebigquery

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"

	errortools "github.com/leapforce-libraries/go_errortools"
	go_http "github.com/leapforce-libraries/go_http"
	go_types "github.com/leapforce-libraries/go_types"
)

type DatasetsResponse struct {
	Kind          string    `json:"kind"`
	Etag          string    `json:"etag"`
	NextPageToken *string   `json:"nextPageToken"`
	Datasets      []Dataset `json:"datasets"`
}

type Dataset struct {
	Kind                           string                `json:"kind"`
	Etag                           string                `json:"etag"`
	ID                             string                `json:"id"`
	SelfLink                       string                `json:"selfLink"`
	DatasetReference               DatasetReference      `json:"datasetReference"`
	FriendlyName                   *string               `json:"friendlyName"`
	Description                    *string               `json:"description"`
	DefaultTableExpirationMS       *go_types.Int64String `json:"defaultTableExpirationMs"`
	DefaultPartitionExpirationMS   *go_types.Int64String `json:"defaultPartitionExpirationMs"`
	Labels                         *json.RawMessage      `json:"labels"`
	Access                         *[]DatasetAccess      `json:"access"`
	CreationTime                   go_types.Int64String  `json:"creationTime"`
	LastModifiedTime               go_types.Int64String  `json:"lastModifiedTime"`
	Location                       string                `json:"location"`
	Type                           string                `json:"type"`
	DefaultEncryptionConfiguration *string               `json:"defaultEncryptionConfiguration"`
	SatisfiesPZS                   *bool                 `json:"satisfiesPzs"`
}

type DatasetAccess struct {
	Role         string            `json:"role"`
	UserByEmail  *string           `json:"userByEmail"`
	GroupByEmail *string           `json:"groupByEmail"`
	Domain       *string           `json:"domain"`
	SpecialGroup *string           `json:"specialGroup"`
	IAMMember    *string           `json:"iamMember"`
	View         *TableReference   `json:"view"`
	Routine      *RoutineReference `json:"routine"`
}

type GetDatasetsConfig struct {
	ProjectID  string
	All        *bool
	Filter     *string
	MaxResults *int
	PageToken  *string
}

func (service *Service) GetDatasets(config *GetDatasetsConfig) (*[]Dataset, *errortools.Error) {
	if config == nil {
		return nil, errortools.ErrorMessage("GetDatasetsConfig must not be a nil pointer")
	}

	values := url.Values{}

	if config.All != nil {
		values.Set("all", fmt.Sprintf("%v", *config.All))
	}
	if config.Filter != nil {
		values.Set("filter", *config.Filter)
	}
	if config.MaxResults != nil {
		values.Set("maxResults", fmt.Sprintf("%v", *config.MaxResults))
	}
	pageToken := config.PageToken

	datasets := []Dataset{}

	for {
		if pageToken != nil {
			values.Set("pageToken", *pageToken)
		}

		datasetsReponse := DatasetsResponse{}

		requestConfig := go_http.RequestConfig{
			Method:        http.MethodGet,
			Url:           service.url(fmt.Sprintf("projects/%s/datasets?%s", config.ProjectID, values.Encode())),
			ResponseModel: &datasetsReponse,
		}
		_, _, e := service.googleService.HttpRequest(&requestConfig)
		if e != nil {
			return nil, e
		}

		datasets = append(datasets, datasetsReponse.Datasets...)

		if config.PageToken != nil {
			break
		}
		if datasetsReponse.NextPageToken == nil {
			break
		}

		pageToken = datasetsReponse.NextPageToken
	}

	return &datasets, nil
}

type GetDatasetConfig struct {
	ProjectID string
	DatasetID string
}

func (service *Service) GetDataset(config *GetDatasetConfig) (*Dataset, *errortools.Error) {
	if config == nil {
		return nil, errortools.ErrorMessage("GetDatasetsConfig must not be a nil pointer")
	}

	dataset := Dataset{}

	requestConfig := go_http.RequestConfig{
		Method:        http.MethodGet,
		Url:           service.url(fmt.Sprintf("projects/%s/datasets/%s", config.ProjectID, config.DatasetID)),
		ResponseModel: &dataset,
	}
	_, _, e := service.googleService.HttpRequest(&requestConfig)
	if e != nil {
		return nil, e
	}

	return &dataset, nil
}
