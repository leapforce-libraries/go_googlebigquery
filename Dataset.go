package googlebigquery

import (
	"fmt"
	"net/url"

	errortools "github.com/leapforce-libraries/go_errortools"
	go_http "github.com/leapforce-libraries/go_http"
)

type DatasetsResponse struct {
	Kind          string    `json:"kind"`
	Etag          string    `json:"etag"`
	NextPageToken string    `json:"nextPageToken"`
	Datasets      []Dataset `json:"datasets"`
}

type Dataset struct {
	Kind             string `json:"kind"`
	ID               string `json:"id"`
	DatasetReference struct {
		DatasetID string `json:"datasetId"`
		ProjectID string `json:"projectId"`
	} `json:"datasetReference"`
	Location string `json:"location"`
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

	datasetsReponse := DatasetsResponse{}

	requestConfig := go_http.RequestConfig{
		URL:           service.url(fmt.Sprintf("projects/%s/datasets?%s", config.ProjectID, values.Encode())),
		ResponseModel: &datasetsReponse,
	}
	_, _, e := service.googleService.Get(&requestConfig)
	if e != nil {
		return nil, e
	}

	return &datasetsReponse.Datasets, nil
}
