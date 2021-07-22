package googlebigquery

import (
	"encoding/json"
	"fmt"
	"net/url"

	errortools "github.com/leapforce-libraries/go_errortools"
	go_http "github.com/leapforce-libraries/go_http"
	go_types "github.com/leapforce-libraries/go_types"
)

type TablesResponse struct {
	Kind          string  `json:"kind"`
	Etag          string  `json:"etag"`
	NextPageToken *string `json:"nextPageToken"`
	Tables        []Table `json:"tables"`
}

type Table struct {
	Kind                      string                      `json:"kind"`
	Etag                      string                      `json:"etag"`
	ID                        string                      `json:"id"`
	SelfLink                  string                      `json:"selfLink"`
	TableReference            TableReference              `json:"tableReference"`
	FriendlyName              string                      `json:"friendlyName"`
	Description               string                      `json:"description"`
	Labels                    *json.RawMessage            `json:"labels"`
	Schema                    *TableSchema                `json:"schema"`
	TimePartitioning          *TimePartitioning           `json:"timePartitioning"`
	RangePartitioning         *RangePartitioning          `json:"rangePartitioning"`
	Clustering                *Clustering                 `json:"clustering"`
	RequirePartitionFilter    *bool                       `json:"requirePartitionFilter"`
	NumBytes                  *go_types.Int64String       `json:"numBytes"`
	NumLongTermBytes          *go_types.Int64String       `json:"numLongTermBytes"`
	NumRows                   *go_types.Int64String       `json:"numRows"`
	CreationTime              go_types.Int64String        `json:"creationTime"`
	ExpirationTime            *go_types.Int64String       `json:"expirationTime"`
	LastModifiedTime          *go_types.Int64String       `json:"lastModifiedTime"`
	Type                      string                      `json:"type"`
	View                      *ViewDefinition             `json:"view"`
	MaterializedView          *MaterializedViewDefinition `json:"materializedView"`
	ExternalDataConfiguration *ExternalDataConfiguration  `json:"externalDataConfiguration"`
	Location                  string                      `json:"location"`
	StreamingBuffer           *StreamingBuffer            `json:"streamingBuffer"`
	EncryptionConfiguration   *EncryptionConfiguration    `json:"encryptionConfiguration"`
	SnapshotDefinition        *SnapshotDefinition         `json:"snapshotDefinition"`
}

type TableSchema struct {
	Fields []TableFieldSchema `json:"fields"`
}

type TableFieldSchema struct {
	Name        string             `json:"name"`
	Type        string             `json:"type"`
	Mode        string             `json:"mode"`
	Fields      []TableFieldSchema `json:"fields"`
	Description string             `json:"description"`
	PolicyTags  struct {
		Names []string `json:"names"`
	} `json:"policyTags"`
	MaxLength *go_types.Int64String `json:"maxLength"`
	Precision *go_types.Int64String `json:"precision"`
	Scale     *go_types.Int64String `json:"scale"`
}

type TimePartitioning struct {
	Type         string                `json:"type"`
	ExpirationMS *go_types.Int64String `json:"expirationMs"`
	Field        *string               `json:"field"`
}

type RangePartitioning struct {
	Field string `json:"field"`
	Range struct {
		Start    string `json:"start"`
		End      string `json:"end"`
		Interval string `json:"interval"`
	} `json:"range"`
}

type Clustering struct {
	Fields []string `json:"fields"`
}

type ViewDefinition struct {
	Query                        string                         `json:"query"`
	UserDefinedFunctionResources *[]UserDefinedFunctionResource `json:"userDefinedFunctionResources"`
	UseLegacySQL                 *string                        `json:"useLegacySql"`
}

type UserDefinedFunctionResource struct {
	ResourceURI string `json:"resourceUri"`
	InlineCode  string `json:"inlineCode"`
}

type MaterializedViewDefinition struct {
	Query             string                `json:"query"`
	LastRefreshTime   *go_types.Int64String `json:"lastRefreshTime"`
	EnableRefresh     *bool                 `json:"enableRefresh"`
	RefreshIntervalMS *go_types.Int64String `json:"refreshIntervalMs"`
}

type ExternalDataConfiguration struct {
	SourceURIs              []string                 `json:"sourceUris"`
	Schema                  *TableSchema             `json:"schema"`
	SourceFormat            *string                  `json:"sourceFormat"`
	MaxBadRecords           *int64                   `json:"maxBadRecords"`
	Autodetect              *bool                    `json:"autodetect"`
	IgnoreUnknownValues     *bool                    `json:"ignoreUnknownValues"`
	Compression             *string                  `json:"compression"`
	CSVOptions              *CSVOptions              `json:"csvOptions"`
	BigtableOptions         *BigtableOptions         `json:"bigtableOptions"`
	GoogleSheetsOptions     *GoogleSheetsOptions     `json:"googleSheetsOptions"`
	HivePartitioningOptions *HivePartitioningOptions `json:"hivePartitioningOptions"`
	ConnectionID            *string                  `json:"connectionId"`
	DecimalTargetTypes      *[]string                `json:"decimalTargetTypes"`
	ParquetOptions          *ParquetOptions          `json:"parquetOptions"`
}

type CSVOptions struct {
	FieldDelimiter      *string               `json:"fieldDelimiter"`
	SkipLeadingRows     *go_types.Int64String `json:"skipLeadingRows"`
	Quote               *string               `json:"quote"`
	AllowQuotedNewlines *bool                 `json:"allowQuotedNewlines"`
	AllowJaggedRows     *bool                 `json:"allowJaggedRows"`
	Encoding            *string               `json:"encoding"`
}

type BigtableOptions struct {
	ColumnFamilies                  *[]BigtableColumnFamily `json:"columnFamilies"`
	IgnoreUnspecifiedColumnFamilies *bool                   `json:"ignoreUnspecifiedColumnFamilies"`
	ReadRowkeyAsString              *bool                   `json:"readRowkeyAsString"`
}

type BigtableColumnFamily struct {
	FamilyID       string            `json:"familyId"`
	Type           *string           `json:"type"`
	Encoding       *string           `json:"encoding"`
	Columns        *[]BigtableColumn `json:"columns"`
	OnlyReadLatest *bool             `json:"onlyReadLatest"`
}

type BigtableColumn struct {
	QualifierEncoded string  `json:"qualifierEncoded"`
	QualifierString  *string `json:"qualifierString"`
	FieldName        *string `json:"fieldName"`
	Type             *string `json:"type"`
	Encoding         *string `json:"encoding"`
	OnlyReadLatest   *bool   `json:"onlyReadLatest"`
}

type GoogleSheetsOptions struct {
	SkipLeadingRows *go_types.Int64String `json:"skipLeadingRows"`
	Range           *string               `json:"range"`
}

type HivePartitioningOptions struct {
	Mode                   *string   `json:"mode"`
	SourceURIPrefix        *string   `json:"sourceUriPrefix"`
	RequirePartitionFilter *bool     `json:"requirePartitionFilter"`
	Fields                 *[]string `json:"fields"`
}

type ParquetOptions struct {
	EnumAsString        *bool `json:"enumAsString"`
	EnableListInference *bool `json:"enableListInference"`
}

type StreamingBuffer struct {
	EstimatedBytes  *go_types.Int64String `json:"estimatedBytes"`
	EstimatedRows   *go_types.Int64String `json:"estimatedRows"`
	OldestEntryTime *go_types.Int64String `json:"oldestEntryTime"`
}

type EncryptionConfiguration struct {
	KMSKeyName string `json:"kmsKeyName"`
}

type SnapshotDefinition struct {
	BaseTableReference TableReference       `json:"baseTableReference"`
	SnapshotTime       go_types.Int64String `json:"snapshotTime"`
}

type GetTablesConfig struct {
	ProjectID  string
	DatasetID  string
	MaxResults *int
	PageToken  *string
}

func (service *Service) GetTables(config *GetTablesConfig) (*[]Table, *errortools.Error) {
	if config == nil {
		return nil, errortools.ErrorMessage("GetTablesConfig must not be a nil pointer")
	}

	values := url.Values{}

	if config.MaxResults != nil {
		values.Set("maxResults", fmt.Sprintf("%v", *config.MaxResults))
	}
	pageToken := config.PageToken

	tables := []Table{}

	for {
		if pageToken != nil {
			values.Set("pageToken", *pageToken)
		}

		tablesReponse := TablesResponse{}

		requestConfig := go_http.RequestConfig{
			URL:           service.url(fmt.Sprintf("projects/%s/datasets/%s/tables?%s", config.ProjectID, config.DatasetID, values.Encode())),
			ResponseModel: &tablesReponse,
		}
		_, _, e := service.googleService.Get(&requestConfig)
		if e != nil {
			return nil, e
		}

		tables = append(tables, tablesReponse.Tables...)

		if config.PageToken != nil {
			break
		}
		if tablesReponse.NextPageToken == nil {
			break
		}

		pageToken = tablesReponse.NextPageToken
	}

	return &tables, nil
}

type GetTableConfig struct {
	ProjectID string
	DatasetID string
	TableID   string
}

func (service *Service) GetTable(config *GetTableConfig) (*Table, *errortools.Error) {
	if config == nil {
		return nil, errortools.ErrorMessage("GetTablesConfig must not be a nil pointer")
	}

	table := Table{}

	requestConfig := go_http.RequestConfig{
		URL:           service.url(fmt.Sprintf("projects/%s/datasets/%s/tables/%s", config.ProjectID, config.DatasetID, config.TableID)),
		ResponseModel: &table,
	}
	_, _, e := service.googleService.Get(&requestConfig)
	if e != nil {
		return nil, e
	}

	return &table, nil
}
