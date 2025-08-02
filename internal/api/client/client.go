package client

import (
	"bytes"
	"context"
	"errors"
	"github.com/DIvanCode/filestorage/internal/api/tarstream"
	"github.com/DIvanCode/filestorage/pkg/artifact"
	"io"
	"net/http"
)

type Client struct {
	endpoint string
}

func NewClient(endpoint string) *Client {
	return &Client{
		endpoint: endpoint,
	}
}

func (c *Client) Download(
	ctx context.Context,
	artifactID artifact.ID,
	path string,
) error {
	httpReq, err := http.NewRequestWithContext(
		ctx,
		http.MethodGet,
		c.endpoint+"/artifact?id="+artifactID.String(),
		bytes.NewBuffer(nil))
	if err != nil {
		return err
	}

	httpClient := http.Client{}
	httpResp, err := httpClient.Do(httpReq)
	if err != nil {
		return err
	}
	defer func() { _ = httpResp.Body.Close() }()

	if httpResp.StatusCode != http.StatusOK {
		content, err := io.ReadAll(httpResp.Body)
		if err != nil {
			return err
		}
		return errors.New(string(content))
	}

	return tarstream.Receive(path, httpResp.Body)
}
