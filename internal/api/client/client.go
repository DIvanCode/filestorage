package client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"github.com/DIvanCode/filestorage/internal/api"
	"github.com/DIvanCode/filestorage/internal/api/tarstream"
	"github.com/DIvanCode/filestorage/pkg/artifact/id"
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

func (c *Client) DownloadArtifact(ctx context.Context, artifactID id.ID, path string) error {
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

func (c *Client) DownloadFile(ctx context.Context, artifactID id.ID, path, file string) error {
	req := api.DownloadFileRequest{File: file}
	jsonReq, err := json.Marshal(req)
	if err != nil {
		return err
	}
	httpReq, err := http.NewRequestWithContext(
		ctx,
		http.MethodGet,
		c.endpoint+"/artifact-file?id="+artifactID.String(),
		bytes.NewBuffer(jsonReq))
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
