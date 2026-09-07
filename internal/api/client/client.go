package client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"

	"github.com/DIvanCode/filestorage/internal/api"
	"github.com/DIvanCode/filestorage/internal/lib/tarstream"
	"github.com/DIvanCode/filestorage/pkg/bucket"
)

type Client struct {
	endpoint        string
	internalAuthKey string
}

func NewClient(endpoint, internalAuthKey string) *Client {
	return &Client{
		endpoint:        endpoint,
		internalAuthKey: internalAuthKey,
	}
}

func (c *Client) DownloadBucket(ctx context.Context, id bucket.ID, path string) error {
	httpReq, err := http.NewRequestWithContext(
		ctx,
		http.MethodGet,
		c.endpoint+"/bucket?id="+id.String(),
		bytes.NewBuffer(nil))
	if err != nil {
		return err
	}

	httpReq.Header.Set("internal-auth", c.internalAuthKey)
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

func (c *Client) DownloadFile(ctx context.Context, bucketID bucket.ID, file, path string) error {
	req := api.DownloadFileRequest{File: file}
	jsonReq, err := json.Marshal(req)
	if err != nil {
		return err
	}
	httpReq, err := http.NewRequestWithContext(
		ctx,
		http.MethodGet,
		c.endpoint+"/file?bucket-id="+bucketID.String(),
		bytes.NewBuffer(jsonReq))
	if err != nil {
		return err
	}

	httpReq.Header.Set("internal-auth", c.internalAuthKey)
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
