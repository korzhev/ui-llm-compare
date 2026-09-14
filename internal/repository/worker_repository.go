package repository

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/korzhev/ui-llm-compare/internal/config"
	"github.com/korzhev/ui-llm-compare/internal/model"
	"github.com/segmentio/kafka-go"
)

type LLMRepository struct {
	Client      HttpClient
	llmConf     config.LLMConfig
	kafkaConf   config.KafkaConfig
	KafkaReader KafkaReader
}

func (c LLMRepository) FormatLLMRequest(promt, mimeTypeOrigin, mimeType, base64ImgOrigin, base64Img string) ([]byte, error) {
	msg := model.LLMRequest{
		Model: c.llmConf.Model,
		Messages: []model.LLMMsg{{
			Role: "user",
			Content: []any{
				model.LLMTextContent{
					Type: "system",
					Text: c.llmConf.JudgePromt,
				},
				model.LLMTextContent{
					Type: "user",
					Text: promt,
				},
				model.LLMImgContent{
					Type: "image_url",
					ImgUrl: model.LLMImgURLContent{
						URL: fmt.Sprintf("data:%s;base64,%s", mimeTypeOrigin, base64ImgOrigin),
					},
					MinPixels: 64 * 32 * 32,
					MaxPixels: 2560 * 32 * 32,
				},
				model.LLMImgContent{
					Type: "image_url",
					ImgUrl: model.LLMImgURLContent{
						URL: fmt.Sprintf("data:%s;base64,%s", mimeType, base64Img),
					},
					MinPixels: 64 * 32 * 32,
					MaxPixels: 2560 * 32 * 32,
				},
			},
		}},
	}
	data, err := json.Marshal(msg)
	return data, err
}

func (c LLMRepository) CallLLM(ctx context.Context, msg []byte) (bool, string, error) {
	var res model.LLMResponse
	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		c.llmConf.URL,
		bytes.NewReader(msg),
	)
	if err != nil {
		return false, "", err
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", c.llmConf.Token))

	resp, err := c.Client.Do(req)
	if err != nil {
		return false, "", err
	}
	defer resp.Body.Close()

	responseBody, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20)) // 1 MiB
	if err != nil {
		return false, "", err
	}

	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return false, "", fmt.Errorf(
			"API returned %s: %s",
			resp.Status,
			string(responseBody),
		)
	}

	if err := json.Unmarshal(responseBody, &res); err != nil {
		return false, "", err
	}

	if len(res.Choices) != 1 {
		return false, "", fmt.Errorf("Empty choices: %v", res)
	}

	content := res.Choices[0].Message.Content
	// Result: true
	isEqual := strings.Contains(content[:20], "true")
	// Reason: ...
	sl := strings.Split(content, "Reason:")
	if len(sl) != 2 {
		return false, "", fmt.Errorf("Brocken reason: %s", content)
	}
	reason := strings.Trim(sl[1], " ")

	return isEqual, reason, nil
}

type KafkaReader interface {
	FetchMessage(ctx context.Context) (kafka.Message, error)
	CommitMessages(ctx context.Context, msgs ...kafka.Message) error
	Close() error
}

type HttpClient interface {
	Do(req *http.Request) (*http.Response, error)
}

func (c LLMRepository) Close() error {
	return c.KafkaReader.Close()
}

func (c LLMRepository) CommitMsg(ctx context.Context, msg kafka.Message) error {
	return c.KafkaReader.CommitMessages(ctx, msg)
}

func (c LLMRepository) FetchMsg(ctx context.Context) (kafka.Message, error) {
	return c.KafkaReader.FetchMessage(ctx)
}

func NewLLMRepository(ctx context.Context, kc config.KafkaConfig, llmc config.LLMConfig) *LLMRepository {
	client := &http.Client{
		Timeout: time.Duration(llmc.Timeout) * time.Second,
	}
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        []string{kc.Broker},
		Topic:          kc.Topic,
		GroupID:        kc.GroupID,
		StartOffset:    kafka.FirstOffset,
		CommitInterval: 0,
	})

	return &LLMRepository{
		Client:      client,
		llmConf:     llmc,
		kafkaConf:   kc,
		KafkaReader: reader,
	}
}
