package repository

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/korzhev/ui-llm-compare/internal/config"
	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type httpClientMock struct {
	request  *http.Request
	response *http.Response
	err      error
}

func (m *httpClientMock) Do(req *http.Request) (*http.Response, error) {
	m.request = req
	return m.response, m.err
}

type kafkaReaderMock struct {
	fetchedMessage    kafka.Message
	committedMessages []kafka.Message
	fetchErr          error
	commitErr         error
	closeErr          error
	closed            bool
}

func (m *kafkaReaderMock) FetchMessage(_ context.Context) (kafka.Message, error) {
	return m.fetchedMessage, m.fetchErr
}

func (m *kafkaReaderMock) CommitMessages(_ context.Context, msgs ...kafka.Message) error {
	m.committedMessages = append(m.committedMessages, msgs...)
	return m.commitErr
}

func (m *kafkaReaderMock) Close() error {
	m.closed = true
	return m.closeErr
}

type errorReadCloser struct {
	err    error
	closed bool
}

func (r *errorReadCloser) Read(_ []byte) (int, error) {
	return 0, r.err
}

func (r *errorReadCloser) Close() error {
	r.closed = true
	return nil
}

func TestLLMRepository_FormatLLMRequest_FormatsJSON(t *testing.T) {
	repository := LLMRepository{llmConf: config.LLMConfig{
		Model:      "vision-model",
		JudgePromt: "Compare the screenshots",
	}}

	data, err := repository.FormatLLMRequest(
		"Focus on the header",
		"image/png",
		"image/jpeg",
		"b3JpZ2lu",
		"Y3VycmVudA==",
	)

	require.NoError(t, err)
	assert.JSONEq(t, `{
		"model": "vision-model",
		"messages": [{
			"role": "user",
			"content": [
				{"type": "system", "text": "Compare the screenshots"},
				{"type": "user", "text": "Focus on the header"},
				{
					"type": "image_url",
					"image_url": {"url": "data:image/png;base64,b3JpZ2lu"},
					"min_pixels": 65536,
					"max_pixels": 2621440
				},
				{
					"type": "image_url",
					"image_url": {"url": "data:image/jpeg;base64,Y3VycmVudA=="},
					"min_pixels": 65536,
					"max_pixels": 2621440
				}
			]
		}]
	}`, string(data))
}

func TestLLMRepository_CallLLM_ReturnsComparisonResult(t *testing.T) {
	tests := []struct {
		name           string
		content        string
		expectedEqual  bool
		expectedReason string
	}{
		{
			name:           "equal images",
			content:        "Result: true\nReason: images are equal",
			expectedEqual:  true,
			expectedReason: "images are equal",
		},
		{
			name:           "different images",
			content:        "Result: false\nReason: header color differs",
			expectedEqual:  false,
			expectedReason: "header color differs",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			body := `{"choices":[{"message":{"content":"` + strings.ReplaceAll(tt.content, "\n", `\n`) + `"}}]}`
			client := &httpClientMock{response: &http.Response{
				StatusCode: http.StatusOK,
				Status:     "200 OK",
				Body:       io.NopCloser(strings.NewReader(body)),
			}}
			repository := LLMRepository{
				Client: client,
				llmConf: config.LLMConfig{
					URL:   "https://llm.example.com/v1/chat/completions",
					Token: "secret-token",
				},
			}
			ctx := context.WithValue(context.Background(), struct{}{}, "request-context")
			requestBody := []byte(`{"model":"vision-model"}`)

			isEqual, reason, err := repository.CallLLM(ctx, requestBody)

			require.NoError(t, err)
			assert.Equal(t, tt.expectedEqual, isEqual)
			assert.Equal(t, tt.expectedReason, reason)
			require.NotNil(t, client.request)
			assert.Equal(t, http.MethodPost, client.request.Method)
			assert.Equal(t, "https://llm.example.com/v1/chat/completions", client.request.URL.String())
			assert.Equal(t, "application/json", client.request.Header.Get("Accept"))
			assert.Equal(t, "application/json", client.request.Header.Get("Content-Type"))
			assert.Equal(t, "Bearer secret-token", client.request.Header.Get("Authorization"))
			assert.Same(t, ctx, client.request.Context())
			actualBody, readErr := io.ReadAll(client.request.Body)
			require.NoError(t, readErr)
			assert.Equal(t, requestBody, actualBody)
		})
	}
}

func TestLLMRepository_CallLLM_ReturnsRequestCreationError(t *testing.T) {
	repository := LLMRepository{llmConf: config.LLMConfig{URL: "://invalid-url"}}

	isEqual, reason, err := repository.CallLLM(context.Background(), []byte(`{}`))

	require.Error(t, err)
	assert.False(t, isEqual)
	assert.Empty(t, reason)
}

func TestLLMRepository_CallLLM_ReturnsHTTPClientError(t *testing.T) {
	clientErr := errors.New("request failed")
	client := &httpClientMock{err: clientErr}
	repository := LLMRepository{
		Client:  client,
		llmConf: config.LLMConfig{URL: "https://llm.example.com"},
	}

	isEqual, reason, err := repository.CallLLM(context.Background(), []byte(`{}`))

	require.ErrorIs(t, err, clientErr)
	assert.False(t, isEqual)
	assert.Empty(t, reason)
}

func TestLLMRepository_CallLLM_ReturnsResponseReadError(t *testing.T) {
	readErr := errors.New("read failed")
	body := &errorReadCloser{err: readErr}
	client := &httpClientMock{response: &http.Response{
		StatusCode: http.StatusOK,
		Status:     "200 OK",
		Body:       body,
	}}
	repository := LLMRepository{
		Client:  client,
		llmConf: config.LLMConfig{URL: "https://llm.example.com"},
	}

	isEqual, reason, err := repository.CallLLM(context.Background(), []byte(`{}`))

	require.ErrorIs(t, err, readErr)
	assert.False(t, isEqual)
	assert.Empty(t, reason)
	assert.True(t, body.closed)
}

func TestLLMRepository_CallLLM_ReturnsAPIError(t *testing.T) {
	client := &httpClientMock{response: &http.Response{
		StatusCode: http.StatusBadRequest,
		Status:     "400 Bad Request",
		Body:       io.NopCloser(strings.NewReader(`{"error":"invalid request"}`)),
	}}
	repository := LLMRepository{
		Client:  client,
		llmConf: config.LLMConfig{URL: "https://llm.example.com"},
	}

	isEqual, reason, err := repository.CallLLM(context.Background(), []byte(`{}`))

	require.EqualError(t, err, `API returned 400 Bad Request: {"error":"invalid request"}`)
	assert.False(t, isEqual)
	assert.Empty(t, reason)
}

func TestLLMRepository_CallLLM_ReturnsInvalidJSONError(t *testing.T) {
	client := &httpClientMock{response: &http.Response{
		StatusCode: http.StatusOK,
		Status:     "200 OK",
		Body:       io.NopCloser(strings.NewReader(`not-json`)),
	}}
	repository := LLMRepository{
		Client:  client,
		llmConf: config.LLMConfig{URL: "https://llm.example.com"},
	}

	isEqual, reason, err := repository.CallLLM(context.Background(), []byte(`{}`))

	require.Error(t, err)
	assert.False(t, isEqual)
	assert.Empty(t, reason)
}

func TestLLMRepository_CallLLM_ReturnsChoicesError(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{name: "empty choices", body: `{"choices":[]}`},
		{name: "multiple choices", body: `{"choices":[{"message":{}},{"message":{}}]}`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := &httpClientMock{response: &http.Response{
				StatusCode: http.StatusOK,
				Status:     "200 OK",
				Body:       io.NopCloser(strings.NewReader(tt.body)),
			}}
			repository := LLMRepository{
				Client:  client,
				llmConf: config.LLMConfig{URL: "https://llm.example.com"},
			}

			isEqual, reason, err := repository.CallLLM(context.Background(), []byte(`{}`))

			require.ErrorContains(t, err, "Empty choices:")
			assert.False(t, isEqual)
			assert.Empty(t, reason)
		})
	}
}

func TestLLMRepository_CallLLM_ReturnsBrokenReasonError(t *testing.T) {
	client := &httpClientMock{response: &http.Response{
		StatusCode: http.StatusOK,
		Status:     "200 OK",
		Body: io.NopCloser(strings.NewReader(
			`{"choices":[{"message":{"content":"Result: true but no explanation"}}]}`,
		)),
	}}
	repository := LLMRepository{
		Client:  client,
		llmConf: config.LLMConfig{URL: "https://llm.example.com"},
	}

	isEqual, reason, err := repository.CallLLM(context.Background(), []byte(`{}`))

	require.EqualError(t, err, "Brocken reason: Result: true but no explanation")
	assert.False(t, isEqual)
	assert.Empty(t, reason)
}

func TestLLMRepository_FetchMsg_ReturnsKafkaMessage(t *testing.T) {
	expected := kafka.Message{Topic: "jobs", Partition: 2, Offset: 17, Value: []byte("job")}
	reader := &kafkaReaderMock{fetchedMessage: expected}
	repository := LLMRepository{KafkaReader: reader}

	message, err := repository.FetchMsg(context.Background())

	require.NoError(t, err)
	assert.Equal(t, expected, message)
}

func TestLLMRepository_FetchMsg_ReturnsKafkaError(t *testing.T) {
	fetchErr := errors.New("fetch failed")
	reader := &kafkaReaderMock{fetchErr: fetchErr}
	repository := LLMRepository{KafkaReader: reader}

	message, err := repository.FetchMsg(context.Background())

	require.ErrorIs(t, err, fetchErr)
	assert.Equal(t, kafka.Message{}, message)
}

func TestLLMRepository_CommitMsg_CommitsKafkaMessage(t *testing.T) {
	reader := &kafkaReaderMock{}
	repository := LLMRepository{KafkaReader: reader}
	message := kafka.Message{Topic: "jobs", Partition: 1, Offset: 8}

	err := repository.CommitMsg(context.Background(), message)

	require.NoError(t, err)
	assert.Equal(t, []kafka.Message{message}, reader.committedMessages)
}

func TestLLMRepository_CommitMsg_ReturnsKafkaError(t *testing.T) {
	commitErr := errors.New("commit failed")
	reader := &kafkaReaderMock{commitErr: commitErr}
	repository := LLMRepository{KafkaReader: reader}
	message := kafka.Message{Topic: "jobs", Offset: 8}

	err := repository.CommitMsg(context.Background(), message)

	require.ErrorIs(t, err, commitErr)
	assert.Equal(t, []kafka.Message{message}, reader.committedMessages)
}

func TestLLMRepository_Close_ClosesKafkaReader(t *testing.T) {
	reader := &kafkaReaderMock{}
	repository := LLMRepository{KafkaReader: reader}

	err := repository.Close()

	require.NoError(t, err)
	assert.True(t, reader.closed)
}

func TestLLMRepository_Close_ReturnsKafkaError(t *testing.T) {
	closeErr := errors.New("close failed")
	reader := &kafkaReaderMock{closeErr: closeErr}
	repository := LLMRepository{KafkaReader: reader}

	err := repository.Close()

	require.ErrorIs(t, err, closeErr)
	assert.True(t, reader.closed)
}

func TestNewLLMRepository_ConfiguresClients(t *testing.T) {
	kafkaConfig := config.KafkaConfig{
		Broker:  "kafka:9092",
		Topic:   "jobs",
		GroupID: "workers",
	}
	llmConfig := config.LLMConfig{
		URL:        "https://llm.example.com",
		Model:      "vision-model",
		Token:      "secret-token",
		JudgePromt: "Compare screenshots",
		Timeout:    12,
	}

	repository := NewLLMRepository(context.Background(), kafkaConfig, llmConfig)
	t.Cleanup(func() {
		require.NoError(t, repository.Close())
	})

	require.NotNil(t, repository)
	assert.Equal(t, llmConfig, repository.llmConf)
	assert.Equal(t, kafkaConfig, repository.kafkaConf)
	httpClient, ok := repository.Client.(*http.Client)
	require.True(t, ok)
	assert.Equal(t, 12*time.Second, httpClient.Timeout)
	kafkaReader, ok := repository.KafkaReader.(*kafka.Reader)
	require.True(t, ok)
	readerConfig := kafkaReader.Config()
	assert.Equal(t, []string{"kafka:9092"}, readerConfig.Brokers)
	assert.Equal(t, "jobs", readerConfig.Topic)
	assert.Equal(t, "workers", readerConfig.GroupID)
	assert.Equal(t, kafka.FirstOffset, readerConfig.StartOffset)
	assert.Zero(t, readerConfig.CommitInterval)
}
