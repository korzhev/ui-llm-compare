package service

import (
	"context"
	"encoding/base64"
	"errors"
	"testing"

	"github.com/korzhev/ui-llm-compare/internal/model"
	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type workerRepositoryMock struct {
	formatLLMRequestFn func(promt, mimeTypeOrigin, mimeType, base64ImgOrigin, base64Img string) ([]byte, error)
	callLLMFn          func(ctx context.Context, msg []byte) (bool, string, error)
	commitMsgFn        func(ctx context.Context, msg kafka.Message) error
	fetchMsgFn         func(ctx context.Context) (kafka.Message, error)
}

func (m *workerRepositoryMock) FormatLLMRequest(
	promt,
	mimeTypeOrigin,
	mimeType,
	base64ImgOrigin,
	base64Img string,
) ([]byte, error) {
	return m.formatLLMRequestFn(promt, mimeTypeOrigin, mimeType, base64ImgOrigin, base64Img)
}

func (m *workerRepositoryMock) CallLLM(ctx context.Context, msg []byte) (bool, string, error) {
	return m.callLLMFn(ctx, msg)
}

func (m *workerRepositoryMock) CommitMsg(ctx context.Context, msg kafka.Message) error {
	return m.commitMsgFn(ctx, msg)
}

func (m *workerRepositoryMock) FetchMsg(ctx context.Context) (kafka.Message, error) {
	return m.fetchMsgFn(ctx)
}

var _ WorkerRepository = (*workerRepositoryMock)(nil)

func TestWorkerService_CheckImgs_CallsLLMWithEncodedImages(t *testing.T) {
	type contextKey string
	ctx := context.WithValue(context.Background(), contextKey("request-id"), "request-42")
	var imageKeys []string
	repo := &jobRepositoryMock{
		getImageFn: func(actualCtx context.Context, key string) (string, []byte, error) {
			assert.Equal(t, "request-42", actualCtx.Value(contextKey("request-id")))
			imageKeys = append(imageKeys, key)
			switch key {
			case "origin-key":
				return "image/png", []byte("origin-image"), nil
			case "current-key":
				return "image/jpeg", []byte("current-image"), nil
			default:
				return "", nil, errors.New("unexpected image key")
			}
		},
	}
	formattedMessage := []byte(`{"request":"formatted"}`)
	workerRepo := &workerRepositoryMock{
		formatLLMRequestFn: func(prompt, originMIMEType, currentMIMEType, originImage, currentImage string) ([]byte, error) {
			assert.Equal(t, "TODO", prompt)
			assert.Equal(t, "image/png", originMIMEType)
			assert.Equal(t, "image/jpeg", currentMIMEType)
			assert.Equal(t, base64.StdEncoding.EncodeToString([]byte("origin-image")), originImage)
			assert.Equal(t, base64.StdEncoding.EncodeToString([]byte("current-image")), currentImage)
			return formattedMessage, nil
		},
		callLLMFn: func(actualCtx context.Context, msg []byte) (bool, string, error) {
			assert.Same(t, ctx, actualCtx)
			assert.Equal(t, formattedMessage, msg)
			return true, "images are equal", nil
		},
	}
	service := WorkerService{Repo: repo, WorkerRepo: workerRepo}

	isEqual, reason, err := service.CheckImgs(ctx, "origin-key", "current-key")

	require.NoError(t, err)
	assert.True(t, isEqual)
	assert.Equal(t, "images are equal", reason)
	assert.Equal(t, []string{"origin-key", "current-key"}, imageKeys)
}

func TestWorkerService_CheckImgs_OriginImageErrorStopsProcessing(t *testing.T) {
	imageErr := errors.New("origin image failed")
	getImageCalls := 0
	repo := &jobRepositoryMock{
		getImageFn: func(context.Context, string) (string, []byte, error) {
			getImageCalls++
			return "", nil, imageErr
		},
	}
	service := WorkerService{
		Repo:       repo,
		WorkerRepo: &workerRepositoryMock{},
	}

	isEqual, reason, err := service.CheckImgs(context.Background(), "origin-key", "current-key")

	require.ErrorIs(t, err, imageErr)
	assert.False(t, isEqual)
	assert.Empty(t, reason)
	assert.Equal(t, 1, getImageCalls)
}

func TestWorkerService_CheckImgs_CurrentImageErrorStopsProcessing(t *testing.T) {
	imageErr := errors.New("current image failed")
	getImageCalls := 0
	repo := &jobRepositoryMock{
		getImageFn: func(_ context.Context, key string) (string, []byte, error) {
			getImageCalls++
			if key == "current-key" {
				return "", nil, imageErr
			}
			return "image/png", []byte("origin-image"), nil
		},
	}
	service := WorkerService{
		Repo:       repo,
		WorkerRepo: &workerRepositoryMock{},
	}

	isEqual, reason, err := service.CheckImgs(context.Background(), "origin-key", "current-key")

	require.ErrorIs(t, err, imageErr)
	assert.False(t, isEqual)
	assert.Empty(t, reason)
	assert.Equal(t, 2, getImageCalls)
}

func TestWorkerService_CheckImgs_ReturnsFormatError(t *testing.T) {
	formatErr := errors.New("format failed")
	callLLMCalled := false
	repo := &jobRepositoryMock{
		getImageFn: func(context.Context, string) (string, []byte, error) {
			return "image/png", []byte("image"), nil
		},
	}
	workerRepo := &workerRepositoryMock{
		formatLLMRequestFn: func(string, string, string, string, string) ([]byte, error) {
			return nil, formatErr
		},
		callLLMFn: func(context.Context, []byte) (bool, string, error) {
			callLLMCalled = true
			return false, "", nil
		},
	}
	service := WorkerService{Repo: repo, WorkerRepo: workerRepo}

	isEqual, reason, err := service.CheckImgs(context.Background(), "origin-key", "current-key")

	require.ErrorIs(t, err, formatErr)
	assert.False(t, isEqual)
	assert.Empty(t, reason)
	assert.False(t, callLLMCalled)
}

func TestWorkerService_CheckImgs_ReturnsLLMError(t *testing.T) {
	llmErr := errors.New("LLM call failed")
	repo := &jobRepositoryMock{
		getImageFn: func(context.Context, string) (string, []byte, error) {
			return "image/png", []byte("image"), nil
		},
	}
	workerRepo := &workerRepositoryMock{
		formatLLMRequestFn: func(string, string, string, string, string) ([]byte, error) {
			return []byte("formatted message"), nil
		},
		callLLMFn: func(context.Context, []byte) (bool, string, error) {
			return false, "", llmErr
		},
	}
	service := WorkerService{Repo: repo, WorkerRepo: workerRepo}

	isEqual, reason, err := service.CheckImgs(context.Background(), "origin-key", "current-key")

	require.ErrorIs(t, err, llmErr)
	assert.False(t, isEqual)
	assert.Empty(t, reason)
}

func TestWorkerService_GetImgKeys_ReturnsJobImageKeys(t *testing.T) {
	type contextKey string
	ctx := context.WithValue(context.Background(), contextKey("request-id"), "request-42")
	repo := &jobRepositoryMock{
		getByIDFn: func(actualCtx context.Context, id int) (model.Job, error) {
			assert.Same(t, ctx, actualCtx)
			assert.Equal(t, 42, id)
			return model.Job{OriginKey: "origin-key", CurrentStateKey: "current-key"}, nil
		},
	}
	service := WorkerService{Repo: repo}

	originKey, currentKey, err := service.GetImgKeys(ctx, 42)

	require.NoError(t, err)
	assert.Equal(t, "origin-key", originKey)
	assert.Equal(t, "current-key", currentKey)
}

func TestWorkerService_GetImgKeys_ReturnsRepositoryError(t *testing.T) {
	repoErr := errors.New("get job failed")
	repo := &jobRepositoryMock{
		getByIDFn: func(context.Context, int) (model.Job, error) {
			return model.Job{}, repoErr
		},
	}
	service := WorkerService{Repo: repo}

	originKey, currentKey, err := service.GetImgKeys(context.Background(), 7)

	require.ErrorIs(t, err, repoErr)
	assert.Empty(t, originKey)
	assert.Empty(t, currentKey)
}

func TestWorkerService_RunJob_SavesLLMResult(t *testing.T) {
	type contextKey string
	ctx := context.WithValue(context.Background(), contextKey("request-id"), "request-42")
	var calls []string
	repo := &jobRepositoryMock{
		startJobFn: func(actualCtx context.Context, id int) error {
			assert.Same(t, ctx, actualCtx)
			assert.Equal(t, 42, id)
			calls = append(calls, "start")
			return nil
		},
		getImageFn: func(actualCtx context.Context, key string) (string, []byte, error) {
			assert.Same(t, ctx, actualCtx)
			calls = append(calls, "image:"+key)
			return "image/png", []byte(key), nil
		},
		saveJobResultFn: func(actualCtx context.Context, id int, isEqual bool, reason string) error {
			assert.Same(t, ctx, actualCtx)
			assert.Equal(t, 42, id)
			assert.True(t, isEqual)
			assert.Equal(t, "same layout", reason)
			calls = append(calls, "save")
			return nil
		},
	}
	workerRepo := &workerRepositoryMock{
		formatLLMRequestFn: func(string, string, string, string, string) ([]byte, error) {
			calls = append(calls, "format")
			return []byte("formatted"), nil
		},
		callLLMFn: func(actualCtx context.Context, msg []byte) (bool, string, error) {
			assert.Same(t, ctx, actualCtx)
			assert.Equal(t, []byte("formatted"), msg)
			calls = append(calls, "call")
			return true, "same layout", nil
		},
	}
	service := WorkerService{Repo: repo, WorkerRepo: workerRepo}
	message := model.JobKafkaMsg{ID: 42, OriginKey: "origin-key", CurrentStateKey: "current-key"}

	err := service.RunJob(ctx, message)

	require.NoError(t, err)
	assert.Equal(t, []string{
		"start",
		"image:origin-key",
		"image:current-key",
		"format",
		"call",
		"save",
	}, calls)
}

func TestWorkerService_RunJob_StartErrorStopsProcessing(t *testing.T) {
	startErr := errors.New("start failed")
	getImageCalled := false
	repo := &jobRepositoryMock{
		startJobFn: func(context.Context, int) error {
			return startErr
		},
		getImageFn: func(context.Context, string) (string, []byte, error) {
			getImageCalled = true
			return "", nil, nil
		},
	}
	service := WorkerService{Repo: repo, WorkerRepo: &workerRepositoryMock{}}

	err := service.RunJob(context.Background(), model.JobKafkaMsg{ID: 42})

	require.ErrorIs(t, err, startErr)
	assert.False(t, getImageCalled)
}

func TestWorkerService_RunJob_CheckImagesErrorStopsProcessing(t *testing.T) {
	imageErr := errors.New("get image failed")
	saveResultCalled := false
	failJobCalled := false
	repo := &jobRepositoryMock{
		getImageFn: func(context.Context, string) (string, []byte, error) {
			return "", nil, imageErr
		},
		saveJobResultFn: func(context.Context, int, bool, string) error {
			saveResultCalled = true
			return nil
		},
		failJobFn: func(context.Context, int) error {
			failJobCalled = true
			return nil
		},
	}
	service := WorkerService{Repo: repo, WorkerRepo: &workerRepositoryMock{}}

	err := service.RunJob(context.Background(), model.JobKafkaMsg{ID: 42, OriginKey: "origin-key"})

	require.ErrorIs(t, err, imageErr)
	assert.False(t, saveResultCalled)
	assert.False(t, failJobCalled)
}

func TestWorkerService_RunJob_JoinsSaveResultAndFailJobErrors(t *testing.T) {
	saveErr := errors.New("save result failed")
	failErr := errors.New("fail job failed")
	repo := &jobRepositoryMock{
		getImageFn: func(context.Context, string) (string, []byte, error) {
			return "image/png", []byte("image"), nil
		},
		saveJobResultFn: func(_ context.Context, id int, isEqual bool, reason string) error {
			assert.Equal(t, 42, id)
			assert.False(t, isEqual)
			assert.Equal(t, "different", reason)
			return saveErr
		},
		failJobFn: func(_ context.Context, id int) error {
			assert.Equal(t, 42, id)
			return failErr
		},
	}
	workerRepo := &workerRepositoryMock{
		formatLLMRequestFn: func(string, string, string, string, string) ([]byte, error) {
			return []byte("formatted"), nil
		},
		callLLMFn: func(context.Context, []byte) (bool, string, error) {
			return false, "different", nil
		},
	}
	service := WorkerService{Repo: repo, WorkerRepo: workerRepo}

	err := service.RunJob(context.Background(), model.JobKafkaMsg{ID: 42})

	require.Error(t, err)
	assert.ErrorIs(t, err, saveErr)
	assert.ErrorIs(t, err, failErr)
}

func TestWorkerService_ProcessMsg_ProcessesAndCommitsMessage(t *testing.T) {
	type contextKey string
	ctx := context.WithValue(context.Background(), contextKey("request-id"), "request-42")
	var startedID int
	var savedID int
	repo := &jobRepositoryMock{
		startJobFn: func(actualCtx context.Context, id int) error {
			assert.Same(t, ctx, actualCtx)
			startedID = id
			return nil
		},
		getImageFn: func(_ context.Context, key string) (string, []byte, error) {
			return "image/png", []byte(key), nil
		},
		saveJobResultFn: func(_ context.Context, id int, isEqual bool, reason string) error {
			savedID = id
			assert.True(t, isEqual)
			assert.Equal(t, "equal", reason)
			return nil
		},
	}
	message := kafka.Message{
		Topic:     "jobs",
		Partition: 1,
		Offset:    17,
		Value:     []byte(`{"id":42,"origin_key":"origin-key","current_state_key":"current-key"}`),
	}
	commitCalled := false
	workerRepo := &workerRepositoryMock{
		formatLLMRequestFn: func(_ string, _ string, _ string, originImage, currentImage string) ([]byte, error) {
			assert.Equal(t, base64.StdEncoding.EncodeToString([]byte("origin-key")), originImage)
			assert.Equal(t, base64.StdEncoding.EncodeToString([]byte("current-key")), currentImage)
			return []byte("formatted"), nil
		},
		callLLMFn: func(context.Context, []byte) (bool, string, error) {
			return true, "equal", nil
		},
		commitMsgFn: func(actualCtx context.Context, actualMessage kafka.Message) error {
			assert.Same(t, ctx, actualCtx)
			assert.Equal(t, message, actualMessage)
			commitCalled = true
			return nil
		},
	}
	service := WorkerService{Repo: repo, WorkerRepo: workerRepo}

	err := service.ProcessMsg(ctx, message)

	require.NoError(t, err)
	assert.Equal(t, 42, startedID)
	assert.Equal(t, 42, savedID)
	assert.True(t, commitCalled)
}

func TestWorkerService_ProcessMsg_InvalidJSONSkipsJobAndCommit(t *testing.T) {
	startCalled := false
	commitCalled := false
	repo := &jobRepositoryMock{
		startJobFn: func(context.Context, int) error {
			startCalled = true
			return nil
		},
	}
	workerRepo := &workerRepositoryMock{
		commitMsgFn: func(context.Context, kafka.Message) error {
			commitCalled = true
			return nil
		},
	}
	service := WorkerService{Repo: repo, WorkerRepo: workerRepo}

	err := service.ProcessMsg(context.Background(), kafka.Message{Value: []byte(`not-json`)})

	require.Error(t, err)
	assert.False(t, startCalled)
	assert.False(t, commitCalled)
}

func TestWorkerService_ProcessMsg_RunJobErrorSkipsCommit(t *testing.T) {
	startErr := errors.New("start failed")
	commitCalled := false
	repo := &jobRepositoryMock{
		startJobFn: func(context.Context, int) error {
			return startErr
		},
	}
	workerRepo := &workerRepositoryMock{
		commitMsgFn: func(context.Context, kafka.Message) error {
			commitCalled = true
			return nil
		},
	}
	service := WorkerService{Repo: repo, WorkerRepo: workerRepo}
	message := kafka.Message{Value: []byte(`{"id":42}`)}

	err := service.ProcessMsg(context.Background(), message)

	require.ErrorIs(t, err, startErr)
	assert.False(t, commitCalled)
}

func TestWorkerService_ProcessMsg_ReturnsCommitError(t *testing.T) {
	commitErr := errors.New("commit failed")
	repo := &jobRepositoryMock{
		getImageFn: func(context.Context, string) (string, []byte, error) {
			return "image/png", []byte("image"), nil
		},
	}
	workerRepo := &workerRepositoryMock{
		formatLLMRequestFn: func(string, string, string, string, string) ([]byte, error) {
			return []byte("formatted"), nil
		},
		callLLMFn: func(context.Context, []byte) (bool, string, error) {
			return true, "equal", nil
		},
		commitMsgFn: func(context.Context, kafka.Message) error {
			return commitErr
		},
	}
	service := WorkerService{Repo: repo, WorkerRepo: workerRepo}
	message := kafka.Message{Value: []byte(`{"id":42}`)}

	err := service.ProcessMsg(context.Background(), message)

	require.ErrorIs(t, err, commitErr)
}

func TestWorkerService_FetchMsg_ReturnsRepositoryResult(t *testing.T) {
	type contextKey string
	ctx := context.WithValue(context.Background(), contextKey("request-id"), "request-42")
	expectedMessage := kafka.Message{Topic: "jobs", Partition: 2, Offset: 17, Value: []byte("job")}
	workerRepo := &workerRepositoryMock{
		fetchMsgFn: func(actualCtx context.Context) (kafka.Message, error) {
			assert.Same(t, ctx, actualCtx)
			return expectedMessage, nil
		},
	}
	service := WorkerService{WorkerRepo: workerRepo}

	message, err := service.FetchMsg(ctx)

	require.NoError(t, err)
	assert.Equal(t, expectedMessage, message)
}

func TestWorkerService_FetchMsg_ReturnsRepositoryError(t *testing.T) {
	fetchErr := errors.New("fetch failed")
	workerRepo := &workerRepositoryMock{
		fetchMsgFn: func(context.Context) (kafka.Message, error) {
			return kafka.Message{}, fetchErr
		},
	}
	service := WorkerService{WorkerRepo: workerRepo}

	message, err := service.FetchMsg(context.Background())

	require.ErrorIs(t, err, fetchErr)
	assert.Equal(t, kafka.Message{}, message)
}
