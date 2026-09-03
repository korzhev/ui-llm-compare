package service

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/textproto"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/korzhev/ui-llm-compare/internal/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type saveImgCall struct {
	ctx         context.Context
	key         string
	data        string
	contentType string
	size        int64
}

type jobRepositoryMock struct {
	mu sync.Mutex

	getByIDFn   func(ctx context.Context, id int) (model.Job, error)
	createNewFn func(ctx context.Context, originKey string, currentStateKey string) (model.Job, error)
	saveImgFn   func(ctx context.Context, key string, reader io.Reader, contentType string, size int64) error
	sendMsgFn   func(ctx context.Context, id int, value model.JobKafkaMsg) error

	saveImgCalls []saveImgCall
}

func (m *jobRepositoryMock) GetByID(ctx context.Context, id int) (model.Job, error) {
	return m.getByIDFn(ctx, id)
}

func (m *jobRepositoryMock) CreateNew(ctx context.Context, originKey string, currentStateKey string) (model.Job, error) {
	return m.createNewFn(ctx, originKey, currentStateKey)
}

func (m *jobRepositoryMock) SaveImg(ctx context.Context, key string, reader io.Reader, contentType string, size int64) error {
	data, err := io.ReadAll(reader)
	if err != nil {
		return err
	}

	m.mu.Lock()
	m.saveImgCalls = append(m.saveImgCalls, saveImgCall{
		ctx:         ctx,
		key:         key,
		data:        string(data),
		contentType: contentType,
		size:        size,
	})
	m.mu.Unlock()

	if m.saveImgFn != nil {
		return m.saveImgFn(ctx, key, bytes.NewReader(data), contentType, size)
	}
	return nil
}

func (m *jobRepositoryMock) SendMsg(ctx context.Context, id int, value model.JobKafkaMsg) error {
	return m.sendMsgFn(ctx, id, value)
}

func (m *jobRepositoryMock) Close() error {
	return nil
}

func (m *jobRepositoryMock) getSaveImgCalls() []saveImgCall {
	m.mu.Lock()
	defer m.mu.Unlock()

	calls := make([]saveImgCall, len(m.saveImgCalls))
	copy(calls, m.saveImgCalls)
	return calls
}

func newMultipartFileHeader(t *testing.T, filename, contentType, content string) *multipart.FileHeader {
	t.Helper()

	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	header := make(textproto.MIMEHeader)
	header.Set("Content-Disposition", fmt.Sprintf(`form-data; name="images"; filename="%s"`, filename))
	if contentType != "" {
		header.Set("Content-Type", contentType)
	}
	part, err := writer.CreatePart(header)
	require.NoError(t, err)
	_, err = part.Write([]byte(content))
	require.NoError(t, err)
	require.NoError(t, writer.Close())

	form, err := multipart.NewReader(&body, writer.Boundary()).ReadForm(1 << 20)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, form.RemoveAll())
	})

	files := form.File["images"]
	require.Len(t, files, 1)
	return files[0]
}

func findSaveImgCallByData(t *testing.T, calls []saveImgCall, data string) saveImgCall {
	t.Helper()

	for _, call := range calls {
		if call.data == data {
			return call
		}
	}

	require.FailNow(t, "save image call not found", "data: %q", data)
	return saveImgCall{}
}

func TestJobService_GetByID_ReturnsRepositoryResult(t *testing.T) {
	type contextKey string
	ctx := context.WithValue(context.Background(), contextKey("request-id"), "request-42")
	expectedJob := model.Job{ID: 42, Status: model.JobStatusDone, IsEqual: true}
	repo := &jobRepositoryMock{
		getByIDFn: func(actualCtx context.Context, id int) (model.Job, error) {
			assert.Equal(t, "request-42", actualCtx.Value(contextKey("request-id")))
			assert.Equal(t, 42, id)
			return expectedJob, nil
		},
	}
	service := JobService{Repo: repo}

	job, err := service.GetByID(ctx, 42)

	require.NoError(t, err)
	assert.Equal(t, expectedJob, job)
}

func TestJobService_GetByID_ReturnsRepositoryError(t *testing.T) {
	repoErr := errors.New("get job failed")
	repo := &jobRepositoryMock{
		getByIDFn: func(context.Context, int) (model.Job, error) {
			return model.Job{}, repoErr
		},
	}
	service := JobService{Repo: repo}

	job, err := service.GetByID(context.Background(), 7)

	require.ErrorIs(t, err, repoErr)
	assert.Equal(t, model.Job{}, job)
}

func TestJobService_CreateNew_ReturnsRepositoryResult(t *testing.T) {
	expectedJob := model.Job{
		ID:              15,
		OriginKey:       "origin-key",
		CurrentStateKey: "current-key",
		Status:          model.JobStatusCreated,
	}
	repo := &jobRepositoryMock{
		createNewFn: func(_ context.Context, originKey string, currentStateKey string) (model.Job, error) {
			assert.Equal(t, "origin-key", originKey)
			assert.Equal(t, "current-key", currentStateKey)
			return expectedJob, nil
		},
	}
	service := JobService{Repo: repo}

	job, err := service.CreateNew(context.Background(), "origin-key", "current-key")

	require.NoError(t, err)
	assert.Equal(t, expectedJob, job)
}

func TestJobService_CreateNew_ReturnsRepositoryError(t *testing.T) {
	repoErr := errors.New("create job failed")
	repo := &jobRepositoryMock{
		createNewFn: func(context.Context, string, string) (model.Job, error) {
			return model.Job{}, repoErr
		},
	}
	service := JobService{Repo: repo}

	job, err := service.CreateNew(context.Background(), "origin-key", "current-key")

	require.ErrorIs(t, err, repoErr)
	assert.Equal(t, model.Job{}, job)
}

func TestJobService_SaveImg_GeneratesUUIDAndSavesImage(t *testing.T) {
	repo := &jobRepositoryMock{}
	service := JobService{Repo: repo}
	reader := bytes.NewBufferString("image-data")

	key, err := service.SaveImg(context.Background(), reader, "image/png", 10)

	require.NoError(t, err)
	_, err = uuid.Parse(key)
	require.NoError(t, err)
	calls := repo.getSaveImgCalls()
	require.Len(t, calls, 1)
	assert.Equal(t, key, calls[0].key)
	assert.Equal(t, "image-data", calls[0].data)
	assert.Equal(t, "image/png", calls[0].contentType)
	assert.Equal(t, int64(10), calls[0].size)
}

func TestJobService_SaveImg_ReturnsRepositoryErrorWithoutKey(t *testing.T) {
	repoErr := errors.New("save image failed")
	repo := &jobRepositoryMock{
		saveImgFn: func(context.Context, string, io.Reader, string, int64) error {
			return repoErr
		},
	}
	service := JobService{Repo: repo}

	key, err := service.SaveImg(context.Background(), bytes.NewBufferString("data"), "image/jpeg", 4)

	require.ErrorIs(t, err, repoErr)
	assert.Empty(t, key)
	require.Len(t, repo.getSaveImgCalls(), 1)
}

func TestJobService_GetFileInfo_ReturnsContentTypeAndSize(t *testing.T) {
	fileHeader := &multipart.FileHeader{
		Header: textproto.MIMEHeader{"Content-Type": []string{"image/webp"}},
		Size:   128,
	}
	service := JobService{}

	contentType, size := service.GetFileInfo(fileHeader)

	assert.Equal(t, "image/webp", contentType)
	assert.Equal(t, int64(128), size)
}

func TestJobService_GetFileInfo_UsesDefaultContentType(t *testing.T) {
	fileHeader := &multipart.FileHeader{Header: make(textproto.MIMEHeader), Size: 64}
	service := JobService{}

	contentType, size := service.GetFileInfo(fileHeader)

	assert.Equal(t, "application/octet-stream", contentType)
	assert.Equal(t, int64(64), size)
}

func TestJobService_SaveImgsParallel_SavesBothImages(t *testing.T) {
	originFile := newMultipartFileHeader(t, "origin.png", "image/png", "origin-data")
	currentFile := newMultipartFileHeader(t, "current.jpeg", "image/jpeg", "current-data")
	repo := &jobRepositoryMock{}
	service := JobService{Repo: repo}

	originKey, currentKey, err := service.SaveImgsParallel(context.Background(), originFile, currentFile)

	require.NoError(t, err)
	assert.NotEmpty(t, originKey)
	assert.NotEmpty(t, currentKey)
	assert.NotEqual(t, originKey, currentKey)
	calls := repo.getSaveImgCalls()
	require.Len(t, calls, 2)
	originCall := findSaveImgCallByData(t, calls, "origin-data")
	currentCall := findSaveImgCallByData(t, calls, "current-data")
	assert.Equal(t, originKey, originCall.key)
	assert.Equal(t, "image/png", originCall.contentType)
	assert.Equal(t, int64(len("origin-data")), originCall.size)
	assert.Equal(t, currentKey, currentCall.key)
	assert.Equal(t, "image/jpeg", currentCall.contentType)
	assert.Equal(t, int64(len("current-data")), currentCall.size)
}

func TestJobService_SaveImgsParallel_ReturnsFileOpenError(t *testing.T) {
	invalidOriginFile := &multipart.FileHeader{
		Filename: "missing.png",
		Header:   textproto.MIMEHeader{"Content-Type": []string{"image/png"}},
		Size:     12,
	}
	currentFile := newMultipartFileHeader(t, "current.png", "image/png", "current-data")
	repo := &jobRepositoryMock{}
	service := JobService{Repo: repo}

	originKey, currentKey, err := service.SaveImgsParallel(context.Background(), invalidOriginFile, currentFile)

	require.Error(t, err)
	assert.Empty(t, originKey)
	assert.NotEmpty(t, currentKey)
	calls := repo.getSaveImgCalls()
	require.Len(t, calls, 1)
	assert.Equal(t, "current-data", calls[0].data)
	assert.Equal(t, currentKey, calls[0].key)
}

func TestJobService_SaveImgsParallel_ReturnsRepositoryError(t *testing.T) {
	originFile := newMultipartFileHeader(t, "origin.png", "image/png", "origin-data")
	currentFile := newMultipartFileHeader(t, "current.png", "image/png", "current-data")
	repoErr := errors.New("origin upload failed")
	repo := &jobRepositoryMock{
		saveImgFn: func(_ context.Context, _ string, reader io.Reader, _ string, _ int64) error {
			data, err := io.ReadAll(reader)
			if err != nil {
				return err
			}
			if string(data) == "origin-data" {
				return repoErr
			}
			return nil
		},
	}
	service := JobService{Repo: repo}

	originKey, currentKey, err := service.SaveImgsParallel(context.Background(), originFile, currentFile)

	require.ErrorIs(t, err, repoErr)
	assert.Empty(t, originKey)
	assert.NotEmpty(t, currentKey)
	require.Len(t, repo.getSaveImgCalls(), 2)
}

func TestJobService_SendMsg_BuildsKafkaMessage(t *testing.T) {
	repo := &jobRepositoryMock{
		sendMsgFn: func(_ context.Context, id int, value model.JobKafkaMsg) error {
			assert.Equal(t, 42, id)
			assert.Equal(t, model.JobKafkaMsg{
				ID:              42,
				OriginKey:       "origin-key",
				CurrentStateKey: "current-key",
			}, value)
			return nil
		},
	}
	service := JobService{Repo: repo}

	err := service.SendMsg(context.Background(), 42, "origin-key", "current-key")

	require.NoError(t, err)
}

func TestJobService_SendMsg_ReturnsRepositoryError(t *testing.T) {
	repoErr := errors.New("send message failed")
	repo := &jobRepositoryMock{
		sendMsgFn: func(context.Context, int, model.JobKafkaMsg) error {
			return repoErr
		},
	}
	service := JobService{Repo: repo}

	err := service.SendMsg(context.Background(), 7, "origin-key", "current-key")

	require.ErrorIs(t, err, repoErr)
}
