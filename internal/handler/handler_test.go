package handler

import (
	"bytes"
	"context"
	"errors"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/korzhev/ui-llm-compare/internal/logger"
	"github.com/korzhev/ui-llm-compare/internal/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

type jobServiceMock struct {
	getByIDFn          func(ctx context.Context, id int) (model.Job, error)
	createNewFn        func(ctx context.Context, originKey string, currentStateKey string) (model.Job, error)
	saveImgsParallelFn func(ctx context.Context, origin model.FormFile, current model.FormFile) (string, string, error)
	sendMsgFn          func(ctx context.Context, id int, originKey, currentStateKey string) error
	rollbackJobFn      func(ctx context.Context, id int) (model.Job, error)
	rollbackImgsFn     func(ctx context.Context, originKey, currentStateKey string) error

	getByIDCalls          int
	createNewCalls        int
	saveImgsParallelCalls int
	sendMsgCalls          int
	rollbackJobCalls      int
	rollbackImgsCalls     int
}

func (m *jobServiceMock) GetByID(ctx context.Context, id int) (model.Job, error) {
	m.getByIDCalls++
	if m.getByIDFn != nil {
		return m.getByIDFn(ctx, id)
	}
	return model.Job{}, nil
}

func (m *jobServiceMock) CreateNew(ctx context.Context, originKey string, currentStateKey string) (model.Job, error) {
	m.createNewCalls++
	if m.createNewFn != nil {
		return m.createNewFn(ctx, originKey, currentStateKey)
	}
	return model.Job{}, nil
}

func (m *jobServiceMock) SaveImgsParallel(
	ctx context.Context,
	origin model.FormFile,
	current model.FormFile,
) (string, string, error) {
	m.saveImgsParallelCalls++
	if m.saveImgsParallelFn != nil {
		return m.saveImgsParallelFn(ctx, origin, current)
	}
	return "", "", nil
}

var _ JobService = (*jobServiceMock)(nil)

func (m *jobServiceMock) SendMsg(ctx context.Context, id int, originKey, currentStateKey string) error {
	m.sendMsgCalls++
	if m.sendMsgFn != nil {
		return m.sendMsgFn(ctx, id, originKey, currentStateKey)
	}
	return nil
}

func (m *jobServiceMock) RollbackJob(ctx context.Context, id int) (model.Job, error) {
	m.rollbackJobCalls++
	if m.rollbackJobFn != nil {
		return m.rollbackJobFn(ctx, id)
	}
	return model.Job{}, nil
}

func (m *jobServiceMock) RollbackImgs(ctx context.Context, originKey, currentStateKey string) error {
	m.rollbackImgsCalls++
	if m.rollbackImgsFn != nil {
		return m.rollbackImgsFn(ctx, originKey, currentStateKey)
	}
	return nil
}

func setupTestLogger(t *testing.T) {
	t.Helper()

	previousLogger := logger.Log
	logger.Log = zap.NewNop().Sugar()
	t.Cleanup(func() {
		logger.Log = previousLogger
	})
}

func newMultipartRequest(t *testing.T, addParts func(writer *multipart.Writer)) *http.Request {
	t.Helper()

	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	addParts(writer)
	require.NoError(t, writer.Close())

	request := httptest.NewRequest(http.MethodPost, "/api/compare", &body)
	request.Header.Set("Content-Type", writer.FormDataContentType())
	return request
}

func addMultipartFile(t *testing.T, writer *multipart.Writer, fieldName, filename string, data []byte) {
	t.Helper()

	part, err := writer.CreateFormFile(fieldName, filename)
	require.NoError(t, err)
	_, err = part.Write(data)
	require.NoError(t, err)
}

func performGetJobStatusRequest(handler JobHandler, path string) *httptest.ResponseRecorder {
	router := chi.NewRouter()
	router.Get("/api/jobs/{id}", handler.GetJobStatusHandlerFunc)
	request := httptest.NewRequest(http.MethodGet, path, nil)
	response := httptest.NewRecorder()
	router.ServeHTTP(response, request)
	return response
}

func TestJobHandler_RecieveImgsHandlerFunc_CreatesJob(t *testing.T) {
	setupTestLogger(t)
	service := &jobServiceMock{
		saveImgsParallelFn: func(_ context.Context, origin model.FormFile, current model.FormFile) (string, string, error) {
			assert.Equal(t, int64(len("origin-image")), origin.Size)
			assert.Equal(t, "application/octet-stream", origin.ContentType)
			assert.Equal(t, int64(len("current-image")), current.Size)
			assert.Equal(t, "application/octet-stream", current.ContentType)

			originData, err := io.ReadAll(origin.File)
			require.NoError(t, err)
			currentData, err := io.ReadAll(current.File)
			require.NoError(t, err)
			assert.Equal(t, "origin-image", string(originData))
			assert.Equal(t, "current-image", string(currentData))
			return "origin-key", "current-key", nil
		},
		createNewFn: func(_ context.Context, originKey string, currentStateKey string) (model.Job, error) {
			assert.Equal(t, "origin-key", originKey)
			assert.Equal(t, "current-key", currentStateKey)
			return model.Job{ID: 42, Status: model.JobStatusCreated, IsEqual: false}, nil
		},
		sendMsgFn: func(_ context.Context, id int, originKey, currentStateKey string) error {
			assert.Equal(t, 42, id)
			assert.Equal(t, "origin-key", originKey)
			assert.Equal(t, "current-key", currentStateKey)
			return nil
		},
	}
	handler := JobHandler{JS: service}
	request := newMultipartRequest(t, func(writer *multipart.Writer) {
		addMultipartFile(t, writer, "origin", "origin.png", []byte("origin-image"))
		addMultipartFile(t, writer, "current", "current.png", []byte("current-image"))
	})
	response := httptest.NewRecorder()

	handler.RecieveImgsHandlerFunc(response, request)

	assert.Equal(t, http.StatusCreated, response.Code)
	assert.Equal(t, "application/json", response.Header().Get("Content-Type"))
	assert.JSONEq(t, `{"id":42,"job_status":"created","is_equal":false}`, response.Body.String())
	assert.Equal(t, 1, service.saveImgsParallelCalls)
	assert.Equal(t, 1, service.createNewCalls)
	assert.Equal(t, 1, service.sendMsgCalls)
}

func TestJobHandler_RecieveImgsHandlerFunc_RejectsInvalidMultipart(t *testing.T) {
	setupTestLogger(t)
	service := &jobServiceMock{}
	handler := JobHandler{JS: service}
	request := httptest.NewRequest(http.MethodPost, "/api/compare", strings.NewReader("not multipart"))
	request.Header.Set("Content-Type", "text/plain")
	response := httptest.NewRecorder()

	handler.RecieveImgsHandlerFunc(response, request)

	assert.Equal(t, http.StatusBadRequest, response.Code)
	assert.Equal(t, "invalid multipart form\n", response.Body.String())
	assert.Zero(t, service.saveImgsParallelCalls)
}

func TestJobHandler_RecieveImgsHandlerFunc_RejectsRequestOverLimit(t *testing.T) {
	setupTestLogger(t)
	service := &jobServiceMock{}
	handler := JobHandler{JS: service}
	request := newMultipartRequest(t, func(writer *multipart.Writer) {
		addMultipartFile(t, writer, "origin", "origin.png", bytes.Repeat([]byte("a"), maxRequestSize))
	})
	response := httptest.NewRecorder()

	handler.RecieveImgsHandlerFunc(response, request)

	assert.Equal(t, http.StatusRequestEntityTooLarge, response.Code)
	assert.Equal(t, "request body is too large\n", response.Body.String())
	assert.Zero(t, service.saveImgsParallelCalls)
}

func TestJobHandler_RecieveImgsHandlerFunc_RejectsMissingCurrentImage(t *testing.T) {
	setupTestLogger(t)
	service := &jobServiceMock{}
	handler := JobHandler{JS: service}
	request := newMultipartRequest(t, func(writer *multipart.Writer) {
		addMultipartFile(t, writer, "origin", "origin.png", []byte("origin-image"))
	})
	response := httptest.NewRecorder()

	handler.RecieveImgsHandlerFunc(response, request)

	assert.Equal(t, http.StatusBadRequest, response.Code)
	assert.Equal(t, "http: no such file\n", response.Body.String())
	assert.Zero(t, service.saveImgsParallelCalls)
}

func TestJobHandler_RecieveImgsHandlerFunc_RejectsThreeImages(t *testing.T) {
	setupTestLogger(t)
	service := &jobServiceMock{}
	handler := JobHandler{JS: service}
	request := newMultipartRequest(t, func(writer *multipart.Writer) {
		addMultipartFile(t, writer, "origin", "origin.png", []byte("origin-image"))
		addMultipartFile(t, writer, "current", "current.png", []byte("current-image"))
		addMultipartFile(t, writer, "current", "extra.png", []byte("extra-image"))
	})
	response := httptest.NewRecorder()

	handler.RecieveImgsHandlerFunc(response, request)

	assert.Equal(t, http.StatusBadRequest, response.Code)
	assert.Equal(t, "exactly two images are required\n", response.Body.String())
	assert.Zero(t, service.saveImgsParallelCalls)
}

func TestJobHandler_RecieveImgsHandlerFunc_RejectsImageOverLimit(t *testing.T) {
	setupTestLogger(t)
	service := &jobServiceMock{}
	handler := JobHandler{JS: service}
	request := newMultipartRequest(t, func(writer *multipart.Writer) {
		addMultipartFile(t, writer, "origin", "origin.png", bytes.Repeat([]byte("a"), maxImageSize+1))
		addMultipartFile(t, writer, "current", "current.png", []byte("current-image"))
	})
	response := httptest.NewRecorder()

	handler.RecieveImgsHandlerFunc(response, request)

	assert.Equal(t, http.StatusBadRequest, response.Code)
	assert.Equal(t, "File: origin is too large\n", response.Body.String())
	assert.Zero(t, service.saveImgsParallelCalls)
}

func TestJobHandler_RecieveImgsHandlerFunc_ReturnsSaveImagesError(t *testing.T) {
	setupTestLogger(t)
	serviceErr := errors.New("save images failed")
	service := &jobServiceMock{
		saveImgsParallelFn: func(context.Context, model.FormFile, model.FormFile) (string, string, error) {
			return "", "", serviceErr
		},
	}
	handler := JobHandler{JS: service}
	request := newMultipartRequest(t, func(writer *multipart.Writer) {
		addMultipartFile(t, writer, "origin", "origin.png", []byte("origin-image"))
		addMultipartFile(t, writer, "current", "current.png", []byte("current-image"))
	})
	response := httptest.NewRecorder()

	handler.RecieveImgsHandlerFunc(response, request)

	assert.Equal(t, http.StatusInternalServerError, response.Code)
	assert.Equal(t, "Images saving error\n", response.Body.String())
	assert.Equal(t, 1, service.saveImgsParallelCalls)
	assert.Zero(t, service.createNewCalls)
	assert.Zero(t, service.sendMsgCalls)
}

func TestJobHandler_RecieveImgsHandlerFunc_ReturnsCreateJobError(t *testing.T) {
	setupTestLogger(t)
	serviceErr := errors.New("create job failed")
	service := &jobServiceMock{
		saveImgsParallelFn: func(context.Context, model.FormFile, model.FormFile) (string, string, error) {
			return "origin-key", "current-key", nil
		},
		createNewFn: func(context.Context, string, string) (model.Job, error) {
			return model.Job{}, serviceErr
		},
		rollbackImgsFn: func(_ context.Context, originKey, currentStateKey string) error {
			assert.Equal(t, "origin-key", originKey)
			assert.Equal(t, "current-key", currentStateKey)
			return nil
		},
	}
	handler := JobHandler{JS: service}
	request := newMultipartRequest(t, func(writer *multipart.Writer) {
		addMultipartFile(t, writer, "origin", "origin.png", []byte("origin-image"))
		addMultipartFile(t, writer, "current", "current.png", []byte("current-image"))
	})
	response := httptest.NewRecorder()

	handler.RecieveImgsHandlerFunc(response, request)

	assert.Equal(t, http.StatusInternalServerError, response.Code)
	assert.Equal(t, "Creating job error\n", response.Body.String())
	assert.Equal(t, 1, service.saveImgsParallelCalls)
	assert.Equal(t, 1, service.createNewCalls)
	assert.Zero(t, service.sendMsgCalls)
	assert.Equal(t, 1, service.rollbackImgsCalls)
}

func TestJobHandler_RecieveImgsHandlerFunc_MarksJobFailedWhenSendingMessageFails(t *testing.T) {
	setupTestLogger(t)
	serviceErr := errors.New("send message failed")
	service := &jobServiceMock{
		saveImgsParallelFn: func(context.Context, model.FormFile, model.FormFile) (string, string, error) {
			return "origin-key", "current-key", nil
		},
		createNewFn: func(context.Context, string, string) (model.Job, error) {
			return model.Job{ID: 42, Status: model.JobStatusCreated}, nil
		},
		sendMsgFn: func(context.Context, int, string, string) error {
			return serviceErr
		},
		rollbackJobFn: func(_ context.Context, id int) (model.Job, error) {
			assert.Equal(t, 42, id)
			return model.Job{ID: id}, nil
		},
	}
	handler := JobHandler{JS: service}
	request := newMultipartRequest(t, func(writer *multipart.Writer) {
		addMultipartFile(t, writer, "origin", "origin.png", []byte("origin-image"))
		addMultipartFile(t, writer, "current", "current.png", []byte("current-image"))
	})
	response := httptest.NewRecorder()

	handler.RecieveImgsHandlerFunc(response, request)

	assert.Equal(t, http.StatusCreated, response.Code)
	assert.Equal(t, "application/json", response.Header().Get("Content-Type"))
	assert.JSONEq(t, `{"id":42,"job_status":"failed","is_equal":false}`, response.Body.String())
	assert.Equal(t, 1, service.saveImgsParallelCalls)
	assert.Equal(t, 1, service.createNewCalls)
	assert.Equal(t, 1, service.sendMsgCalls)
	assert.Equal(t, 1, service.rollbackJobCalls)
}

func TestJobHandler_RecieveImgsHandlerFunc_ReturnsErrorWhenJobRollbackFails(t *testing.T) {
	setupTestLogger(t)
	service := &jobServiceMock{
		saveImgsParallelFn: func(context.Context, model.FormFile, model.FormFile) (string, string, error) {
			return "origin-key", "current-key", nil
		},
		createNewFn: func(context.Context, string, string) (model.Job, error) {
			return model.Job{ID: 42, Status: model.JobStatusCreated}, nil
		},
		sendMsgFn: func(context.Context, int, string, string) error {
			return errors.New("send message failed")
		},
		rollbackJobFn: func(context.Context, int) (model.Job, error) {
			return model.Job{}, errors.New("rollback job failed")
		},
	}
	handler := JobHandler{JS: service}
	request := newMultipartRequest(t, func(writer *multipart.Writer) {
		addMultipartFile(t, writer, "origin", "origin.png", []byte("origin-image"))
		addMultipartFile(t, writer, "current", "current.png", []byte("current-image"))
	})
	response := httptest.NewRecorder()

	handler.RecieveImgsHandlerFunc(response, request)

	assert.Equal(t, http.StatusInternalServerError, response.Code)
	assert.Equal(t, "Msg sending error\n", response.Body.String())
	assert.Equal(t, 1, service.rollbackJobCalls)
}

func TestJobHandler_GetJobStatusHandlerFunc_ReturnsJobStatus(t *testing.T) {
	service := &jobServiceMock{
		getByIDFn: func(_ context.Context, id int) (model.Job, error) {
			assert.Equal(t, 42, id)
			return model.Job{ID: 42, Status: model.JobStatusDone, IsEqual: true}, nil
		},
	}
	handler := JobHandler{JS: service}

	response := performGetJobStatusRequest(handler, "/api/jobs/42")

	assert.Equal(t, http.StatusCreated, response.Code)
	assert.Equal(t, "application/json", response.Header().Get("Content-Type"))
	assert.JSONEq(t, `{"id":42,"job_status":"done","is_equal":true}`, response.Body.String())
	assert.Equal(t, 1, service.getByIDCalls)
}

func TestJobHandler_GetJobStatusHandlerFunc_RejectsNonNumericID(t *testing.T) {
	service := &jobServiceMock{}
	handler := JobHandler{JS: service}

	response := performGetJobStatusRequest(handler, "/api/jobs/not-a-number")

	assert.Equal(t, http.StatusBadRequest, response.Code)
	assert.Equal(t, "Invalid id\n", response.Body.String())
	assert.Zero(t, service.getByIDCalls)
}

func TestJobHandler_GetJobStatusHandlerFunc_RejectsZeroID(t *testing.T) {
	service := &jobServiceMock{}
	handler := JobHandler{JS: service}

	response := performGetJobStatusRequest(handler, "/api/jobs/0")

	assert.Equal(t, http.StatusBadRequest, response.Code)
	assert.Equal(t, "Invalid id\n", response.Body.String())
	assert.Zero(t, service.getByIDCalls)
}

func TestJobHandler_GetJobStatusHandlerFunc_RejectsNegativeID(t *testing.T) {
	service := &jobServiceMock{}
	handler := JobHandler{JS: service}

	response := performGetJobStatusRequest(handler, "/api/jobs/-5")

	assert.Equal(t, http.StatusBadRequest, response.Code)
	assert.Equal(t, "Invalid id\n", response.Body.String())
	assert.Zero(t, service.getByIDCalls)
}

func TestJobHandler_GetJobStatusHandlerFunc_ReturnsServiceError(t *testing.T) {
	setupTestLogger(t)
	serviceErr := errors.New("get job failed")
	service := &jobServiceMock{
		getByIDFn: func(context.Context, int) (model.Job, error) {
			return model.Job{}, serviceErr
		},
	}
	handler := JobHandler{JS: service}

	response := performGetJobStatusRequest(handler, "/api/jobs/42")

	assert.Equal(t, http.StatusInternalServerError, response.Code)
	assert.Equal(t, "Get job error\n", response.Body.String())
	assert.Equal(t, 1, service.getByIDCalls)
}
