package handler

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"mime/multipart"
	"net/http"
	"strconv"

	"github.com/go-chi/chi/v5"
	"github.com/korzhev/ui-llm-compare/internal/logger"
	"github.com/korzhev/ui-llm-compare/internal/model"
)

const (
	imagesPerRequest = 2
	maxImageSize     = 10 << 20 // 10 MiB на одно изображение
	maxRequestSize   = 21 << 20 // два изображения и multipart-заголовки
)

type JobService interface {
	GetByID(ctx context.Context, id int) (model.Job, error)
	CreateNew(ctx context.Context, origiKey string, currentStateKey string) (model.Job, error)
	SaveImgsParallel(ctx context.Context, or model.FormFile, csr model.FormFile) (string, string, error)
	SendMsg(ctx context.Context, id int, originKey, currentStateKey string) error
	RollbackJob(ctx context.Context, id int) (model.Job, error)
	RollbackImgs(ctx context.Context, ok, csk string) error
}

type FileSizeError struct {
	Name string
}

func (e *FileSizeError) Error() string {
	return fmt.Sprintf("File: %s is too large", e.Name)
}

type JobHandler struct {
	JS JobService
}

func getFile(r *http.Request, name string, maxImageSize int64) (multipart.File, *multipart.FileHeader, error) {
	originFile, originHeader, err := r.FormFile(name)
	if err != nil {
		return originFile, originHeader, err
	}
	if originHeader.Size > maxImageSize {
		return originFile, originHeader, &FileSizeError{Name: name}
	}
	return originFile, originHeader, nil
}

func (jh JobHandler) RecieveImgsHandlerFunc(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, maxRequestSize)

	if err := r.ParseMultipartForm(maxImageSize); err != nil {
		var maxBytesError *http.MaxBytesError
		if errors.As(err, &maxBytesError) {
			http.Error(w, "request body is too large", http.StatusRequestEntityTooLarge)
			return
		}

		http.Error(w, "invalid multipart form", http.StatusBadRequest)
		return
	}
	defer r.MultipartForm.RemoveAll()
	originFile, originHeader, err := getFile(r, "origin", maxImageSize)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	currentStateFile, currentStateHeader, err := getFile(r, "current", maxImageSize)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	fileCount := 0
	for _, files := range r.MultipartForm.File {
		fileCount += len(files)
	}

	if fileCount != imagesPerRequest {
		http.Error(w, "exactly two images are required", http.StatusBadRequest)
		return
	}

	originKey, currentStateKey, err := jh.JS.SaveImgsParallel(
		r.Context(),
		model.FormFile{
			Size:        originHeader.Size,
			ContentType: originHeader.Header.Get("Content-Type"),
			File:        originFile,
		},
		model.FormFile{
			Size:        currentStateHeader.Size,
			ContentType: currentStateHeader.Header.Get("Content-Type"),
			File:        currentStateFile,
		},
	)
	if err != nil {
		logger.Log.Infow("Images saving error", "error", err)
		http.Error(w, "Images saving error", http.StatusInternalServerError)
		return
	}

	job, err := jh.JS.CreateNew(r.Context(), originKey, currentStateKey)
	if err != nil {
		logger.Log.Infow("Creating job error", "error", err)
		errRI := jh.JS.RollbackImgs(r.Context(), originKey, currentStateKey)
		if errRI != nil {
			logger.Log.Infow("Rollback Images error", "error", errRI)
		}
		http.Error(w, "Creating job error", http.StatusInternalServerError)
		return
	}

	err = jh.JS.SendMsg(r.Context(), job.ID, originKey, currentStateKey)
	if err != nil {
		logger.Log.Infow("Msg sending error", "error", err)
		_, errRJ := jh.JS.RollbackJob(r.Context(), job.ID)
		if errRJ != nil {
			logger.Log.Infow("Rollback Job error", "error", errRJ)
			http.Error(w, "Msg sending error", http.StatusInternalServerError)
			return
		}
		job.Status = model.JobStatusFailed
	}

	toJSON(job, w)
}

func (jh JobHandler) GetJobStatusHandlerFunc(w http.ResponseWriter, r *http.Request) {
	idString := chi.URLParam(r, "id")

	id, err := strconv.Atoi(idString)
	if err != nil || id <= 0 {
		http.Error(w, "Invalid id", http.StatusBadRequest)
		return
	}
	job, err := jh.JS.GetByID(r.Context(), id)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			http.Error(w, fmt.Sprintf("Job with id: %v not found", id), http.StatusNotFound)
			return
		}
		logger.Log.Infow("Get job error", "error", err)
		http.Error(w, "Get job error", http.StatusInternalServerError)
		return
	}

	toJSON(job, w)
}

func toJSON(job model.Job, w http.ResponseWriter) {
	res := model.JobStatusResponse{
		ID:        job.ID,
		JobStatus: job.Status,
		IsEqual:   job.IsEqual,
	}
	resp, err := json.Marshal(res)
	if err != nil {
		logger.Log.Infow("Enccoding response", "error", err)
		http.Error(w, "Internal error", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	w.Write(resp)
}
