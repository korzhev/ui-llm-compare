package handler

import (
	"encoding/json"
	"errors"
	"mime/multipart"
	"net/http"
	"strconv"

	"github.com/go-chi/chi/v5"
	"github.com/korzhev/ui-llm-compare/internal/logger"
	"github.com/korzhev/ui-llm-compare/internal/model"
	"github.com/korzhev/ui-llm-compare/internal/service"
)

const (
	imagesPerRequest = 2
	maxImageSize     = 10 << 20 // 10 MiB на одно изображение
	maxRequestSize   = 21 << 20 // два изображения и multipart-заголовки
)

type JobHandler struct {
	JS service.IJobService
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
	var originFile *multipart.FileHeader
	var currentStateFile *multipart.FileHeader

	fileCount := 0
	for key, files := range r.MultipartForm.File {
		for _, file := range files {
			logger.Log.Info("____> %v, %v, %v", file.Filename, file.Header, key)
			fileCount++
			if file.Size > maxImageSize {
				http.Error(w, "each image must not exceed 10 MiB", http.StatusRequestEntityTooLarge)
				return
			}
			if key == "origin" {
				originFile = file
			}
			if key == "current" {
				currentStateFile = file
			}
		}
	}

	if fileCount != imagesPerRequest {
		http.Error(w, "exactly two images are required", http.StatusBadRequest)
		return
	}

	originKey, currentStateKey, err := jh.JS.SaveImgsParallel(r.Context(), originFile, currentStateFile)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	job, err := jh.JS.CreateNew(r.Context(), originKey, currentStateKey)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	err = jh.JS.SendMsg(r.Context(), job.ID, originKey, currentStateKey)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	res := model.JobStatusResponse{
		ID:        job.ID,
		JobStatus: job.Status,
		IsEqual:   job.IsEqual,
	}
	resp, err := json.Marshal(res)
	if err != nil {
		logger.Log.Infow("Enccoding response", "error", err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	w.Write(resp)
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
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	res := model.JobStatusResponse{
		ID:        job.ID,
		JobStatus: job.Status,
		IsEqual:   job.IsEqual,
	}
	resp, err := json.Marshal(res)
	if err != nil {
		logger.Log.Infow("Enccoding response", "error", err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	w.Write(resp)
}
