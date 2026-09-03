package handler

import (
	"bytes"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestRecieveImgsHandlerFunc(t *testing.T) {
	tests := []struct {
		name       string
		fileSizes  []int
		wantStatus int
	}{
		{
			name:       "accepts two images",
			fileSizes:  []int{1024, 2048},
			wantStatus: http.StatusNoContent,
		},
		{
			name:       "accepts two images at size limit",
			fileSizes:  []int{maxImageSize, maxImageSize},
			wantStatus: http.StatusNoContent,
		},
		{
			name:       "rejects image over size limit",
			fileSizes:  []int{maxImageSize + 1, 1024},
			wantStatus: http.StatusRequestEntityTooLarge,
		},
		{
			name:       "rejects one image",
			fileSizes:  []int{1024},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "rejects three images",
			fileSizes:  []int{1024, 1024, 1024},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var body bytes.Buffer
			writer := multipart.NewWriter(&body)
			for i, size := range tt.fileSizes {
				part, err := writer.CreateFormFile("images", "image.jpg")
				if err != nil {
					t.Fatalf("create multipart file %d: %v", i, err)
				}
				if _, err := part.Write(bytes.Repeat([]byte{'a'}, size)); err != nil {
					t.Fatalf("write multipart file %d: %v", i, err)
				}
			}
			if err := writer.Close(); err != nil {
				t.Fatalf("close multipart writer: %v", err)
			}

			req := httptest.NewRequest(http.MethodPost, "/api/compare", &body)
			req.Header.Set("Content-Type", writer.FormDataContentType())
			response := httptest.NewRecorder()

			JobHandler{}.RecieveImgsHandlerFunc(response, req)

			if response.Code != tt.wantStatus {
				t.Errorf("status = %d, want %d; body = %q", response.Code, tt.wantStatus, response.Body.String())
			}
		})
	}
}

func TestRecieveImgsHandlerFuncRejectsInvalidMultipart(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "/api/compare", strings.NewReader("not multipart"))
	req.Header.Set("Content-Type", "text/plain")
	response := httptest.NewRecorder()

	JobHandler{}.RecieveImgsHandlerFunc(response, req)

	if response.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want %d", response.Code, http.StatusBadRequest)
	}
}
