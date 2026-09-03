package service

import (
	"context"
	"io"
	"mime/multipart"
	"sync"

	"github.com/google/uuid"
	"github.com/korzhev/ui-llm-compare/internal/model"
	"github.com/korzhev/ui-llm-compare/internal/repository"
)

type IJobService interface {
	GetByID(ctx context.Context, id int) (model.Job, error)
	CreateNew(ctx context.Context, origiKey string, currentStateKey string) (model.Job, error)
	SaveImg(ctx context.Context, reader io.Reader, contentType string, size int64) (string, error)
	GetFileInfo(mfile *multipart.FileHeader) (ct string, size int64)
	SaveImgsParallel(ctx context.Context, or *multipart.FileHeader, csr *multipart.FileHeader) (string, string, error)
	SendMsg(ctx context.Context, id int, originKey, currentStateKey string) error
}

type JobService struct {
	Repo repository.IJobRepository
}

func (js JobService) SaveImg(ctx context.Context, reader io.Reader, contentType string, size int64) (string, error) {
	key := uuid.NewString()
	err := js.Repo.SaveImg(ctx, key, reader, contentType, size)
	if err != nil {
		return "", err
	}
	return key, nil
}

func (js JobService) CreateNew(ctx context.Context, origiKey string, currentStateKey string) (model.Job, error) {
	return js.Repo.CreateNew(ctx, origiKey, currentStateKey)
}

func (js JobService) GetByID(ctx context.Context, id int) (model.Job, error) {
	return js.Repo.GetByID(ctx, id)
}

func (js JobService) GetFileInfo(mfile *multipart.FileHeader) (ct string, size int64) {
	ct = mfile.Header.Get("Content-Type")

	if ct == "" {
		ct = "application/octet-stream"
	}
	size = mfile.Size
	return
}

const (
	originKeyType = iota
	currentKeyType
)

type ImgResult struct {
	KeyType int
	Key     string
	Error   error
}

func (js JobService) SaveImgsParallel(ctx context.Context, or *multipart.FileHeader, csr *multipart.FileHeader) (string, string, error) {
	resultCh := make(chan ImgResult, 2)
	var err error
	var originKey, currentStateKey string
	fhMap := map[int]*multipart.FileHeader{
		originKeyType:  or,
		currentKeyType: csr,
	}

	var wg sync.WaitGroup

	for kt, fh := range fhMap {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ct, size := js.GetFileInfo(fh)
			file, err := fh.Open()
			if err != nil {
				resultCh <- ImgResult{
					KeyType: kt,
					Key:     "",
					Error:   err,
				}
				return
			}
			key, err := js.SaveImg(ctx, file, ct, size)
			closeErr := file.Close()
			if closeErr != nil {
				resultCh <- ImgResult{
					KeyType: kt,
					Key:     "",
					Error:   closeErr,
				}
				return
			}
			resultCh <- ImgResult{
				KeyType: kt,
				Key:     key,
				Error:   err,
			}
		}()
	}
	go func() {
		wg.Wait()
		close(resultCh)
	}()

	for res := range resultCh {
		if res.Error != nil {
			err = res.Error
		}
		if res.KeyType == originKeyType {
			originKey = res.Key
		} else {
			currentStateKey = res.Key
		}
	}
	return originKey, currentStateKey, err
}

func (js JobService) SendMsg(ctx context.Context, id int, originKey, currentStateKey string) error {
	value := model.JobKafkaMsg{
		ID:              id,
		OriginKey:       originKey,
		CurrentStateKey: currentStateKey,
	}
	return js.Repo.SendMsg(ctx, id, value)
}
