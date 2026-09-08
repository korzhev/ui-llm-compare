package service

import (
	"context"
	"errors"
	"io"

	"github.com/google/uuid"
	"github.com/korzhev/ui-llm-compare/internal/model"
	"golang.org/x/sync/errgroup"
)

type JobRepository interface {
	GetByID(ctx context.Context, id int) (model.Job, error)
	CreateNew(ctx context.Context, origiKey string, currentStateKey string) (model.Job, error)
	SaveImg(ctx context.Context, key string, reader io.Reader, contentType string, size int64) error
	SendMsg(ctx context.Context, id int, value model.JobKafkaMsg) error
	FailJob(ctx context.Context, id int) error
	DeleteImg(ctx context.Context, key string) error
}

type JobService struct {
	Repo JobRepository
}

func (js JobService) SaveImg(ctx context.Context, reader io.Reader, contentType string, size int64, key string) error {
	if contentType == "" {
		contentType = "application/octet-stream"
	}

	err := js.Repo.SaveImg(ctx, key, reader, contentType, size)
	if err != nil {
		return err
	}
	return nil
}

func (js JobService) CreateNew(ctx context.Context, origiKey string, currentStateKey string) (model.Job, error) {
	return js.Repo.CreateNew(ctx, origiKey, currentStateKey)
}

func (js JobService) GetByID(ctx context.Context, id int) (model.Job, error) {
	return js.Repo.GetByID(ctx, id)
}

func (js JobService) SaveImgsParallel(ctx context.Context, or model.FormFile, csr model.FormFile) (string, string, error) {
	keyOrigin := uuid.NewString()
	keyCurrentState := uuid.NewString()
	g, gCtx := errgroup.WithContext(ctx)
	g.Go(func() error {
		err := js.SaveImg(gCtx, or.File, or.ContentType, or.Size, keyOrigin)
		return err
	})
	g.Go(func() error {
		err := js.SaveImg(gCtx, csr.File, csr.ContentType, csr.Size, keyCurrentState)
		return err
	})

	err := g.Wait()

	return keyOrigin, keyCurrentState, err
}

func (js JobService) SendMsg(ctx context.Context, id int, originKey, currentStateKey string) error {
	value := model.JobKafkaMsg{
		ID:              id,
		OriginKey:       originKey,
		CurrentStateKey: currentStateKey,
	}
	return js.Repo.SendMsg(ctx, id, value)
}

func (js JobService) RollbackJob(ctx context.Context, id int) (model.Job, error) {
	job, errGI := js.Repo.GetByID(ctx, id)
	var errRI error
	if errGI == nil {
		errRI = js.RollbackImgs(ctx, job.OriginKey, job.CurrentStateKey)
	}
	errFJ := js.Repo.FailJob(ctx, id)
	return job, errors.Join(errGI, errFJ, errRI)
}

func (js JobService) RollbackImgs(ctx context.Context, ok, csk string) error {
	errD1 := js.Repo.DeleteImg(ctx, ok)
	errD2 := js.Repo.DeleteImg(ctx, csk)
	return errors.Join(errD1, errD2)
}
