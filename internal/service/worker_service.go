package service

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"

	"github.com/korzhev/ui-llm-compare/internal/model"
	"github.com/segmentio/kafka-go"
)

type WorkerRepository interface {
	FormatLLMRequest(promt, mimeTypeOrigin, mimeType, base64ImgOrigin, base64Img string) ([]byte, error)
	CallLLM(ctx context.Context, msg []byte) (bool, string, error)
	CommitMsg(ctx context.Context, msg kafka.Message) error
	FetchMsg(ctx context.Context) (kafka.Message, error)
}

type WorkerService struct {
	WorkerRepo WorkerRepository
	Repo       JobRepository
}

func (s WorkerService) CheckImgs(ctx context.Context, ok, csk string) (bool, string, error) {
	mtOK, imgOK, err := s.Repo.GetImage(ctx, ok)
	if err != nil {
		return false, "", err
	}
	mtCSK, imgCSK, err := s.Repo.GetImage(ctx, csk)
	if err != nil {
		return false, "", err
	}
	base64OK := base64.StdEncoding.EncodeToString(imgOK)
	base64CSK := base64.StdEncoding.EncodeToString(imgCSK)

	msg, err := s.WorkerRepo.FormatLLMRequest("TODO", mtOK, mtCSK, base64OK, base64CSK)
	if err != nil {
		return false, "", err
	}

	return s.WorkerRepo.CallLLM(ctx, msg)
}

func (s WorkerService) GetImgKeys(ctx context.Context, id int) (string, string, error) {
	job, err := s.Repo.GetByID(ctx, id)
	if err != nil {
		return "", "", err
	}
	return job.OriginKey, job.CurrentStateKey, nil
}

func (s WorkerService) RunJob(ctx context.Context, msg model.JobKafkaMsg) error {
	err := s.Repo.StartJob(ctx, msg.ID)
	if err != nil {
		return err
	}
	isEqual, reason, err := s.CheckImgs(ctx, msg.OriginKey, msg.CurrentStateKey)
	if err != nil {
		return err
	}
	err = s.Repo.SaveJobResult(ctx, msg.ID, isEqual, reason)
	if err != nil {
		errf := s.Repo.FailJob(ctx, msg.ID)
		return errors.Join(err, errf)
	}
	return nil
}

func (s WorkerService) ProcessMsg(ctx context.Context, msg kafka.Message) error {
	var j model.JobKafkaMsg
	v := msg.Value
	if err := json.Unmarshal(v, &j); err != nil {
		return err
	}
	if err := s.RunJob(ctx, j); err != nil {
		return err
	}

	return s.WorkerRepo.CommitMsg(ctx, msg)
}

func (s WorkerService) FetchMsg(ctx context.Context) (kafka.Message, error) {
	return s.WorkerRepo.FetchMsg(ctx)
}
