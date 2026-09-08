package repository

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"strconv"

	"github.com/korzhev/ui-llm-compare/internal/config"
	"github.com/korzhev/ui-llm-compare/internal/model"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/segmentio/kafka-go"
)

type KafkaWriter interface {
	WriteMessages(ctx context.Context, msgs ...kafka.Message) error
	Close() error
}

type S3Client interface {
	PutObject(
		ctx context.Context,
		bucketName string,
		objectName string,
		reader io.Reader,
		objectSize int64,
		opts minio.PutObjectOptions,
	) (minio.UploadInfo, error)
	RemoveObject(
		ctx context.Context,
		bucketName string,
		objectName string,
		opts minio.RemoveObjectOptions,
	) error
}

type JobRepository struct {
	DB      *sql.DB
	S3      S3Client
	Bucket  string
	KWriter KafkaWriter
	// S3      minio.Client
}

func (d *JobRepository) GetByID(ctx context.Context, id int) (model.Job, error) {
	row := d.DB.QueryRowContext(ctx, "SELECT id, origin_key, current_state_key, status, is_equal, details FROM jobs WHERE id = $1 LIMIT 1", id)
	j := model.Job{}
	// нельзя конвертнуть NULL в строку для pg простотак
	var details *string
	err := row.Scan(&j.ID, &j.OriginKey, &j.CurrentStateKey, &j.Status, &j.IsEqual, &details)
	if details != nil {
		j.Details = *details
	}
	return j, err
}

func (d *JobRepository) CreateNew(ctx context.Context, origiKey string, currentStateKey string) (model.Job, error) {
	j := model.Job{
		OriginKey:       origiKey,
		CurrentStateKey: currentStateKey,
		Status:          model.JobStatusCreated,
		IsEqual:         false,
	}
	var lid int64
	err := d.DB.QueryRowContext(
		ctx,
		"INSERT INTO jobs (origin_key, current_state_key, status) VALUES ($1, $2, $3) RETURNING id",
		origiKey,
		currentStateKey,
		model.JobStatusCreated,
	).Scan(&lid)
	if err != nil {
		return j, err
	}
	j.ID = int(lid)
	return j, err
}

func (d *JobRepository) FailJob(ctx context.Context, id int) error {
	_, err := d.DB.ExecContext(ctx, "UPDATE jobs SET status = 'failed' WHERE id = $1", id)
	if err != nil {
		return err
	}
	return nil
}

func (d *JobRepository) Close() error {
	return errors.Join(
		d.KWriter.Close(),
		d.DB.Close(),
	)
}

func (d *JobRepository) SaveImg(ctx context.Context, key string, reader io.Reader, contentType string, size int64) error {
	_, err := d.S3.PutObject(ctx,
		d.Bucket,
		key,
		reader,
		size,
		minio.PutObjectOptions{
			ContentType: contentType,
		},
	)
	return err
}

func (d *JobRepository) DeleteImg(ctx context.Context, key string) error {
	err := d.S3.RemoveObject(ctx,
		d.Bucket,
		key,
		minio.RemoveObjectOptions{},
	)
	return err
}

func (d *JobRepository) SendMsg(ctx context.Context, id int, value model.JobKafkaMsg) error {
	valueStr, err := json.Marshal(value)
	if err != nil {
		return err
	}
	msg := kafka.Message{
		Key:   []byte(strconv.Itoa(id)),
		Value: []byte(valueStr),
		Headers: []kafka.Header{
			{
				Key:   "content-type",
				Value: []byte("application/json"),
			},
		},
	}
	return d.KWriter.WriteMessages(ctx, msg)
}

func NewJobRepository(dsn string, s3 config.S3Config, kc config.KafkaConfig) (*JobRepository, error) {
	pg, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, err
	}
	client, err := minio.New(s3.Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(s3.AccessKeyID, s3.SecretAccessKey, ""),
		Secure: s3.UseSSL,
	})
	if err != nil {
		return nil, err
	}

	writer := &kafka.Writer{
		Addr:         kafka.TCP(kc.Broker),
		Topic:        kc.Topic,
		Balancer:     &kafka.Hash{},
		RequiredAcks: kafka.RequireAll,
	}

	return &JobRepository{
		DB:      pg,
		S3:      client,
		Bucket:  s3.Bucket,
		KWriter: writer,
	}, nil
}
