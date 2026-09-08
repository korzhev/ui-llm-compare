package repository

import (
	"context"
	"database/sql"
	"errors"
	"io"
	"regexp"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/korzhev/ui-llm-compare/internal/model"
	"github.com/minio/minio-go/v7"
	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	getByIDQuery = "SELECT id, origin_key, current_state_key, status, is_equal, details FROM jobs WHERE id = $1 LIMIT 1"
	createQuery  = "INSERT INTO jobs (origin_key, current_state_key, status) VALUES ($1, $2, $3) RETURNING id"
)

type s3ClientMock struct {
	bucketName string
	objectName string
	reader     io.Reader
	objectSize int64
	opts       minio.PutObjectOptions
	removeOpts minio.RemoveObjectOptions
	putErr     error
	removeErr  error
}

func (m *s3ClientMock) PutObject(
	_ context.Context,
	bucketName string,
	objectName string,
	reader io.Reader,
	objectSize int64,
	opts minio.PutObjectOptions,
) (minio.UploadInfo, error) {
	m.bucketName = bucketName
	m.objectName = objectName
	m.reader = reader
	m.objectSize = objectSize
	m.opts = opts

	return minio.UploadInfo{}, m.putErr
}

func (m *s3ClientMock) RemoveObject(
	_ context.Context,
	bucketName string,
	objectName string,
	opts minio.RemoveObjectOptions,
) error {
	m.bucketName = bucketName
	m.objectName = objectName
	m.removeOpts = opts

	return m.removeErr
}

type kafkaWriterMock struct {
	messages []kafka.Message
	writeErr error
	closeErr error
	closed   bool
}

func (m *kafkaWriterMock) WriteMessages(_ context.Context, msgs ...kafka.Message) error {
	m.messages = append(m.messages, msgs...)
	return m.writeErr
}

func (m *kafkaWriterMock) Close() error {
	m.closed = true
	return m.closeErr
}

func newSQLMock(t *testing.T) (*sql.DB, sqlmock.Sqlmock) {
	t.Helper()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	t.Cleanup(func() {
		_ = db.Close()
	})

	return db, mock
}

func TestJobRepository_GetByID_ReturnsJobWithDetails(t *testing.T) {
	db, mock := newSQLMock(t)
	repository := &JobRepository{DB: db}

	mock.ExpectQuery(regexp.QuoteMeta(getByIDQuery)).
		WithArgs(42).
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "origin_key", "current_state_key", "status", "is_equal", "details",
		}).AddRow(42, "origin.png", "current.png", model.JobStatusDone, true, "images are equal"))

	job, err := repository.GetByID(context.Background(), 42)

	require.NoError(t, err)
	assert.Equal(t, model.Job{
		ID:              42,
		OriginKey:       "origin.png",
		CurrentStateKey: "current.png",
		Status:          model.JobStatusDone,
		IsEqual:         true,
		Details:         "images are equal",
	}, job)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestJobRepository_GetByID_HandlesNullDetails(t *testing.T) {
	db, mock := newSQLMock(t)
	repository := &JobRepository{DB: db}

	mock.ExpectQuery(regexp.QuoteMeta(getByIDQuery)).
		WithArgs(7).
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "origin_key", "current_state_key", "status", "is_equal", "details",
		}).AddRow(7, "origin.png", "current.png", model.JobStatusCreated, false, nil))

	job, err := repository.GetByID(context.Background(), 7)

	require.NoError(t, err)
	assert.Equal(t, model.Job{
		ID:              7,
		OriginKey:       "origin.png",
		CurrentStateKey: "current.png",
		Status:          model.JobStatusCreated,
		IsEqual:         false,
		Details:         "",
	}, job)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestJobRepository_GetByID_ReturnsDatabaseError(t *testing.T) {
	db, mock := newSQLMock(t)
	repository := &JobRepository{DB: db}
	dbErr := errors.New("query failed")

	mock.ExpectQuery(regexp.QuoteMeta(getByIDQuery)).
		WithArgs(99).
		WillReturnError(dbErr)

	job, err := repository.GetByID(context.Background(), 99)

	require.ErrorIs(t, err, dbErr)
	assert.Equal(t, model.Job{}, job)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestJobRepository_CreateNew_CreatesJob(t *testing.T) {
	db, mock := newSQLMock(t)
	repository := &JobRepository{DB: db}

	mock.ExpectQuery(regexp.QuoteMeta(createQuery)).
		WithArgs("origin.png", "current.png", model.JobStatusCreated).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(15))

	job, err := repository.CreateNew(context.Background(), "origin.png", "current.png")

	require.NoError(t, err)
	assert.Equal(t, model.Job{
		ID:              15,
		OriginKey:       "origin.png",
		CurrentStateKey: "current.png",
		Status:          model.JobStatusCreated,
		IsEqual:         false,
	}, job)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestJobRepository_CreateNew_ReturnsDatabaseError(t *testing.T) {
	db, mock := newSQLMock(t)
	repository := &JobRepository{DB: db}
	dbErr := errors.New("insert failed")

	mock.ExpectQuery(regexp.QuoteMeta(createQuery)).
		WithArgs("origin.png", "current.png", model.JobStatusCreated).
		WillReturnError(dbErr)

	job, err := repository.CreateNew(context.Background(), "origin.png", "current.png")

	require.ErrorIs(t, err, dbErr)
	assert.Equal(t, model.Job{
		OriginKey:       "origin.png",
		CurrentStateKey: "current.png",
		Status:          model.JobStatusCreated,
		IsEqual:         false,
	}, job)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestJobRepository_SaveImg_UploadsImage(t *testing.T) {
	s3 := &s3ClientMock{}
	repository := &JobRepository{S3: s3, Bucket: "screenshots"}
	reader := strings.NewReader("image data")

	err := repository.SaveImg(context.Background(), "jobs/42/origin.png", reader, "image/png", 10)

	require.NoError(t, err)
	assert.Equal(t, "screenshots", s3.bucketName)
	assert.Equal(t, "jobs/42/origin.png", s3.objectName)
	assert.Same(t, reader, s3.reader)
	assert.Equal(t, int64(10), s3.objectSize)
	assert.Equal(t, "image/png", s3.opts.ContentType)
}

func TestJobRepository_SaveImg_ReturnsS3Error(t *testing.T) {
	s3Err := errors.New("s3 upload failed")
	s3 := &s3ClientMock{putErr: s3Err}
	repository := &JobRepository{S3: s3, Bucket: "screenshots"}

	err := repository.SaveImg(context.Background(), "origin.png", strings.NewReader("data"), "image/png", 4)

	require.ErrorIs(t, err, s3Err)
}

func TestJobRepository_DeleteImg_DeletesImage(t *testing.T) {
	s3 := &s3ClientMock{}
	repository := &JobRepository{S3: s3, Bucket: "screenshots"}

	err := repository.DeleteImg(context.Background(), "jobs/42/origin.png")

	require.NoError(t, err)
	assert.Equal(t, "screenshots", s3.bucketName)
	assert.Equal(t, "jobs/42/origin.png", s3.objectName)
	assert.Equal(t, minio.RemoveObjectOptions{}, s3.removeOpts)
}

func TestJobRepository_DeleteImg_ReturnsS3Error(t *testing.T) {
	s3Err := errors.New("s3 delete failed")
	s3 := &s3ClientMock{removeErr: s3Err}
	repository := &JobRepository{S3: s3, Bucket: "screenshots"}

	err := repository.DeleteImg(context.Background(), "origin.png")

	require.ErrorIs(t, err, s3Err)
}

func TestJobRepository_SendMsg_WritesJSONMessage(t *testing.T) {
	writer := &kafkaWriterMock{}
	repository := &JobRepository{KWriter: writer}
	value := model.JobKafkaMsg{
		ID:              42,
		OriginKey:       "origin.png",
		CurrentStateKey: "current.png",
	}

	err := repository.SendMsg(context.Background(), 42, value)

	require.NoError(t, err)
	require.Len(t, writer.messages, 1)
	message := writer.messages[0]
	assert.Equal(t, []byte("42"), message.Key)
	assert.JSONEq(t, `{"id":42,"origin_key":"origin.png","current_state_key":"current.png"}`, string(message.Value))
	require.Len(t, message.Headers, 1)
	assert.Equal(t, "content-type", message.Headers[0].Key)
	assert.Equal(t, []byte("application/json"), message.Headers[0].Value)
}

func TestJobRepository_SendMsg_ReturnsKafkaError(t *testing.T) {
	kafkaErr := errors.New("kafka write failed")
	writer := &kafkaWriterMock{writeErr: kafkaErr}
	repository := &JobRepository{KWriter: writer}

	err := repository.SendMsg(context.Background(), 9, model.JobKafkaMsg{ID: 9})

	require.ErrorIs(t, err, kafkaErr)
	require.Len(t, writer.messages, 1)
}

func TestJobRepository_Close_ClosesKafkaAndDatabase(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	writer := &kafkaWriterMock{}
	repository := &JobRepository{DB: db, KWriter: writer}
	mock.ExpectClose()

	err = repository.Close()

	require.NoError(t, err)
	assert.True(t, writer.closed)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestJobRepository_Close_JoinsKafkaAndDatabaseErrors(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	kafkaErr := errors.New("kafka close failed")
	dbErr := errors.New("database close failed")
	writer := &kafkaWriterMock{closeErr: kafkaErr}
	repository := &JobRepository{DB: db, KWriter: writer}
	mock.ExpectClose().WillReturnError(dbErr)

	err = repository.Close()

	require.Error(t, err)
	assert.ErrorIs(t, err, kafkaErr)
	assert.ErrorIs(t, err, dbErr)
	assert.True(t, writer.closed)
	require.NoError(t, mock.ExpectationsWereMet())
}
