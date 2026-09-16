package deps

import (
	"context"
	"strconv"
	"time"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	"github.com/golang-migrate/migrate/v4/source/iofs"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/segmentio/kafka-go"

	"github.com/korzhev/ui-llm-compare/internal/config"
	"github.com/korzhev/ui-llm-compare/migrations"
)

func InitDBSchema(dsn string) error {
	source, err := iofs.New(migrations.Files, ".")
	if err != nil {
		return err
	}

	m, err := migrate.NewWithSourceInstance(
		"iofs",
		source,
		dsn,
	)
	if err != nil {
		return err
	}

	defer m.Close()
	// migrate.ErrNoChange - fires if there isn't any new migration to run
	if err := m.Up(); err != nil && err != migrate.ErrNoChange {
		return err
	}

	return nil
}

func InitBucket(c config.S3Config) error {
	ctx := context.Background()
	client, err := minio.New(c.Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(c.AccessKeyID, c.SecretAccessKey, ""),
		Secure: c.UseSSL,
	})
	if err != nil {
		return err
	}

	exists, err := client.BucketExists(ctx, c.Bucket)
	if err != nil {
		return err
	}

	if !exists {
		err = client.MakeBucket(ctx, c.Bucket, minio.MakeBucketOptions{
			Region: c.Region,
		})
		if err != nil {
			return err
		}
	}
	return nil
}

func InitKafkaTopic(c config.KafkaConfig) error {

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	brokerConn, err := kafka.DialContext(ctx, "tcp", c.Broker)
	if err != nil {
		return err
	}
	defer brokerConn.Close()

	controller, err := brokerConn.Controller()
	if err != nil {
		return err
	}

	controllerConn, err := kafka.DialContext(ctx, "tcp", controller.Host + ":" + strconv.Itoa(controller.Port))
	if err != nil {
		return err
	}
	defer controllerConn.Close()

	if deadline, ok := ctx.Deadline(); ok {
		if err := controllerConn.SetDeadline(deadline); err != nil {
			return err
		}
	}

	// CreateTopics is idempotent: an existing topic is left unchanged.
	if err := controllerConn.CreateTopics(kafka.TopicConfig{
		Topic:             c.Topic,
		NumPartitions:     c.NumPartitions,
		ReplicationFactor: c.ReplicationFactor,
	}); err != nil {
		return err
	}

	return nil
}
