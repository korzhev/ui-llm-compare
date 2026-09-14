package config

import (
	"encoding/json"
	"os"
)

type S3Config struct {
	Endpoint        string `json:"endpoint"`
	AccessKeyID     string `json:"access_key_id"`
	SecretAccessKey string `json:"secret_access_key"`
	Region          string `json:"region"`
	Bucket          string `json:"bucket"`
	UseSSL          bool   `json:"use_ssl"`
}

type KafkaConfig struct {
	Broker            string `json:"broker"`
	Topic             string `json:"topic"`
	NumPartitions     int    `json:"partitions"`
	ReplicationFactor int    `json:"replication"`
	GroupID           string `json:"group_id"`
}

type LLMConfig struct {
	URL        string `json:"url"`
	Model      string `json:"model"`
	Token      string `json:"token"`
	JudgePromt string `json:"judge_promt"`
	Timeout    int    `json:"timeout"`
}

type Config struct {
	ServerAddress string      `json:"server_address"`
	LogLevel      string      `json:"log_level"`
	DBDSN         string      `json:"db_dsn"`
	S3            S3Config    `json:"s3"`
	Kafka         KafkaConfig `json:"kafka"`
}

type WorkerConfig struct {
	LogLevel string      `json:"log_level"`
	DBDSN    string      `json:"db_dsn"`
	S3       S3Config    `json:"s3"`
	Kafka    KafkaConfig `json:"kafka"`
	LLM      LLMConfig   `json:"llm"`
}

func ParseConfig[T any](name string) (T, error) {
	var c T
	f, e := os.Open(name)
	if e != nil {
		return c, e
	}
	defer f.Close()

	decoder := json.NewDecoder(f)
	if e := decoder.Decode(&c); e != nil {
		return c, e
	}

	return c, nil
}
