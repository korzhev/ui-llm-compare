package main

import (
	"net/http"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/go-chi/chi/v5"
	chiMW "github.com/go-chi/chi/v5/middleware"

	"github.com/korzhev/ui-llm-compare/internal/config"
	"github.com/korzhev/ui-llm-compare/internal/config/deps"
	"github.com/korzhev/ui-llm-compare/internal/handler"
	"github.com/korzhev/ui-llm-compare/internal/logger"
	"github.com/korzhev/ui-llm-compare/internal/repository"
	"github.com/korzhev/ui-llm-compare/internal/service"
)

func RootRouter(c config.Config, repo *repository.JobRepository) chi.Router {
	var JobHandler = handler.JobHandler{
		JS: service.JobService{
			Repo: repo,
		},
	}

	r := chi.NewRouter()

	r.Use(chiMW.Logger)
	r.Use(chiMW.Compress(5, "text/html", "text/css", "application/json"))
	r.Use(chiMW.CleanPath)
	r.Use(chiMW.RedirectSlashes)
	r.Use(chiMW.Recoverer)

	r.Get("/api/jobs/{id}", JobHandler.GetJobStatusHandlerFunc)
	r.Post("/api/compare", JobHandler.RecieveImgsHandlerFunc)
	return r
}

func main() {
	c, err := config.ParseConfig("config.json")
	if err != nil {
		logger.Log.Errorf("Error starting server: %s\n", err)
		return
	}
	logger.InitLogger(c.LogLevel)
	defer logger.Log.Sync()
	if err := deps.InitDBSchema(c.DBDSN); err != nil {
		logger.Log.Fatalw(
			"Failed to apply database migrations",
			"error", err,
		)
	}
	if err := deps.InitBucket(c.S3); err != nil {
		logger.Log.Fatalw(
			"Failed to create bucket",
			"error", err,
		)
	}

	if err := deps.InitKafkaTopic(c.Kafka); err != nil {
		logger.Log.Fatalw(
			"Failed to create kafka topic",
			"error", err,
		)
	}

	logger.Log.Infow("Server starting with params",
		"address", c.ServerAddress,
	)

	repo, err := repository.NewJobRepository(c.DBDSN, c.S3, c.Kafka)
	if err != nil {
		logger.Log.Fatalw(
			"Failed to create repository",
			"error", err,
		)
	}
	defer repo.Close()

	r := RootRouter(c, repo)
	err = http.ListenAndServe(c.ServerAddress, r)
	if err != nil {
		logger.Log.Errorf("Error starting server: %s\n", err)
	}
	logger.Log.Infof("Server is running on: %s", c.ServerAddress)
}
