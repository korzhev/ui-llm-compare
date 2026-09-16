package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/korzhev/ui-llm-compare/internal/config"
	"github.com/korzhev/ui-llm-compare/internal/logger"
	"github.com/korzhev/ui-llm-compare/internal/repository"
	"github.com/korzhev/ui-llm-compare/internal/service"
)

func main() {
	c, err := config.ParseConfig[config.WorkerConfig]("config.json")
	if err != nil {
		fmt.Printf("Error starting server: %s\n", err)
		return
	}
	logger.InitLogger(c.LogLevel)
	defer logger.Log.Sync()

	jr, err := repository.NewJobRepository(c.DBDSN, c.S3, c.Kafka)
	if err != nil {
		logger.Log.Fatalw(
			"Failed to create repository",
			"error", err,
		)
	}
	defer jr.Close()
	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()

	lr := repository.NewLLMRepository(ctx, c.Kafka, c.LLM)
	defer lr.Close()

	for i := 0; i < 4; i++ {
		go func(i int) {
			s := service.WorkerService{
				WorkerRepo: lr,
				Repo:       jr,
			}
			logger.Log.Infof("Worker #%v is running", i)
			for {
				msg, err := s.FetchMsg(ctx)
				if err != nil {
					if errors.Is(err, context.Canceled) {
						break
					}
					logger.Log.Errorf("Error fetching msg: %v\n", err)
					continue
				}
				logger.Log.Infof("Msg(%v): %v", i, msg)
				if err := s.ProcessMsg(ctx, msg); err != nil {
					logger.Log.Errorf("Error processing msg(%v): %v\n", i, err)
				}
			}
		}(i)
	}

	logger.Log.Infof("Workerpool is running")
}
