package worker

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
)

type Worker interface {
	start() error
	stop() error
	run(data any) (*WorkerResponse, error)
	workload() int
}

type WorkerConfig struct {
	timeout time.Duration
}

type WorkerData struct {
	Id   uuid.UUID `json:"id"`
	Data any       `json:"data"`
}

type WorkerResponse struct {
	Data  string
	Error error
}

type workerTask struct {
	id       uuid.UUID
	data     []byte
	timeout  <-chan time.Time
	response chan *WorkerResponse
}

type workerBase struct {
	timeout time.Duration
	queue   *workerQueue
}

func newWorkerBase(config WorkerConfig) *workerBase {
	timeout := config.timeout
	if timeout == 0 {
		timeout = 3000
	}
	return &workerBase{
		timeout: timeout,
		queue:   newWorkerQueue(),
	}
}

func (w *workerBase) workload() int {
	return len(w.queue.tasks)
}

func (w *workerBase) run(data any, cb func(task *workerTask) error) (*WorkerResponse, error) {
	id, err := uuid.NewV7()
	if err != nil {
		return nil, fmt.Errorf("creating uuid: %w", err)
	}
	jsonData, err := json.Marshal(WorkerData{
		Id:   id,
		Data: data,
	})
	if err != nil {
		return nil, fmt.Errorf("creating json data: %w", err)
	}
	response := make(chan *WorkerResponse, 1)
	t := &workerTask{
		id:       id,
		data:     jsonData,
		response: response,
		timeout:  time.After(w.timeout * time.Millisecond),
	}
	w.queue.add(id, t)
	slog.Info("Sending data", "id", id, "data", data)
	err = cb(t)
	if err != nil {
		return nil, err
	}

	select {
	case res := <-t.response:
		slog.Info("Task completet")
		return res, nil
	case <-t.timeout:
		slog.Warn("Task timeout")
		w.queue.delete(id)
		return &WorkerResponse{
			Error: fmt.Errorf("timeout"),
		}, err
	}
}

func (w *workerBase) resolve(message []byte, isError bool) {
	response := &WorkerResponse{}

	data := &WorkerData{}
	err := json.Unmarshal(message, data)
	if err != nil {
		slog.Error("error parsing message")
		return
	}
	if data.Id == uuid.Nil {
		slog.Error("invalid message id")
		return
	}

	t := w.queue.get(data.Id)
	if t != nil {
		w.queue.delete(data.Id)
		t.response <- response
	} else {
		slog.Error("queue entry not found", "id", data.Id)
	}
}
