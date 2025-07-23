package adapter

import (
	"github.com/ssr-adapter/go/core/internal/worker"
)

type AdapterConfig struct {
	Cache  bool
	Worker int
}

type Adapter struct {
	config     *AdapterConfig
	workerPool *worker.Pool
}

func NewAdapter(config *AdapterConfig) (*Adapter, error) {
	a := &Adapter{
		config:     config,
		workerPool: worker.NewPool(),
	}

	a.workerPool.Add(config.Worker, worker.ProcessConfig{Command: "node", Args: []string{"--experimental-strip-types", "./test.ts"}})
	err := a.Start()
	if err != nil {
		return nil, err
	}
	return a, nil
}

func (a *Adapter) Start() error {
	return a.workerPool.Start()
}

func (a *Adapter) Stop() error {
	return a.workerPool.Stop()
}

func (a *Adapter) Render(data any) (string, error) {
	response, err := a.workerPool.Run(data)
	if err != nil {
		return "", err
	}
	if response.Error != nil {
		return "", err
	}
	return "", nil
}
