package worker

import (
	"context"
	"errors"
	"fmt"
)

type Pool struct {
	running bool
	ctx     context.Context
	cancel  context.CancelFunc
	worker  []Worker
}

func NewPool() *Pool {
	ctx, cancel := context.WithCancel(context.Background())
	p := &Pool{
		ctx:    ctx,
		cancel: cancel,
		worker: []Worker{},
	}
	return p
}

func (p *Pool) Add(num int, config ProcessConfig) {
	for range num {
		p.worker = append(p.worker, newProcessWorker(p.ctx, config))
	}
}

func (p *Pool) Start() error {
	if p.running {
		return fmt.Errorf("pool already running")
	}
	p.running = true
	for _, w := range p.worker {
		err := w.start()
		if err != nil {
			return fmt.Errorf("running worker: %w", err)
		}
	}
	return nil
}

func (p *Pool) Stop() error {
	errs := []error{}
	for _, w := range p.worker {
		err := w.stop()
		if err != nil {
			errs = append(errs, err)
		}
	}

	if len(errs) != 0 {
		return errors.Join(errs...)
	}
	return nil
}

func (p *Pool) Run(data any) (*WorkerResponse, error) {
	w := p.getWorker()
	return w.run(data, 3000)
}

func (p *Pool) getWorker() Worker {
	var worker Worker
	for _, w := range p.worker {
		if worker == nil || worker.workload() < w.workload() {
			worker = w
		}
	}
	return worker
}
