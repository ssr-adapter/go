package worker

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log/slog"
	"os/exec"
	"sync"
	"time"
)

type ProcessConfig struct {
	WorkerConfig
	Command string
	Args    []string
}

type processWorker struct {
	workerBase
	config    ProcessConfig
	cmd       *exec.Cmd
	stdin     io.WriteCloser
	stdout    io.ReadCloser
	stderr    io.ReadCloser
	ctx       context.Context
	cancel    context.CancelFunc
	waitGroup sync.WaitGroup
}

func newProcessWorker(pctx context.Context, config ProcessConfig) *processWorker {
	ctx, cancel := context.WithCancel(pctx)
	return &processWorker{
		workerBase: *newWorkerBase(config.WorkerConfig),
		config:     config,
		ctx:        ctx,
		cancel:     cancel,
		waitGroup:  sync.WaitGroup{},
	}
}

func (w *processWorker) start() error {
	cmd := exec.CommandContext(w.ctx, w.config.Command, w.config.Args...)

	stdin, err := cmd.StdinPipe()
	if err != nil {
		w.cancel()
		return fmt.Errorf("failed to create stdin pipe: %w", err)
	}

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		_ = stdin.Close()
		w.cancel()
		return fmt.Errorf("failed to create stdout pipe: %w", err)
	}

	stderr, err := cmd.StderrPipe()
	if err != nil {
		_ = stdin.Close()
		_ = stdout.Close()
		w.cancel()
		return fmt.Errorf("failed to create stderr pipe: %w", err)
	}

	w.cmd = cmd
	w.stderr = stderr
	w.stdin = stdin
	w.stdout = stdout

	err = cmd.Start()
	if err != nil {
		return fmt.Errorf("failed to start process: %w", err)
	}

	w.waitGroup.Add(3)
	go w.handleStdout()
	go w.handleStderr()
	go w.wait()

	return nil
}

func (w *processWorker) stop() error {
	// Close stdin to signal the process to stop gracefully
	if err := w.stdin.Close(); err != nil {
		slog.Error("Error closing stdin", "error", err)
	}

	// Give the process time to shut down gracefully
	done := make(chan struct{})
	go func() {
		w.waitGroup.Wait()
		close(done)
	}()

	select {
	case <-done:
		// Process stopped gracefully
		return nil
	case <-time.After(5 * time.Second):
		// Force kill if it doesn't stop gracefully
		slog.Info("Process didn't stop gracefully, forcing termination")
		w.cancel()

		if w.cmd.Process != nil {
			if err := w.cmd.Process.Kill(); err != nil {
				return fmt.Errorf("failed to kill process: %w", err)
			}
		}

		// Wait for cleanup
		w.waitGroup.Wait()
		return nil
	}
}

func (w *processWorker) run(data any) (*WorkerResponse, error) {
	return w.workerBase.run(data, w.send)
}

func (w *processWorker) wait() {
	defer w.waitGroup.Done()

	err := w.cmd.Wait()
	if err != nil {
		slog.Error("Process exited with error", "error", err)
	} else {
		slog.Info("Process completed successfully")
	}

	for id := range w.queue.tasks {
		w.resolve(fmt.Appendf(nil, `{"id":"%s","data":"process exited"}`, id), true)
	}

	// Cancel context when process exits
	w.cancel()
}

func (w *processWorker) send(t *workerTask) error {
	select {
	case <-w.ctx.Done():
		return fmt.Errorf("process context cancelled")
	default:
		_, err := w.stdin.Write(t.data)
		return err
	}
}

func (w *processWorker) handleStdout() {
	defer w.waitGroup.Done()

	scanner := bufio.NewScanner(w.stdout)
	for scanner.Scan() {
		select {
		case <-w.ctx.Done():
			return
		default:
			message := scanner.Bytes()
			slog.Info("STDOUT", "message", message)
			w.resolve(message, false)
		}
	}

	err := scanner.Err()
	if err != nil {
		slog.Error("stdout scanner error")
	}
}

func (w *processWorker) handleStderr() {
	defer w.waitGroup.Done()

	scanner := bufio.NewScanner(w.stderr)
	for scanner.Scan() {
		select {
		case <-w.ctx.Done():
			return
		default:
			message := scanner.Bytes()
			slog.Error("STDERR", "message", message)
			w.resolve(message, true)
		}
	}

	err := scanner.Err()
	if err != nil {
		slog.Error("stdterr scanner error")
	}
}
