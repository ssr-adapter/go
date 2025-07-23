package worker

import (
	"github.com/google/uuid"
	"sync"
)

type workerQueue struct {
	mutex sync.RWMutex
	tasks map[uuid.UUID]*workerTask
}

func newWorkerQueue() *workerQueue {
	return &workerQueue{
		mutex: sync.RWMutex{},
		tasks: make(map[uuid.UUID]*workerTask),
	}
}

func (q *workerQueue) add(id uuid.UUID, task *workerTask) {
	q.mutex.Lock()
	q.tasks[id] = task
	q.mutex.Unlock()
}

func (q *workerQueue) get(id uuid.UUID) *workerTask {
	q.mutex.RLock()
	t := q.tasks[id]
	q.mutex.RUnlock()
	return t
}

func (q *workerQueue) delete(id uuid.UUID) {
	q.mutex.Lock()
	delete(q.tasks, id)
	q.mutex.Unlock()
}
