package taskmgr

import "github.com/twinj/uuid"
import "os/exec"
import "log"
import "fmt"
import "bytes"
import "time"
import "errors"
import "syscall"
import "container/heap"
import "bufio"
import "path"
import "sync"
import "os"

type PriorityQueue []*Task

const CgroupPath = "/sys/fs/cgroup"

type TaskMgr struct {
	queues sync.Mutex
	update *sync.Cond

	workers sync.WaitGroup
	// how many tasks to run in parallel
	parallel int

	// a queue for each task state
	tasks [Nqueue]PriorityQueue

	name string

	logger *log.Logger
	die    bool
}

type Task struct {
	// for heap
	index    int
	priority int

	// parameters
	id         Id
	depends    Id
	conflicts  Group
	group      Group
	maxretries int
	nice       int
	ionice     int

	// command & process
	*exec.Cmd

	// state
	state     State
	retries   int
	startTime *time.Time
	endTime   *time.Time

	notify chan exec.Cmd
}

func (t *Task) String() string {
	return fmt.Sprintf("id: [%s] cmd: [%s] group: [%s] state: [%s]",
		t.id, func(cmd *exec.Cmd) string {
			if cmd == nil {
				return "none"
			} else {
				return fmt.Sprintf("%v", (*cmd).Args)
			}
		}(t.Cmd), t.group, t.state)
}

func (pq PriorityQueue) Len() int { return len(pq) }

func (pq PriorityQueue) Less(i, j int) bool {
	return pq[i].priority > pq[j].priority
}

func (pq PriorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i
	pq[j].index = j
}

func (pq *PriorityQueue) Push(x interface{}) {
	n := len(*pq)
	item := x.(*Task)
	item.index = n
	*pq = append(*pq, item)
}

func (pq *PriorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	if n == 0 {
		return nil
	}
	item := old[n-1]
	item.index = -1 // for safety
	*pq = old[0 : n-1]
	return item
}

func GetStdout(cmd *exec.Cmd) string {
	if cmd.Stdout == nil {
		return ""
	}
	return cmd.Stdout.(*bytes.Buffer).String()
}

func GetStderr(cmd *exec.Cmd) string {
	if cmd.Stdout == nil {
		return ""
	}
	return cmd.Stderr.(*bytes.Buffer).String()
}

func WaitAll(wait [](<-chan exec.Cmd)) []exec.Cmd {
	results := make([]exec.Cmd, len(wait))

	for i, w := range wait {
		res := <-w
		results[i] = res
	}
	return results
}

// Start task and wait for it to finish synchronously
func (task *Task) Start(log *log.Logger, onStart func(pid int)) error {
	now := time.Now()
	task.startTime = &now
	var stdout, stderr bytes.Buffer

	switch task.state {
	case Running:
		log.Printf("Task %#v already running", task)
		return nil
	case Complete:
		log.Printf("Task %#v already complete", task)
		return nil
	case Fail:
		if task.retries < task.maxretries {
			task.retries++
		} else {
			log.Printf("Task %#v exceeded maximum retries")
			return errors.New("Max retries exceeded")
		}
	case Waiting:
	}
	task.Cmd.Stdout = &stdout
	task.Cmd.Stderr = &stderr
	log.Printf("Task %v process starting", task)
	err := task.Cmd.Start()
	if err != nil {
		task.state = Fail
	} else {
		task.state = Running
		onStart(task.Cmd.Process.Pid)
		err := task.Cmd.Wait()
		log.Printf("Task %v process finished", task)
		now = time.Now()
		task.endTime = &now
		if err != nil {
			task.state = Fail
			log.Printf("[task %s] failed.", task.id)
		} else {
			task.state = Complete
			log.Printf("[task %s] completed successfully", task.id)
		}
		go func() {
			log.Printf("Task %v SENDING NOTIFICATION", task)
			task.notify <- *task.Cmd
			log.Printf("Task %v NOTIFICATION SENT", task)
			close(task.notify)
		}()
	}
	return err
}

func (self *TaskMgr) ListTasks(which *State, reply *Tasks) error {
	if which == nil || *which < 0 || *which >= Nqueue {
		return errors.New("Invalid state requested")
	}
	tasks := make([]TaskInfo, len(self.tasks[*which]))
	for i, v := range self.tasks[*which] {
		var pid int
		if v.Process != nil {
			pid = v.Process.Pid
		} else {
			pid = -1
		}
		tasks[i] = TaskInfo{Path: Path(v.Path), Args: v.Args, Pid: pid, Status: int(v.state)}
	}
	*reply = Tasks(tasks)
	return nil
}

func (self *TaskMgr) GenerateId() Id {
	return Id(uuid.NewV4())
}

// create new task
func (self *TaskMgr) NewTask(command exec.Cmd, group Group, priority int, nice int, ionice int, maxretries int, id Id) (<-chan exec.Cmd, error) {
	self.logger.Printf("New task received: %s (%#v)", id, command)
	task := self.findTask(&id, self.tasks[Waiting])
	if task == nil {
		task = &Task{
			id:         id,
			Cmd:        &command,
			group:      group,
			priority:   priority,
			nice:       nice,
			ionice:     ionice,
			maxretries: maxretries,
			notify:     make(chan exec.Cmd, 1),
		}
		//self.Lock()
		heap.Push(&self.tasks[Waiting], task)
		self.update.Signal()
		//self.Unlock()
		return task.notify, nil
	}
	return nil, errors.New(fmt.Sprintf("Task with id %d already exists", id))
}

func (self *TaskMgr) addToCgroup(pid int) {
	f, err := os.OpenFile(fmt.Sprintf(path.Join(CgroupPath, self.name, "tasks")), os.O_WRONLY|os.O_APPEND, 0700)
	if err != nil {
		self.logger.Printf("Can't add %d to cgroup: %v", pid, err)
		return
	}
	defer f.Close()
	wr := bufio.NewWriter(f)
	_, err = wr.WriteString(fmt.Sprintf("%d\n", pid))
	if err != nil {
		self.logger.Printf("Can't add %d to cgroup: %v", pid, err)
		return
	}
	wr.Flush()
}

func (self *TaskMgr) StartTask(which *Id, reply *Empty) error {
	task := self.findTask(which, self.tasks[Waiting])
	if task == nil {
		return errors.New(fmt.Sprintf("[task %d] not found", *which))
	}
	if task.depends != nil {
	}
	go task.Start(self.logger, self.addToCgroup)
	return nil
}

func (self *TaskMgr) findTask(which *Id, where []*Task) *Task {
	if which == nil {
		self.logger.Print("invalid task id (null)")
		return nil
	}
	for _, task := range where {
		if task.id == *which {
			return task
		}
	}
	return nil
}

func (self *TaskMgr) StopTask(which *Id, reply *Empty) error {
	task := self.findTask(which, self.tasks[Running])
	if task == nil {
		return errors.New(fmt.Sprintf("[task %d] not found", *which))
	}

	if task.state == Running {
		err := syscall.Kill(task.Process.Pid, syscall.SIGSTOP)
		if err != nil {
			self.log(task, "can't send signal to task")
			return errors.New("Can't send signal to task")
		} else {
			self.setState(task, Stop)
			self.log(task, "stopped")
			return nil
		}
	} else {
		self.log(task, "not running")
		return errors.New("Task not running")
	}
	return nil
}

func (self *TaskMgr) setState(task *Task, newstate State) {
	self.queues.Lock()
	heap.Remove(&self.tasks[task.state], task.index)
	heap.Push(&self.tasks[newstate], task)
	task.state = newstate
	self.queues.Unlock()
}

func (self *TaskMgr) Suspend() error {
	for _, task := range self.tasks[Running] {
		if task.state == Running {
			err := syscall.Kill(task.Process.Pid, syscall.SIGSTOP)
			if err != nil {
				self.log(task, "can't send signal to task %v", task)
				return fmt.Errorf("Can't send signal to task %v", task.id)
			} else {
				self.setState(task, Stop)
				self.log(task, "stopped")
				return nil
			}
		}
	}
	return nil
}

func (self *TaskMgr) Resume() error {
	for _, task := range self.tasks[Stop] {
		if task.state == Running {
			err := syscall.Kill(task.Process.Pid, syscall.SIGCONT)
			if err != nil {
				self.log(task, "can't send signal to task %v", task)
				return fmt.Errorf("Can't send signal to task %v", task.id)
			} else {
				self.setState(task, Running)
				self.log(task, "resumed")
				return nil
			}
		}
	}
	return nil
}

func (self *TaskMgr) ResumeTask(id *Id, reply *Empty) error {
	task := self.findTask(id, self.tasks[Stop])
	if task == nil {
		return errors.New(fmt.Sprintf("[task %d] not found", *id))
	}

	if task.state == Stop {
		err := syscall.Kill(task.Process.Pid, syscall.SIGCONT)
		if err != nil {
			self.log(task, "can't send signal to task")
			return errors.New("Can't send signal to task")
		} else {
			self.setState(task, Stop)
			self.log(task, "stopped")
			return nil
		}
	} else {
		self.log(task, "not running")
		return errors.New("Task not running")
	}
	return nil
}

func (self *TaskMgr) Run() {
	self.init()
	for !self.die {
		self.logger.Printf("Running %d tasks in parallel", self.parallel)
		for i := 0; i < self.parallel; i++ {
			self.queues.Lock()
			for self.tasks[Waiting].Len() == 0 {
				self.logger.Print("No runnable tasks. Waiting for new tasks")
				self.update.Wait()
			}
			self.queues.Unlock()

			self.queues.Lock()
			task := self.tasks[Waiting].Pop().(*Task)
			heap.Push(&self.tasks[Running], task)
			self.queues.Unlock()

			self.workers.Add(1)
			go func() {
				self.logger.Printf("Worker %d: STARTING TASK %v", i, task)
				err := task.Start(self.logger, self.addToCgroup)
				self.queues.Lock()
				heap.Remove(&self.tasks[Running], task.index)
				if err != nil {
					heap.Push(&self.tasks[Fail], task)
				} else {
					heap.Push(&self.tasks[Complete], task)
				}
				self.queues.Unlock()
				self.workers.Done()
				self.logger.Printf("Worker %d: TASK DONE %v", i, task)
			}()
		}
		self.logger.Printf("Waiting for workers to complete...")
		self.workers.Wait()
	}
}

func (self *TaskMgr) Save(task *Task) {
}

func (self *TaskMgr) log(task *Task, format string, args ...interface{}) {
	prefix := fmt.Sprintf("[task %d]", task.id)
	self.logger.Print(prefix + fmt.Sprintf(format, args))
}

func (self *TaskMgr) init() {
	for i := 0; i < Nqueue; i++ {
		heap.Init(&self.tasks[i])
	}
	self.update = sync.NewCond(&self.queues)
}

func New(logger log.Logger, parallel int, name string) *TaskMgr {
	taskmgr := &TaskMgr{logger: &logger, parallel: parallel, name: name}

	logger.SetPrefix(fmt.Sprintf("[%s] ", name))

	_, err := os.Stat(path.Join(CgroupPath, "tasks"))
	if err != nil {
		logger.Fatal("cgroups not mounted at /sys/fs/cgroup")
	}
	err = os.Mkdir(path.Join(CgroupPath, name), 0700)
	if err != nil && !os.IsExist(err) {
		logger.Print(err)
	}

	taskmgr.init()

	return taskmgr
}
