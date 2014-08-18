package taskmgr

// Possible task states
const (
	Waiting    = 0
	Ready      = 1
	Running    = 2
	Stop       = 3
	Complete   = 4
	Fail       = 5
	Abort      = 6

	Nqueue     = 7 // number of queues
)

type Group  string
type Path   string
type Server string
type Login  string

type Empty struct{}
type Id int
type State int

const TaskZero = Id(0)

// for GetStatus
type TaskInfo struct {
	Path   Path
	Args   []string
	Pid    int
	Group  string
	Status int
}

type Tasks []TaskInfo
