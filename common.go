package taskmgr

import "github.com/twinj/uuid"

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
type Id uuid.UUID
type State int

func (state State) String() string {
    switch state {
    case Waiting:
        return "waiting"
    case Ready:
        return "ready"
    case Running:
        return "running"
    case Stop:
        return "stopped"
    case Complete:
        return "complete"
    case Fail:
        return "fail"
    case Abort:
        return "aborted"
    default:
        return "INVALID STATE"
    }
}

// for GetStatus
type TaskInfo struct {
	Path   Path
	Args   []string
	Pid    int
	Group  string
	Status int
}

type Tasks []TaskInfo
