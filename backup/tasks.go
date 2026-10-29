package backup

// Task types the backup mode executes. Every other mode only enqueues
// them; the backup nodes are the only consumers.
const (
	TaskBackup   = "backup"          // {mailbox, force, full}
	TaskJob      = "backup-job"      // {job}: restore, restore-message, export, verify, forget
	TaskMaintain = "backup-maintain" // {mailbox}: GC of expired runs' packs, verification
	TaskExpire   = "backup-expire"   // periodic: expire runs, exports and unreferenced blobs
)

// AllTasks lists every queue the backup mode drains.
var AllTasks = []string{TaskBackup, TaskJob, TaskMaintain, TaskExpire}
