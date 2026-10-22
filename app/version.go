package app

// Release is the product release, bumped by hand for every release. The
// scheme is rolling: <two-digit year>.<release number within that year>,
// so 26.0 is the first release of 2026 and the first release of 2027 is
// 27.1 (the second number simply counts up during the year). Nothing else
// in the tree carries the number; the Makefile reads it from here.
const Release = "26.0"

// Commit is the short git commit the binary was built from, with a
// "-dirty" suffix for an uncommitted tree; set by the Makefile with
// -ldflags "-X kittiwake/app.Commit=...". A plain `go build` leaves it
// unknown.
var Commit = "unknown"

// Version is what every daemon reports: the release with the commit
// embedded, e.g. 26.0+2b4ab79. Shown by `kittiwake version`, the admin
// status, the node registry and the startup log line, and recorded in
// the schema_version table by whichever build applied a migration.
var Version string

func init() { Version = Release + "+" + Commit }
