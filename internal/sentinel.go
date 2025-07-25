package internal

import "os"

// Sentinel is used to signal completion of a walked
// Directory.
type Sentinel struct {
	Info os.FileInfo
	Path string
}
