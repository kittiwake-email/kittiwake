package main

import (
	"archive/tar"
	"compress/gzip"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

// packMaildir writes dir as a gzip tar whose top-level directory is
// "maildir", the layout the import endpoint expects. Only regular files
// and directories are included; symlinks are skipped.
func packMaildir(dir string, w io.Writer) error {
	st, err := os.Stat(dir)
	if err != nil {
		return err
	}
	if !st.IsDir() {
		return errors.New(dir + " is not a directory")
	}
	gz := gzip.NewWriter(w)
	tw := tar.NewWriter(gz)
	err = filepath.WalkDir(dir, func(p string, d fs.DirEntry, werr error) error {
		if werr != nil {
			return werr
		}
		rel, err := filepath.Rel(dir, p)
		if err != nil {
			return err
		}
		name := "maildir"
		if rel != "." {
			name = "maildir/" + filepath.ToSlash(rel)
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		switch {
		case d.IsDir():
			return tw.WriteHeader(&tar.Header{Name: name + "/", Mode: 0o700, ModTime: info.ModTime(), Typeflag: tar.TypeDir})
		case !info.Mode().IsRegular():
			return nil
		}
		if err := tw.WriteHeader(&tar.Header{Name: name, Mode: 0o600, Size: info.Size(), ModTime: info.ModTime(), Typeflag: tar.TypeReg}); err != nil {
			return err
		}
		f, err := os.Open(p)
		if err != nil {
			return err
		}
		defer f.Close()
		_, err = io.Copy(tw, f)
		return err
	})
	if err != nil {
		return err
	}
	if err := tw.Close(); err != nil {
		return err
	}
	return gz.Close()
}

// isGzipName reports whether a file name looks gzip-compressed.
func isGzipName(name string) bool {
	return strings.HasSuffix(name, ".gz") || strings.HasSuffix(name, ".tgz")
}
