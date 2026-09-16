package model

import "io"

type FormFile struct {
	Size        int64
	ContentType string
	File        io.Reader
}
