package migrations

import "embed"

// Files содержит все SQL-миграции, встроенные в бинарник.
//
//go:embed *.sql
var Files embed.FS
