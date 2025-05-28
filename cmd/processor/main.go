//go:build wasm

package main

import (
	sdk "github.com/conduitio/conduit-processor-sdk"
	sql "github.com/devarispbrown/conduit-processor-sql"
)

func main() {
	sdk.Run(sql.NewProcessor())
}
