package main

import (
	"github.com/cube2222/octosql/plugins"
	"github.com/tjungblu/octosql-plugin-etcdsnapshot/etcdsnapshot"
)

func main() {
	plugins.Run(etcdsnapshot.Creator)
}
