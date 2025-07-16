package main

import (
	"github.com/cube2222/octosql/plugins"
	"github.com/tjungblu/octosql-plugin-etcdsnapshot/pkg/etcdsnapshot"
)

func main() {
	plugins.Run(etcdsnapshot.Creator)
}
