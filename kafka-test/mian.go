package main

import (
	"github.com/pingcap/tidb-tools/pkg/kafka"
)

func main() {
	cfgFile := "/tmp/sarama.toml"
	_ = kafka.NewConfig(cfgFile)

}
