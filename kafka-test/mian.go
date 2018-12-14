package main

import (
	"github.com/pingcap/tidb-tools/pkg/kafka"
	log "github.com/sirupsen/logrus"
)

func main() {
	cfgFile := "/tmp/sarama.toml"
	c, err := kafka.NewConfig(cfgFile)
	if err != nil {
		panic(err)
	}

	log.Infof("sarama config: %v", c)
}
