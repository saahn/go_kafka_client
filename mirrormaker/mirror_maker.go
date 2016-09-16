/* Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License. */

package main

import (
	"flag"
	"fmt"
	"github.com/elodina/go-kafka-avro"
	kafka "github.com/elodina/go_kafka_client"
	"os"
	"os/signal"
	"runtime"
	"syscall"
)

type consumerConfigs []string

func (i *consumerConfigs) String() string {
	return fmt.Sprintf("%s", *i)
}

func (i *consumerConfigs) Set(value string) error {
	*i = append(*i, value)
	return nil
}

var whitelist = flag.String("whitelist", "", "regex pattern for whitelist. Providing both whitelist and blacklist is an error.")
var blacklist = flag.String("blacklist", "", "regex pattern for blacklist. Providing both whitelist and blacklist is an error.")
var consumerConfig consumerConfigs
var producerConfig = flag.String("producer.config", "", "Path to producer configuration file.")
var numProducers = flag.Int("num.producers", 1, "Number of producers.")
var numStreams = flag.Int("num.streams", 1, "Number of consumption streams.")
var preservePartitions = flag.Bool("preserve.partitions", false, "preserve partition number. E.g. if message was read from partition 5 it'll be written to partition 5.")
var preserveOrder = flag.Bool("preserve.order", false, "E.g. message sequence 1, 2, 3, 4, 5 will remain 1, 2, 3, 4, 5 in destination topic.")
var prefix = flag.String("prefix", "", "Destination topic prefix.")
var queueSize = flag.Int("queue.size", 10000, "Number of messages that are buffered between the consumer and producer.")
var maxProcs = flag.Int("max.procs", runtime.NumCPU(), "Maximum number of CPUs that can be executing simultaneously.")
var schemaRegistryUrl = flag.String("schema.registry.url", "", "Avro schema registry URL for message encoding/decoding")
var remoteUrl = flag.String("remote.url", "", "host:port of the remote mirrormaker, if mirroring over the network.")
var listenUrl = flag.String("listen.url", "", "host:port of the listen address of this mirrormaker, if mirroring over the network")

func parseAndValidateArgs() *kafka.MirrorMakerConfig {
	flag.Var(&consumerConfig, "consumer.config", "Path to consumer configuration file.")
	flag.Parse()
	runtime.GOMAXPROCS(*maxProcs)

	if (*whitelist != "" && *blacklist != "") || (*whitelist == "" && *blacklist == "") {
		fmt.Println("Exactly one of whitelist or blacklist is required.")
		os.Exit(1)
	}
	if *producerConfig == "" && len(consumerConfig) == 0 {
		fmt.Println("A producer config or at least one consumer config is required.")
		os.Exit(1)
	}
	if *remoteUrl != "" && len(consumerConfig) == 0 {
		fmt.Println("A consumer config is required to mirror messages to a remote producer.")
		os.Exit(1)
	}
	if *listenUrl != "" && *producerConfig == "" {
		fmt.Println("A producer config is required to receive messages from a remote consumer.")
		os.Exit(1)
	}
	if *queueSize < 0 {
		fmt.Println("Queue size should be equal or greater than 0")
		os.Exit(1)
	}

	config := kafka.NewMirrorMakerConfig()
	config.Blacklist = *blacklist
	config.Whitelist = *whitelist
	config.ChannelSize = *queueSize
	config.ConsumerConfigs = []string(consumerConfig)
	config.NumProducers = *numProducers
	config.NumStreams = *numStreams
	config.PreservePartitions = *preservePartitions
	config.PreserveOrder = *preserveOrder
	config.ProducerConfig = *producerConfig
	config.TopicPrefix = *prefix
	config.RemoteUrl = *remoteUrl
	config.ListenUrl = *listenUrl
	if *schemaRegistryUrl != "" {
		config.KeyEncoder = avro.NewKafkaAvroEncoder(*schemaRegistryUrl).Encode
		config.ValueEncoder = avro.NewKafkaAvroEncoder(*schemaRegistryUrl).Encode
		config.KeyDecoder = avro.NewKafkaAvroDecoder(*schemaRegistryUrl)
		config.ValueDecoder = avro.NewKafkaAvroDecoder(*schemaRegistryUrl)
	}

	return config
}

func main() {
	config := parseAndValidateArgs()
	mirrorMaker := kafka.NewMirrorMaker(config)
	go mirrorMaker.Start()

	sigc := make(chan os.Signal, 1)
	signal.Notify(sigc, os.Interrupt, syscall.SIGTERM, syscall.SIGKILL)
	<-sigc
	mirrorMaker.Stop()
}
