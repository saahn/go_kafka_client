/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * 
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package go_kafka_client

import (
	"time"
	"fmt"
	"sync"
)

type BatchAccumulator struct {
	Config *ConsumerConfig
	InputChannel *SharedBlockChannel
	MessageBuffers map[TopicAndPartition]*MessageBuffer
	MessageBuffersLock  sync.Mutex
	closeFinished       chan bool
	askNextBatch        chan TopicAndPartition
	reconnectChannels   chan bool
	disconnectChannelsForPartition chan TopicAndPartition
	removeBufferChannel chan TopicAndPartition
	stopProcessing      chan bool
}

func NewBatchAccumulator(config *ConsumerConfig, askNextBatch chan TopicAndPartition, reconnectChannels chan bool, disconnectChannelsForPartition chan TopicAndPartition) *BatchAccumulator {
	blockChannel := &SharedBlockChannel{make(chan *TopicPartitionData, config.QueuedMaxMessages), false}
	ba := &BatchAccumulator {
		Config : config,
		InputChannel : blockChannel,
		MessageBuffers : make(map[TopicAndPartition]*MessageBuffer),
		closeFinished : make(chan bool),
		askNextBatch: askNextBatch,
		reconnectChannels: reconnectChannels,
		disconnectChannelsForPartition: disconnectChannelsForPartition,
		removeBufferChannel: make(chan TopicAndPartition),
		stopProcessing: make(chan bool),
	}

	go ba.processIncomingBlocks()
	return ba
}

func (ba *BatchAccumulator) String() string {
	return fmt.Sprintf("%s-batchAccumulator", ba.Config.Consumerid)
}

func (ba *BatchAccumulator) RemoveBuffer(topicPartition TopicAndPartition) {
	ba.removeBufferChannel <- topicPartition
}

func (ba *BatchAccumulator) processIncomingBlocks() {
	Debug(ba, "Started processing blocks")

	for {
		select {
		case b := <-ba.InputChannel.chunks: {
			fetchResponseBlock := b.Data
			topicPartition := b.TopicPartition
			buffer, exists := ba.MessageBuffers[topicPartition]
			if !exists {
				Tracef(ba, "Adding new MessageBuffer for %s", topicPartition)
				ba.MessageBuffers[topicPartition] = NewMessageBuffer(&topicPartition, make(chan []*Message, ba.Config.QueuedMaxMessages), ba.Config)
				buffer = ba.MessageBuffers[topicPartition]
				ba.reconnectChannels <-true
			}
			if fetchResponseBlock != nil {
				for _, message := range fetchResponseBlock.MsgSet.Messages {
					buffer.Add(&Message {
						Key : message.Msg.Key,
						Value : message.Msg.Value,
						Topic : topicPartition.Topic,
						Partition : topicPartition.Partition,
						Offset : message.Offset,
					})
				}
			}
			select {
			case ba.askNextBatch <- topicPartition:
			case tp := <-ba.removeBufferChannel: {
				if mb, exists := ba.MessageBuffers[tp]; exists {
					mb.Stop()
					delete(ba.MessageBuffers, tp)
					ba.disconnectChannelsForPartition <- tp
				}
			}
			case <-ba.stopProcessing: {
				ba.closeFinished <- true
				return
			}
			}
		}
		case tp := <-ba.removeBufferChannel: {
			if mb, exists := ba.MessageBuffers[tp]; exists {
				mb.Stop()
				delete(ba.MessageBuffers, tp)
				ba.disconnectChannelsForPartition <- tp
			}
		}
		case <-ba.stopProcessing: {
			ba.closeFinished <- true
			return
		}
		}
	}
	Trace(ba, "Left processing incoming blocks")
}

func (ba *BatchAccumulator) Stop() {
	Debug(ba, "Trying to stop BA")
	if !ba.InputChannel.closed {
		ba.InputChannel.closed = true
		for _, buffer := range ba.MessageBuffers {
			buffer.Stop()
		}
		Trace(ba, "Stopping processing")
		ba.stopProcessing <- true
		Trace(ba, "Stopped processing")
		<-ba.closeFinished
	}
}

type MessageBuffer struct {
	OutputChannel chan []*Message
	Messages      []*Message
	Config *ConsumerConfig
	Timer *time.Timer
	MessageLock   sync.Mutex
	Close         chan bool
	stopSending   bool
	TopicPartition *TopicAndPartition
}

func NewMessageBuffer(topicPartition *TopicAndPartition, outputChannel chan []*Message, config *ConsumerConfig) *MessageBuffer {
	buffer := &MessageBuffer{
		OutputChannel : outputChannel,
		Messages : make([]*Message, 0),
		Config : config,
		Timer : time.NewTimer(config.FetchBatchTimeout),
		Close : make(chan bool),
		TopicPartition : topicPartition,
	}

	go buffer.Start()

	return buffer
}

func (mb *MessageBuffer) String() string {
	return fmt.Sprintf("%s-messageBuffer", mb.TopicPartition)
}

func (mb *MessageBuffer) Start() {
	for {
		select {
		case <-mb.Close: return
		case <-mb.Timer.C: {
			Debug(mb, "Batch accumulation timed out. Flushing...")
			InLock(&mb.MessageLock, func() {
				mb.Flush(true)
			})
		}
		}
	}
}

func (mb *MessageBuffer) Stop() {
	Debug(mb, "Stopping message buffer")
	mb.stopSending = true
	mb.Close <- true
	Debug(mb, "Stopped message buffer")
}

func (mb *MessageBuffer) Add(msg *Message) {
	InLock(&mb.MessageLock, func() {
		Debugf(mb, "Added message: %s", msg)
		mb.Messages = append(mb.Messages, msg)
		if len(mb.Messages) == mb.Config.FetchBatchSize {
			Debug(mb, "Batch is ready. Flushing")
			mb.Flush(false)
		}
	})
}

func (mb *MessageBuffer) Flush(optional bool) {
	if len(mb.Messages) > 0 {
		Debug(mb, "Flushing")
		mb.Timer.Reset(mb.Config.FetchBatchTimeout)
	flushLoop:
		for {
			select {
			case mb.OutputChannel <- mb.Messages: break flushLoop
			case <-time.After(200 * time.Millisecond): if mb.stopSending || optional {
				return
			}
			}
		}
		Debug(mb, "Flushed")
		mb.Messages = make([]*Message, 0)
	}
}
