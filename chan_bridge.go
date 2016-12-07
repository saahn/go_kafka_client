package go_kafka_client

/*
chan_bridge leverages the libchan library to implement a network bridge over which two
mirrormakers instances can send and receive Kafka messages to replicate.
The libchan library: https://github.com/docker/libchan
The BridgeSender and BridgeReceiver abstractions are based on the remote example
provided by: https://github.com/bfosberry/banano
 */

import (
    "log"
    "net"
    "os"
    "github.com/docker/libchan"
    "github.com/docker/libchan/spdy"
    "crypto/tls"
    "time"
    "errors"
    "expvar"
)

// Vars exposed to health endpoint
var (
    // States
    MMode = expvar.NewString("mode")  // "Receiver" or "Sender"
    MHealth = expvar.NewString("health")  // "Healthy", "Waiting", or "Failed"
    MStatus = expvar.NewString("status")  // More details on current state

    // Sender metrics
    MMessageSendAttemptCount = expvar.NewInt("message_send_attempt_count")
    MMessageSendSuccessCount = expvar.NewInt("message_send_success_count")
    MMessageSendFailureCount = expvar.NewInt("message_send_failure_count")

    // Receiver metrics
    MMessageReceiveSuccessCount = expvar.NewInt("message_receive_success_count")
    MMessageReceiveFailureCount = expvar.NewInt("message_receive_failure_count")
)

// Possible MHealth states
const (
    MHealthy     = "Healthy"
    MWaiting     = "Waiting"
    MFailed      = "Failed"
)

type SenderFunc func() (libchan.Sender, error)

type BridgeMessage struct {
    Msg             Message
    Seq             int
    ResponseChan    libchan.Sender
}

type BridgeResponse struct {
	Msg 	string
    Err     error
}


/* BridgeSender */

type bridgeSender struct {
    senderFunc      SenderFunc
    receiver        libchan.Receiver
    remoteSender    libchan.Sender
}

func NewBridgeSender(senderFunc SenderFunc, receiver libchan.Receiver, remoteSender libchan.Sender) *bridgeSender {
    return &bridgeSender{
        senderFunc:     senderFunc,
        receiver:       receiver,
        remoteSender:   remoteSender,
    }
}

/*
ChanBridge
Wraps a ChanBridgeSender and ChanBridgeReceiver with their local Kafka Message Go Channel streams.
*/

type ChanBridge struct {
    sender          *ChanBridgeSender
    receiver        *ChanBridgeReceiver
    //connStateChan   chan ConnState
}

func NewChanBridge(sender *ChanBridgeSender, receiver *ChanBridgeReceiver) *ChanBridge {
    return &ChanBridge{
        sender:           sender,
        receiver:         receiver,
    }
}

type ConnState int

const (
    Connected ConnState = iota
    Disconnected
    Stopped
)

type ChanBridgeSender struct {
    goChannels      []chan *Message
    remoteUrl       string
    failedMessages  []*Message

	streamProvider	spdy.StreamProvider
	transport 		libchan.Transport
	tcpConn 		net.Conn

	block 			chan struct{}
	connState		chan ConnState
}

type ChanBridgeReceiver struct {
    goChannels      []chan *Message
    listenUrl       string
	connState 		chan ConnState
    //failedSends     chan *Message
}

func dialTcp(addr string, useTLS string) (net.Conn, error) {
	if useTLS == "true" {
		return tls.Dial("tcp", addr, &tls.Config{InsecureSkipVerify: true})
	} else {
		return net.Dial("tcp", addr)
	}
}

func listenTcp(laddr string, cert string, key string) (net.Listener, error) {
	log.Print("Starting listener...")
	if cert != "" && key != "" {
		log.Print("Starting listener with TLS support")
		tlsCert, err := tls.LoadX509KeyPair(cert, key)
		if err != nil {
			log.Fatal(err)
		}
		tlsConfig := &tls.Config{
			InsecureSkipVerify: true,
			Certificates:       []tls.Certificate{tlsCert},
		}
		return tls.Listen("tcp", laddr, tlsConfig)
	} else {
		return net.Listen("tcp", laddr)
	}
}

func (cb *ChanBridge) Start() {
    MStatus.Set("Starting...")
    if cb.receiver != nil {
        log.Print("Trying to start ChanBridgeReceiver...")
        MMode.Set("Receiver")
        go tryConnect(cb.receiver, cb.receiver.connState)
    }
    if cb.sender != nil {
        log.Print("Trying to connect ChanBridgeSender...")
        MMode.Set("Sender")
        go tryConnect(cb.sender, cb.sender.connState)
    }
}

func (cb *ChanBridge) Stop() {
    log.Print("Stopping ChanBridge")
    MStatus.Set("Stopping...")
    MHealth.Set(MWaiting)
    if cb.receiver != nil {
        log.Print("Trying to stop ChanBridgeReceiver...")
        cb.receiver.Stop()
    }
    if cb.sender != nil {
        log.Print("Trying to stop ChanBridgeSender...")
        cb.sender.Stop()
    }
    //cb.connStateChan <-Stopped
}

func (cbr *ChanBridgeReceiver) Stop() {
    log.Print("Stopping ChanBridgeReceiver's goChannels...")
    for _, ch := range cbr.goChannels {
        close(ch)
    }
    log.Print("All of ChanBridgeReceiver's goChannels are stopped.")
	cbr.connState <- Stopped
}

func (cbs *ChanBridgeSender) Stop() {
    log.Print("Stopping ChanBridgeSender...")
	// TODO: drain failedMessages array
	cbs.DrainFailed()
	if len(cbs.failedMessages) > 0 {
		log.Print("!!! [cbs.Stop WARN] There are failed messages in the queue that were not resent...")
	}
	err := cbs.streamProvider.Close()
	if err != nil {
		log.Fatal("Failed to close ChanBridgeSender.streamProvider")
	}
    log.Print("ChanBridgeSender is stopped")
	cbs.connState <- Stopped
}

func NewChanBridgeSender(goChannels []chan *Message, remoteUrl string) *ChanBridgeSender {
    return &ChanBridgeSender{
        goChannels:     goChannels,
        remoteUrl:      remoteUrl,
        failedMessages: make([]*Message, 0),
        block:     		make(chan struct{}),
		connState:      make(chan ConnState, 1),
	}
}

//func (cbs *ChanBridgeSender) Connect() {
//	MHealth.Set(MWaiting)
//	cbs.connState <-Disconnected
//	CONNECT_LOOP:
//	for {
//		select {
//		case cs := <-cbs.connState:
//			switch {
//			case cs == Connected:
//				MStatus.Set("Connected")
//				log.Print("ChanBridgeSender is connected.")
//			case cs == Disconnected:
//				log.Print("ChanBridgeSender is disconnected...restarting.")
//				MStatus.Set("Disconnected")
//				err := cbs.tryConnect()
//				if err != nil {
//					cbs.connState <- Disconnected
//				} else {
//					cbs.connState <- Connected
//				}
//			case cs == Stopped:
//				MStatus.Set("Stopped")
//				close(cbs.connState)
//				break CONNECT_LOOP
//			}
//		default:
//			<-time.After(3 * time.Second)
//		}
//	}
//	MHealth.Set(MFailed)
//	log.Print("Exited tryConnect loop.")
//}

type bridgeEndpoint interface {
	Connect() error
}

func tryConnect(be bridgeEndpoint, c chan ConnState) {
	MHealth.Set(MWaiting)
	c <-Disconnected
	CONNECT_LOOP:
	for {
		select {
		case cs := <-c:
			switch {
			case cs == Connected:
				MStatus.Set("Connected")
				log.Printf("%v is connected.", be)
			case cs == Disconnected:
				log.Printf("%v is disconnected...restarting.", be)
				MStatus.Set("Disconnected")
				err := be.Connect()
				if err != nil {
					c <- Disconnected
				}
			case cs == Stopped:
				MStatus.Set("Stopped")
				close(c)
				break CONNECT_LOOP
			}
		default:
			<-time.After(3 * time.Second)
		}
	}
	MHealth.Set(MFailed)
	log.Print("Exited tryConnect loop.")
}

func (cbs *ChanBridgeSender) Connect() error {
    var clientConn net.Conn
    var err error
    var transport libchan.Transport
    useTLS := os.Getenv("USE_TLS")
    log.Printf("'USE_TLS' env value: %v", useTLS)

    // Resolve remote endpoint to multiple A-record IPs and try each until success
    remoteHost, remotePort, err := net.SplitHostPort(cbs.remoteUrl)
    if err != nil {
        log.Fatal(err)
    }
    for {
        MStatus.Set("Trying to connect to remote receiver...")
        var connected = false
        for !connected {
            remoteIPs, err := net.LookupHost(remoteHost)
            for _, ip := range remoteIPs {
                addr := ip + ":" + remotePort
                log.Printf("Trying to connect to remote host on %v", addr)
				clientConn, err = dialTcp(addr, useTLS)
                if err == nil {
                    log.Printf("Connected to remote host on %v", addr)
					cbs.tcpConn = clientConn
                    connected = true
                    break
                }
                log.Printf("Failed to connect to %v: %+v... trying next ip", addr, err)
                time.Sleep(5 * time.Second)
            }
        }
        p, err := spdy.NewSpdyStreamProvider(clientConn, false)
        if err == nil {
            transport = spdy.NewTransport(p)
			cbs.streamProvider = p
			cbs.transport = transport
            break
        } else {
			clientConn.Close()  // close before trying to connect again
        }
    }
    //cbs.connState <- Connected
    err = cbs.Start()
    return err
}

func (cbs *ChanBridgeSender) Start() error {
	err := cbs.DrainFailed()
	if err != nil {
		// force connection break
		MHealth.Set(MFailed)
		close(cbs.block)
		return errors.New("Sender failed to start")
	}

	// Start a new send stream for each consumer goChannel
	for goChanIndex, msgChan := range cbs.goChannels {
		log.Printf("In cbs.connections loop. goChanIndex: %v", goChanIndex)
		go func(goChanIndex int) {
			defer log.Printf("Ending send goroutine for goChanIndex [%v]...", goChanIndex)

			//log.Printf("... in new goroutine for sender's gochannel index [%v]", goChanIndex)
			LOOP:
			for message := range msgChan {
				select {
				case <-cbs.block:
				    cbs.failedMessages = append(cbs.failedMessages, message)
					log.Print("*** got a signal from block channel")
				    break LOOP
				default:
					log.Printf("... read a message from sender's gochannel index [%v]: %+v", goChanIndex, message)
					err := cbs.TrySend(message, false)
					if err != nil {
						log.Print("!!! Failed to send message!")
						select {
						case _, ok := <-cbs.block:
							if ok {
								close(cbs.block)
								MHealth.Set(MFailed)
								log.Printf("!!!!!! sending  message failed: %+v", message)
							}
						default:
						}
					}
				}
			}
			log.Print("*** broke out of LOOP")
		}(goChanIndex)
	}
	MHealth.Set(MHealthy)
	return nil
}

// Sends messages in the failedMessages list
func (cbs *ChanBridgeSender) DrainFailed() error {
	fmCount := len(cbs.failedMessages)
	log.Printf("Number of failed messages to resend: %v", fmCount)
	for i := 0; i < fmCount; i++ {
	    log.Printf("cbs.failedMessages, len, cap: %v, %v, %v",
	        cbs.failedMessages, len(cbs.failedMessages), cap(cbs.failedMessages))
	    fm := cbs.failedMessages[i]
	    log.Printf("~~~~~~ resending a failed message: %+v", *fm)
		err := cbs.TrySend(fm, true)
	    if err != nil {
	        log.Printf("!!!!!! resending a failed message failed again. msg: %+v", *fm)
	        // This is the first resend failure in the loop, so all previous messages in failedMessages array
	        // successfully resent the message. Remove those, but keep the rest in failedMessages array.
	        cbs.failedMessages = append(make([]*Message, fmCount-i), cbs.failedMessages[i:]...)
	        return err
	    }
	}
	// Successfully resent all failed messages. Reset failedMessages slice.
	cbs.failedMessages = nil
	log.Printf("Done resending failed messages! cbs.failedMessages, len, cap: %v, %v, %v",
	    cbs.failedMessages, len(cbs.failedMessages), cap(cbs.failedMessages))
	return nil
}


// 1. Creates 2 sender/receiver pairs for sending a message and receiving an ack.
// 		a. the sender is created as a new send channel from cbs.transport that was initialized when a
//		   connection was established with the remote server.
//		b. a new remoteSender/receiver pair is created via libchan.Pipe()
// 2. The sender sends a msg as well as the remoteSender to the remote server.
// 3. The server unpacks the remoteSender via which it sends an ack message; client receives via its receiver created
//    in step 1b.
// 4. The client closes the sender, which closes message channel.
// 5. The client closes the remoteSender, which closes the ack channel.
func (cbs *ChanBridgeSender) TrySend(m *Message, resend bool) (e error) {
	receiver, remoteSender := libchan.Pipe()
	bs := NewBridgeSender(cbs.transport.NewSendChannel, receiver, remoteSender)
	MMessageSendAttemptCount.Add(1)

	select {
	case <-cbs.block:
		if !resend {
			cbs.failedMessages = append(cbs.failedMessages, m)
		}
		return errors.New("Sending is blocked")
	default:
		log.Print("trySend is not blocked.")
	}

	sender, err := bs.senderFunc()
	defer func() {
		log.Print("=== In deferred function")
		if e != nil {
			MMessageSendFailureCount.Add(1)
			if !resend {
				cbs.failedMessages = append(cbs.failedMessages, m)
			}
			log.Printf("!!! [TrySend ERROR] Failed to send message: %+v", m)
			log.Printf("... [TrySend INFO] send failure error: %+v", e)
			log.Print("Closing block channel")
			close(cbs.block)
			//log.Print("[TrySend INFO] Forcing client to disconnect.")
			//cbs.streamProvider.Close()
			log.Print("Sending Disconnected signal")
			cbs.connState <- Disconnected
		} else {
			log.Print(":) message send succeeded")
			MMessageSendSuccessCount.Add(1)
		}
	}()
	if err != nil {
		return err
	}
	bm := BridgeMessage{
		Msg: *m,
		Seq: 100,
		ResponseChan: bs.remoteSender,
	}
	if err := sender.Send(bm); err != nil {
		return err
	}
	response := &BridgeResponse{}
	if err := bs.receiver.Receive(response); err != nil {
		log.Printf("!!! [TrySend ERROR] Receive failed with error %+v", err)
		return err
	}
	log.Printf("[TrySend DEBUG] got response from receiver: %+v", response)
	err = sender.Close()
	if err != nil {
		log.Print("=== [TrySend ERROR] FAILED to close sender.")
	} else {
		log.Print("=== [TrySend INFO] Closed sender.")
	}
	err = remoteSender.Close()
	if err != nil {
		log.Print("=== [TrySend ERROR] FAILED to close remoteSender.")
	} else {
		log.Print("=== [TrySend INFO] Closed remoteSender.")
	}
	return err
}


func NewChanBridgeReceiver(goChannels []chan *Message, listenUrl string) *ChanBridgeReceiver {
    return &ChanBridgeReceiver{
        goChannels: goChannels,
        listenUrl:  listenUrl,
		connState:  make(chan ConnState, 1),
		//failedSends: make(chan *Message, 1),
    }
}

func (cbr *ChanBridgeReceiver) Connect() error {
    cert := os.Getenv("TLS_CERT")
    key := os.Getenv("TLS_KEY")
    log.Printf("'TLS_CERT' env value: %v", cert)
    log.Printf("'TLS_KEY' env value: %v", key)

    var listener net.Listener
    var err error

    listener, err = listenTcp(cbr.listenUrl, cert, key)
    if err != nil {
        log.Printf("Failed to listen: %v", err)
        return err
    }
	cbr.connState <-Connected
	go cbr.Start(listener)
	return err
}

func (cbr *ChanBridgeReceiver) Start(listener net.Listener) error {
	defer log.Print("=== Ending ChanBridgeReceiver")
	RECEIVELOOP1:
    for {
		c, err := listener.Accept()
		log.Print("[cbr.Start INFO] === In ChanBridgeReceiver's listner.Accept loop ===")
		if err != nil {
			log.Printf("!!! [cbr.Start ERROR] Failed to establish a connection. Error: %v", err)
			break RECEIVELOOP1
		}
		p, err := spdy.NewSpdyStreamProvider(c, true)
		if err != nil {
			log.Printf("!!! [cbr.Start ERROR] Failed to create a spdyStreamProvider. Error: %v", err)
			break RECEIVELOOP1
		}
		t := spdy.NewTransport(p)
		MStatus.Set("Listening")
		MHealth.Set(MHealthy)
		go func(t libchan.Transport) {
			defer log.Print("====== [cbr.Start INFO] exiting goroutine.")
			log.Print("====== [cbr.Start INFO] in new goroutine")
			chanCount := 0
			RECEIVELOOP2:
			for {
				receiver, err := t.WaitReceiveChannel()
				if err != nil {
					log.Printf("!!! [cbr.Start ERROR] Failed to create a new channel with remote sender. Error: %v", err)
					break RECEIVELOOP2
				}
				chanCount++
				go func(receiver libchan.Receiver) {
					log.Print("[cbr.Start INFO] === Created a new channel with remote sender")
					defer log.Printf("[cbr.Start INFO] === Ending receive goroutine...chanCount is %d", chanCount)
					var receivedCount = 0
					RECEIVELOOP3:
					for {
						log.Printf("receivedCount in channel %v: %d", receiver, receivedCount)
						msg, receiveError, ackError := cbr.Receive(receiver)
						if receiveError != nil || ackError != nil {
							log.Printf("[cbr.Start ERROR] Receiver got errors. receiveError: %+v, " +
								"ackError: %+v, msg: %+v", receiveError, ackError, msg)
							MMessageReceiveFailureCount.Add(1)
							break RECEIVELOOP3
						} else {
							receivedCount++
							MMessageReceiveSuccessCount.Add(1)
							//log.Printf("the msg to send over goChannel: %+v", m)
							h := TopicPartitionHash(&msg)
							goChanLen := len(cbr.goChannels)
							//i := TopicPartitionHash(&m)%len(cbr.goChannels)
							i := h % goChanLen
							log.Printf(">>> h, goChanLen, i: %d, %d, %d", h, goChanLen, i)
							//cbr.goChannels[i] <- &m
							select {
							case cbr.goChannels[i] <- &msg:
								log.Printf(">>> [cbr.Start INFO] sent msg to receiver's goChannels[%v]", i)
								break RECEIVELOOP3
							default:
								log.Printf(">>> [cbr.Start INFO] DID NOT send msg to receiver's goChannels[%v]", i)
							}
						}
					}
					log.Printf("... [cbr.Start INFO] Broke out of RECEIVELOOP3 loop! receivedCount in channel %v: %d", receiver, receivedCount)
				}(receiver)
				//log.Printf("... [cbr.Start DEBUG] ... chanCount: %d", chanCount)
			}
			//log.Print("... [cbr.Start INFO] Broke out of RECEIVELOOP2 for loop!")
		}(t)
    }
	log.Print("!!! [cbr.Start ERROR] broke out of RECEIVELOOP1 for loop!")
	MHealth.Set(MFailed)
    return errors.New("ChanBridgeReceiver failed to start")
}

func (cbr *ChanBridgeReceiver) Receive(receiver libchan.Receiver) (msg Message, receiveErr error, ackErr error) {
	bridgeMessage := &BridgeMessage{}
	bridgeResponse := &BridgeResponse{}
	receiveErr = receiver.Receive(bridgeMessage)
	log.Printf("... [cbr.Receive DEBUG] Received a BridgeMessage: %+v", bridgeMessage)
	//log.Printf("_ _ _ the received msg string: %+v", string(bridgeMessage.Msg.Value))
	msg = bridgeMessage.Msg
	bridgeResponse.Err = receiveErr
	bridgeResponse.Msg = "totally OK"
	if receiveErr != nil {
		bridgeResponse.Msg = "not OK"
		log.Printf("!!! [cbr.Receive ERROR] Failed to receive a message from remote sender. Error: %v", receiveErr)
		if receiveErr.Error() == "EOF" {
			log.Print("[cbr.Receive INFO] *** Got EOF error from sender.")
			bridgeResponse.Msg = "not OK? got EOF"
		}
		if receiveErr.Error() == "stream does not exist" || bridgeMessage.ResponseChan == nil {
			log.Print("[cbr.Receive ERROR] sender's stream is gone")
			bridgeResponse.Msg = "not OK? sender stream is gone"
		}
		return msg, receiveErr, nil
	} else {
		ackErr = bridgeMessage.ResponseChan.Send(bridgeResponse)
		if ackErr != nil {
			log.Printf("!!! [cbr.SendAck ERROR] Failed to send ack from receiver. Error: %v", ackErr)
		} else {
			log.Printf("... [cbr.SendAck INFO] Sent ack from receiver: %v", bridgeResponse)
		}
	}
	log.Printf("... [cbr.Receive INFO] Received message: %+v", bridgeMessage.Msg)
	return msg, receiveErr, ackErr
}


