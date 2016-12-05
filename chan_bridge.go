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
    "github.com/saahn/libchan"
    "github.com/saahn/libchan/spdy"
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


type BridgeSender interface {
    //Send(Message) error
    Close() error
	trySend(*ChanBridgeSender, *Message, chan ConnState, chan struct{}, bool) error
}

type bridgeSender struct {
    senderFunc      SenderFunc
    receiver        libchan.Receiver
    remoteSender    libchan.Sender
}

func NewBridgeSender(senderFunc SenderFunc, receiver libchan.Receiver, remoteSender libchan.Sender) BridgeSender {
    return &bridgeSender{
        senderFunc:     senderFunc,
        receiver:       receiver,
        remoteSender:   remoteSender,
    }
}

//func (bs *bridgeSender) Send(m Message) error {
//    bm := BridgeMessage{
//        Msg: m,
//        Seq: 100,
//        ResponseChan: bs.remoteSender,
//    }
//    //log.Printf("Sending message: %+v", bm)
//    response, err := bs.dispatch(bm)
//    if err != nil {
//        log.Printf("Failed to dispatch a BridgeMessage: %+v", err)
//        return err
//    }
//    //log.Printf("... the sent msg string: %+v", string(bm.Msg.Value))
//    if response.Err != nil {
//        log.Printf("Receiver sent an error response: %+v", response.Err)
//    }
//    return response.Err
//}

func (bs *bridgeSender) Close() error {
    log.Print("Closing bridgeSender...")
    return nil
}

//func (bs *bridgeSender) dispatch(bm BridgeMessage) (*BridgeResponse, error) {
//    sender, err := bs.senderFunc()
//    if err != nil {
//        return nil, err
//    }
//
//    if err := sender.Send(bm); err != nil {
//        return nil, err
//    }
//    response := &BridgeResponse{}
//    if err := bs.receiver.Receive(response); err != nil {
//		sender.Close()
//		return nil, err
//    }
//	sender.Close()
//	return response, nil
//}


/* BridgeReceiver */


type BridgeReceiver interface {
    Listen(libchan.Receiver) (interface{}, error)
}

type bridgeReceiver struct {}

func NewBridgeReceiver() BridgeReceiver {
    return &bridgeReceiver {}
}

func (br *bridgeReceiver) Listen(receiver libchan.Receiver) (interface{}, error) {
    bridgeMessage := &BridgeMessage{}
    bridgeResponse := &BridgeResponse{}
    err := receiver.Receive(bridgeMessage)
    //log.Printf(">>>>> Received a BridgeMessage: %+v", bridgeMessage)
    //log.Printf("_ _ _ the received msg string: %+v", string(bridgeMessage.Msg.Value))
    bridgeResponse.Err = err
	bridgeResponse.Msg = "totally OK"
	if err != nil {
		bridgeResponse.Msg = "not OK"
        log.Printf("Got an error from sender: %v", err)
        //if err.Error() == "EOF" {
        //    log.Print("Ignoring EOF error from sender.")
        //    return nil, nil
        //}
        if err.Error() == "stream does not exist" || bridgeMessage.ResponseChan == nil {
            log.Print("sender's stream is gone")
            return nil, err
        }
    }
    //log.Printf("... sending bridgeResponse to Sender: %+v", bridgeResponse)
    sendErr := bridgeMessage.ResponseChan.Send(bridgeResponse)
    if sendErr != nil {
        log.Printf("Failed to send response to sender: %v", err)
    }
    return bridgeMessage.Msg, err
}


/*
ChanBridge
Wraps a BridgeSender and BridgeReceiver with their local Kafka Message Go Channel streams.
*/


type ChanBridge struct {
    sender          *ChanBridgeSender
    receiver        *ChanBridgeReceiver
    connStateChan   chan ConnState
}

func NewChanBridge(sender *ChanBridgeSender, receiver *ChanBridgeReceiver) *ChanBridge {
    return &ChanBridge{
        sender:           sender,
        receiver:         receiver,
        connStateChan:    make(chan ConnState, 1),
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
    bridgeSender    BridgeSender
    remoteUrl       string
    failedMessages  []*Message
    blockSends      chan bool
}

type ChanBridgeReceiver struct {
    goChannels      []chan *Message
    listenUrl       string
    bridgeReceiver  BridgeReceiver
    failedSends     chan *Message
}

type bridgeEndpoint interface {
    Connect(chan ConnState) error
    Stop()
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
                err := be.Connect(c)
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

func (cb *ChanBridge) Start() {
    MStatus.Set("Starting...")
    if cb.receiver != nil {
        log.Print("Trying to connect ChanBridgeReceiver...")
        MMode.Set("Receiver")
        go tryConnect(cb.receiver, cb.connStateChan)
    }
    if cb.sender != nil {
        log.Print("Trying to connect ChanBridgeSender...")
        MMode.Set("Sender")
        go tryConnect(cb.sender, cb.connStateChan)
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
    cb.connStateChan <-Stopped
}

func (cbr *ChanBridgeReceiver) Stop() {
    log.Print("Stopping ChanBridgeReceiver's goChannels...")
    for _, ch := range cbr.goChannels {
        close(ch)
    }
    log.Print("All of ChanBridgeReceiver's goChannels are stopped.")
}

func (cbs *ChanBridgeSender) Stop() {
    log.Print("Stopping ChanBridgeSender...")
    if cbs.bridgeSender != nil {
        err := cbs.bridgeSender.Close()
        if err != nil {
            log.Printf("There was an error while stopping the ChanBridgeSender: %+v", err)
        }
    }
    log.Print("ChanBridgeSender is stopped")
}

func NewChanBridgeSender(goChannels []chan *Message, remoteUrl string) *ChanBridgeSender {
    return &ChanBridgeSender{
        goChannels:     goChannels,
        bridgeSender:   nil,
        remoteUrl:      remoteUrl,
        failedMessages: make([]*Message, 0),
        blockSends:     make(chan bool, len(goChannels)),
    }
}

func (cbs *ChanBridgeSender) Connect(c chan ConnState) error {
    var client net.Conn
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
                client, err = dialTcp(addr, useTLS)
                if err == nil {
                    log.Printf("Connected to remote host on %v", addr)

                    connected = true
                    break
                }
                log.Printf("Failed to connect to %v: %+v... trying next ip", addr, err)
                time.Sleep(5 * time.Second)
            }
        }
        p, err := spdy.NewSpdyStreamProvider(client, false)
        if err == nil {
            transport = spdy.NewTransport(p)
            break
        } else {
            client.Close()  // close before trying to connect again
        }
    }
    //receiver, remoteSender := libchan.Pipe()
    //bridgeSender := NewBridgeSender(transport.NewSendChannel, receiver, remoteSender)
    //cbs.bridgeSender = bridgeSender
    c <- Connected
    cbs.Start(transport, c)
    return err
}


func dialTcp(addr string, useTLS string) (net.Conn, error) {
    if useTLS == "true" {
        return tls.Dial("tcp", addr, &tls.Config{InsecureSkipVerify: true})
    } else {
        return net.Dial("tcp", addr)
    }
}

func listenTcp(laddr string, cert string, key string) (net.Listener, error) {
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


func NewChanBridgeReceiver(goChannels []chan *Message, listenUrl string) *ChanBridgeReceiver {
    return &ChanBridgeReceiver{
        goChannels: goChannels,
        listenUrl:  listenUrl,
        failedSends: make(chan *Message, 1),
    }
}

func (cbr *ChanBridgeReceiver) Connect(c chan ConnState) error {
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
    c <-Connected
    bridgeReceiver := NewBridgeReceiver()
    go cbr.Start(listener, bridgeReceiver)
    return err
}

func (cbr *ChanBridgeReceiver) Start(listener net.Listener, br BridgeReceiver) error {
	RECEIVELOOP1:
    for {
        c, err := listener.Accept()
        log.Print("=== In ChanBridgeReceiver's listner.Accept loop ===")
        if err != nil {
            log.Print(err)
            break RECEIVELOOP1
        }
        p, err := spdy.NewSpdyStreamProvider(c, true)
        if err != nil {
            log.Print(err)
            break RECEIVELOOP1
        }
        t := spdy.NewTransport(p)
        MStatus.Set("Listening")
        MHealth.Set(MHealthy)
        go func(t libchan.Transport) {
			log.Print("^^^ in go func 1 in receiver.Start")
			chanCount := 0
			RECEIVELOOP2:
			for {
                receiver, err := t.WaitReceiveChannel()
                if err != nil {
                    log.Print(err)
                    break RECEIVELOOP2
                }
				chanCount++
                log.Print("--- Received a new channel")

                go func(receiver libchan.Receiver) {
					defer log.Printf("!!!!! Ending receive goroutine...chanCount is %d", chanCount)
					var receivedCount = 0
					RECEIVELOOP3:
                    for {
						log.Printf("^^^ receivedCount in channel %v: %d", receiver, receivedCount)
                        msg, err := br.Listen(receiver)
						log.Printf("Receiver got msg and err: %+v, %+v", msg, err)
                        if err != nil {
                            log.Printf("Error from bridgeReceiver.Listen: %v", err)
                            MMessageReceiveFailureCount.Add(1)
                            break RECEIVELOOP3
                        } else if msg != nil {
							receivedCount++
                            m := msg.(Message)
                            MMessageReceiveSuccessCount.Add(1)
                            //log.Printf("the msg to send over goChannel: %+v", m)
							h := TopicPartitionHash(&m)
							goChanLen := len(cbr.goChannels)
                            //i := TopicPartitionHash(&m)%len(cbr.goChannels)
							i := h%goChanLen
							log.Printf(">>> h, goChanLen, i: %d, %d, %d", h, goChanLen, i)
							//cbr.goChannels[i] <- &m
                            select {
                            case cbr.goChannels[i] <- &m:
                                log.Printf(">>> sent msg to receiver's goChannels[%v]", i)
								break RECEIVELOOP3
                            default:
                                log.Printf("~~~~~ DID NOT send msg to receiver's goChannels[%v]", i)
                            }
                        } else {
                            log.Print("??? both bridgeMessage and err were nil ???")
                            break RECEIVELOOP3
                        }
                    }
					log.Printf("^^^^^ broke out of RECEIVELOOP3 loop! receivedCount in channel %v: %d", receiver, receivedCount)
				}(receiver)
				log.Printf("...... chanCount: %d", chanCount)
            }
			log.Print("^^!!!^^ broke out of RECEIVELOOP2 for loop!")
        }(t)
    }
	log.Print("^^--^^ broke out of RECEIVELOOP1 for loop!")
	MHealth.Set(MFailed)
    return errors.New("ChanBridgeReceiver failed to start")
}

func (bs *bridgeSender) trySend(cbs *ChanBridgeSender, m *Message, c chan ConnState, block chan struct{}, resend bool) (e error) {
	MMessageSendAttemptCount.Add(1)
	select {
	case <-block:
		if !resend {
			cbs.failedMessages = append(cbs.failedMessages, m)
		}
		return errors.New("Sending is blocked")
	default:
	}

	sender, err := bs.senderFunc()
	defer func() {
		log.Print("=== In deferred function")
		if e != nil {
			MMessageSendFailureCount.Add(1)
			if !resend {
				cbs.failedMessages = append(cbs.failedMessages, m)
			}
			log.Printf("!!!!!!!! Failed to send message: %+v", m)
			log.Printf("... send failure error: %+v", err)
			log.Print("Closing block channel")
			close(block)
			log.Print("Sending Disconnected signal")
			c <- Disconnected
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
		log.Printf("------ Receive failed with error %+v in trySend", err)
		return err
	}
	err = sender.Close()
	if err != nil {
		log.Print("=== FAILED to close sender.")
	} else {
		log.Print("=== Closed sender.")
	}
	return err
}

//func (cbs *ChanBridgeSender) sendMessage(m *Message, c chan ConnState, block chan struct{}, resend bool) error {
//    MMessageSendAttemptCount.Add(1)
//    select {
//    case <-block:
//        if !resend {
//            cbs.failedMessages = append(cbs.failedMessages, m)
//        }
//        return errors.New("Sending is blocked")
//    default:
//    }
//    err := cbs.bridgeSender.Send(*m)
//    if err != nil {
//        MMessageSendFailureCount.Add(1)
//        if !resend {
//            cbs.failedMessages = append(cbs.failedMessages, m)
//        }
//        log.Printf("!!!!!!!! Failed to send message: %+v", m)
//        log.Printf("... send failure error: %+v", err)
//        log.Print("Closing block channel")
//        close(block)
//        log.Print("Sending Disconnected signal")
//        c <- Disconnected
//    } else {
//        MMessageSendSuccessCount.Add(1)
//    }
//    return err
//}

func (cbs *ChanBridgeSender) Start(t libchan.Transport, c chan ConnState) {
    block := make(chan struct{})
    fmCount := len(cbs.failedMessages)
    log.Printf("Number of failed messages to resend: %v", fmCount)
    for i := 0; i < fmCount; i++ {
        log.Printf("cbs.failedMessages, len, cap: %v, %v, %v",
            cbs.failedMessages, len(cbs.failedMessages), cap(cbs.failedMessages))
        fm := cbs.failedMessages[i]
        log.Printf("~~~~~~ resending a failed message: %+v", *fm)

		receiver, remoteSender := libchan.Pipe()
		bridgeSender := NewBridgeSender(t.NewSendChannel, receiver, remoteSender)
		err := bridgeSender.trySend(cbs, fm, c, block, false)

        if err != nil {
            log.Printf("!!!!!! resending a failed message failed again: %+v", *fm)
            // This is the first resend failure in the loop, so all previous messages in failedMessages array
            // successfully resent the message. Remove those, but keep the rest in failedMessages array.
            cbs.failedMessages = append(make([]*Message, fmCount-i), cbs.failedMessages[i:]...)
            return
        }
    }
    // Successfully resent all failed messages. Reset failedMessages slice.
    cbs.failedMessages = nil
    log.Printf("Done resending failed messages! cbs.failedMessages, len, cap: %v, %v, %v",
        cbs.failedMessages, len(cbs.failedMessages), cap(cbs.failedMessages))

    // Start a new send stream for each consumer goChannel
    for goChanIndex, msgChan := range cbs.goChannels {
        log.Printf("In cbs.connections loop. goChanIndex: %v", goChanIndex)
        go func(goChanIndex int, block chan struct{}) {
            defer log.Printf("Ending send goroutine for goChanIndex [%v]...", goChanIndex)

            //log.Printf("... in new goroutine for sender's gochannel index [%v]", goChanIndex)
            LOOP:
            for message := range msgChan {
                select {
                case <-block:
                    cbs.failedMessages = append(cbs.failedMessages, message)
                    break LOOP
                default:
                    log.Printf("... read a message from sender's gochannel index [%v]: %+v", goChanIndex, message)
					receiver, remoteSender := libchan.Pipe()
					bridgeSender := NewBridgeSender(t.NewSendChannel, receiver, remoteSender)
                    err := bridgeSender.trySend(cbs, message, c, block, false)
                    if err != nil {
                        MHealth.Set(MFailed)
                        log.Printf("!!!!!! sending  message failed: %+v", message)
                    }
                }
            }
        }(goChanIndex, block)
    }
    MHealth.Set(MHealthy)
}

