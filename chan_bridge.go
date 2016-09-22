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
    MMode = expvar.NewString("mode")
    MStatus = expvar.NewString("status")
    MConnected = expvar.NewString("connected")

    // Sender metrics
    MMessageSendAttemptCount = expvar.NewInt("message_send_attempt_count")
    MMessageSendSuccessCount = expvar.NewInt("message_send_success_count")

    // Receiver metrics
    MMessageReceiveSuccessCount = expvar.NewInt("message_receive_success_count")
    MMessageReceiveFailedCount = expvar.NewInt("message_receive_failed_count")
)

type SenderFunc func() (libchan.Sender, error)

type BridgeMessage struct {
    Msg             Message
    Seq             int
    ResponseChan    libchan.Sender
}

type BridgeResponse struct {
    Err     error
}


/* BridgeSender */


type BridgeSender interface {
    Send(Message) error
    Close() error
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

func (bs *bridgeSender) Send(m Message) error {
    bm := BridgeMessage{
        Msg: m,
        Seq: 100,
        ResponseChan: bs.remoteSender,
    }
    log.Printf("Sending message: %+v", bm)
    response, err := bs.dispatch(bm)
    if err != nil {
        log.Printf("Failed to dispatch a BridgeMessage: %+v", err)
        return err
    }
    log.Printf("... the sent msg string: %+v", string(bm.Msg.Value))
    if response.Err != nil {
        log.Printf("Receiver sent an error response: %+v", response.Err)
    }
    return response.Err
}

func (bs *bridgeSender) Close() error {
    log.Print("Closing bridgeSender...")
    return nil
}

func (bs *bridgeSender) dispatch(bm BridgeMessage) (*BridgeResponse, error) {
    sender, err := bs.senderFunc()
    if err != nil {
        return nil, err
    }

    if err := sender.Send(bm); err != nil {
        return nil, err
    }
    response := &BridgeResponse{}
    if err := bs.receiver.Receive(response); err != nil {
        return nil, err
    }
    return response, nil
}


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
    log.Printf(">>>>> Received a BridgeMessage: %+v", bridgeMessage)
    log.Printf("_ _ _ the received msg string: %+v", string(bridgeMessage.Msg.Value))
    bridgeResponse.Err = err
    if err != nil {
        log.Printf("Got an error from sender: %v", err)
        if err.Error() == "EOF" {
            log.Print("Ignoring EOF error from sender.")
            return nil, nil
        }
        if err.Error() == "stream does not exist" || bridgeMessage.ResponseChan == nil {
            log.Print("sender's stream is gone")
            return nil, err
        }
    }
    log.Printf("... sending bridgeResponse to Sender: %+v", bridgeResponse)
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
    failedSends     chan *Message
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
    c <-Disconnected
    for cs := range c {
        switch {
        case cs == Connected:
            MConnected.Set("True")
            MStatus.Set("Connected")
            log.Printf("%v is connected.", be)
        case cs == Disconnected:
            log.Printf("%v is disconnected...restarting.", be)
            MStatus.Set("Disconnected")
            err := be.Connect(c)
            if err != nil {
                c <-Disconnected
            }
        case cs == Stopped:
            MStatus.Set("Stopped")
            close(c)
        }
    }
    log.Print("Exited tryConnect loop.")
}

func (cb *ChanBridge) Start() {
    MStatus.Set("Starting...")
    MConnected.Set("False")
    if cb.receiver != nil {
        log.Print("Trying to connect ChanBridgeReceiver...")
        MMode.Set("Receiver")
        tryConnect(cb.receiver, cb.connStateChan)
    }
    if cb.sender != nil {
        log.Print("Trying to connect ChanBridgeSender...")
        MMode.Set("Sender")
        tryConnect(cb.sender, cb.connStateChan)
    }
}

func (cb *ChanBridge) Stop() {
    log.Print("Stopping ChanBridge")
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
    MStatus.Set("Stopping...")
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
        failedSends:    make(chan *Message, 1),
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
    remoteIPs, err := net.LookupHost(remoteHost)
    for {
        var connected = false
        for !connected {
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
    receiver, remoteSender := libchan.Pipe()
    bridgeSender := NewBridgeSender(transport.NewSendChannel, receiver, remoteSender)
    cbs.bridgeSender = bridgeSender
    c <- Connected
    return cbs.Start(c)
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
    for {
        c, err := listener.Accept()
        log.Print("=== In ChanBridgeReceiver's listner.Accept loop ===")
        if err != nil {
            log.Print(err)
            break
        }
        p, err := spdy.NewSpdyStreamProvider(c, true)
        if err != nil {
            log.Print(err)
            break
        }
        t := spdy.NewTransport(p)

        go func(t libchan.Transport) {
            for {
                receiver, err := t.WaitReceiveChannel()
                if err != nil {
                    log.Print(err)
                    break
                }
                log.Print("--- Received a new channel")

                go func() {
                    for {
                        msg, err := br.Listen(receiver)
                        if err == nil && msg == nil {
                            log.Print("Sender sent an EOF.")
                            break
                        }
                        if err != nil {
                            log.Printf("Error from bridgeReceiver.Listen: %v", err)
                            MMessageReceiveFailedCount.Add(1)
                            break
                        } else if msg != nil {
                            m := msg.(Message)
                            MMessageReceiveSuccessCount.Add(1)
                            log.Printf("the msg to send over goChannel: %+v", m)
                            i := TopicPartitionHash(&m)%len(cbr.goChannels)
                            cbr.goChannels[i] <- &m
                            log.Printf("sent msg to receiver's goChannels[%v]", i)
                        }
                    }
                }()
            }
        }(t)
    }
    return errors.New("ChanBridgeReceiver failed to start")
}

func (cbs *ChanBridgeSender) sendMessage(m *Message, c chan ConnState) {
    MMessageSendAttemptCount.Add(1)
    err := cbs.bridgeSender.Send(*m)
    if err != nil {
        log.Printf("!!!!!!!! Failed to send message: %+v", m)
        log.Printf("... send failure error: %+v", err)
        cbs.failedSends <-m
        log.Print("Stopping cbs b/c sendMessage failed.")
        cbs.Stop()
        c <-Disconnected
    } else {
        MMessageSendSuccessCount.Add(1)
    }
}


func (cbs *ChanBridgeSender) Start(c chan ConnState) error {
    select {
    case msg := <-cbs.failedSends:
        log.Printf("!!!!!! resending a failed message: %+v", *msg)
        go cbs.sendMessage(msg, c)
    default:
        log.Print("No failed messages to resend")
    }
    for goChanIndex, msgChan := range cbs.goChannels {
        log.Printf("In cbs.connections loop. goChanIndex: %v", goChanIndex)
        go func() {
            log.Printf("... in new goroutine for sender's gochannel index [%v]", goChanIndex)
            for message := range msgChan {
                log.Printf("... read a message from sender's gochannel index [%v]: %+v", goChanIndex, message)
                go cbs.sendMessage(message, c)
            }
        }()
    }
    return nil
}

