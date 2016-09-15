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
    Close()
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
    log.Printf("... the msg string: %+v", string(bm.Msg.Value))
    response, err := bs.dispatch(bm)
    if err != nil {
        log.Printf("Failed to dispatch a BridgeMessage: %+v", err)
        return err
    }
    if response.Err != nil {
        log.Printf("Receiver sent an error response: %+v", response.Err)
    }
    return response.Err
}

func (bs *bridgeSender) Close() {
    log.Print("Closing bridgeSender...")
    bs.remoteSender.Close()
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
    log.Printf("... the received msg string: %+v", string(bridgeMessage.Msg.Value))
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
    Connected ConnState     = iota
    Disconnected ConnState  = iota
    Stopping ConnState      = iota
)

type ChanBridgeSender struct {
    goChannels      []chan *Message
    bridgeSender    BridgeSender
}

type ChanBridgeReceiver struct {
    goChannels      []chan *Message
    listenUrl       string
    bridgeReceiver  BridgeReceiver
}

type bridgeEndpoint interface {
    Start() error
    Stop()
}

func tryConnect(be bridgeEndpoint, c chan ConnState) {
    for {
        select {
        case cs := <-c:
            switch {
            case cs == Connected:
                log.Printf("%v is connected.", be)
            case cs == Disconnected:
                log.Printf("%v is disconnected...restarting.", be)
                err := be.Start()
                if err != nil {
                    c <-Disconnected
                }
            case cs == Stopping:
                log.Printf("%v is stopping...", be)
                be.Stop()
                log.Printf("%v is stopped.", be)
                break
            }
        default:
        }
    }
}

func (cb *ChanBridge) Start() {
    cb.connStateChan <-Disconnected
    if cb.receiver != nil {
        log.Print("Trying to connect ChanBridgeReceiver...")
        tryConnect(cb.receiver, cb.connStateChan)
    }
    if cb.sender != nil {
        log.Print("Trying to connect ChanBridgeSender...")
        tryConnect(cb.sender, cb.connStateChan)
    }
}

func (cb *ChanBridge) Stop() {
    log.Print("Stopping ChanBridge")
    cb.connStateChan <-Stopping
}

func (cbr *ChanBridgeReceiver) Stop() {
    log.Print("Closing ChanBridgeReceiver's goChannels...")
    for _, ch := range cbr.goChannels {
        close(ch)
    }
}

func (cbs *ChanBridgeSender) Stop() {
    log.Print("Closing ChanBridgeSender...")
    cbs.bridgeSender.Close()
}

func NewChanBridgeSender(goChannels []chan *Message, remoteUrl string) *ChanBridgeSender {
    receiver, remoteSender := libchan.Pipe()
    var client net.Conn
    var err error
    var transport libchan.Transport
    useTLS := os.Getenv("USE_TLS")
    log.Printf("'USE_TLS' env value: %v", useTLS)

    // Resolve remote endpoint to multiple A-record IPs and try each until success
    remoteHost, remotePort, err := net.SplitHostPort(remoteUrl)
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
    bridgeSender := NewBridgeSender(transport.NewSendChannel, receiver, remoteSender)
    return &ChanBridgeSender{
        goChannels:     goChannels,
        bridgeSender:   bridgeSender,
    }
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
    }
}


func (cbr *ChanBridgeReceiver) Start() error {
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
    defer listener.Close()
    bridgeReceiver := NewBridgeReceiver()
    for {
        c, err := listener.Accept()
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

        go func() {
            for {
                receiver, err := t.WaitReceiveChannel()
                if err != nil {
                    log.Print(err)
                    break
                }
                log.Print("--- Received a new channel")

                go func() {
                    for {
                        msg, err := bridgeReceiver.Listen(receiver)
                        if err != nil {
                            log.Printf("Error from bridgeReceiver.Listen: %v", err)
                            break
                        } else if msg != nil {
                            m := msg.(Message)
                            log.Printf("the msg to send over goChannel: %+v", m)
                            i := TopicPartitionHash(&m)%len(cbr.goChannels)
                            cbr.goChannels[i] <- &m
                            log.Printf("sent msg to receiver's goChannels[%v]", i)
                        } else {  // err == nil && msg == nil means sender sent an EOF
                            log.Print("Sender sent an EOF.")
                            break
                        }
                    }
                }()
            }
        }()
    }
    return errors.New("ChanBridgeReceiver disconnected")
}


func (cbs *ChanBridgeSender) Start() error {
    for goChanIndex, msgChan := range cbs.goChannels {
        log.Printf("In cbs.connections loop. goChanIndex: %v", goChanIndex)
        go func() {
            for message := range msgChan {
                var err error
                go func() {
                    err = cbs.bridgeSender.Send(*message)
                    if err != nil {
                        log.Printf("!!!!!!!! Failed to send message: %+v", message)
                        log.Printf("... send failure error: %+v", err)
                        panic("Failed to send a message")
                    }
                }()
            }
        }()
    }
    return nil
}

