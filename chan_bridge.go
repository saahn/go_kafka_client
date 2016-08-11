package go_kafka_client

import (
    "log"
    "net"
    "github.com/docker/libchan"
    "github.com/docker/libchan/spdy"
)

type ChanBridge struct {
    sender      *ChanBridgeSender
    receiver    *ChanBridgeReceiver
}

func NewChanBridge(sender *ChanBridgeSender, receiver *ChanBridgeReceiver) *ChanBridge {
    return &ChanBridge{
        sender:     sender,      // the "client"
        receiver:   receiver,    // the "server"
    }
}

func (cb *ChanBridge) startSender() {
    cb.sender.Connect()
    cb.sender.Start()
}

func (cb *ChanBridge) startReceiver() {
    cb.receiver.Listen()
}

func (cb *ChanBridge) Start() {
    if cb.receiver != nil {
        cb.startReceiver()
        log.Print("Started bridge receiver.")
    }
    if cb.sender != nil {
        cb.startSender()
        log.Print("Started bridge sender.")
    }
}

type BridgeConn struct {
    sender          libchan.Sender
    receiver        libchan.Receiver
    remoteSender    libchan.Sender  // TODO: remove? currently not used, since receiver does not send anything back
}

type BridgeMessage struct {
    msg             Message
    goChanIndex     int
    bridgeChan      libchan.Sender
}

type ChanBridgeSender struct {
    goChannels      []chan *Message   // consumer messages via go channel
    remoteUrl       string
    connections     map[int]BridgeConn
}

func NewChanBridgeSender(goChannels []chan *Message, remoteUrl string) *ChanBridgeSender {
    return &ChanBridgeSender{
        goChannels: goChannels,
        remoteUrl:  remoteUrl,
        connections: make(map[int]BridgeConn),
    }
}

func (cbs *ChanBridgeSender) connect() *BridgeConn {
    var client net.Conn
    var err error

    client, err = net.Dial("tcp", cbs.remoteUrl)
    if err != nil {
        log.Fatal(err)
    }
    p, err := spdy.NewSpdyStreamProvider(client, false)
    if err != nil {
        log.Fatal(err)
    }
    transport := spdy.NewTransport(p)
    sender, err := transport.NewSendChannel()
    if err != nil {
        log.Fatal(err)
    }

    receiver, remoteSender := libchan.Pipe()
    return &BridgeConn{
        sender:         sender,
        receiver:       receiver,
        remoteSender:   remoteSender,
    }
}

func (cbs *ChanBridgeSender) Connect() {
    for i, _ := range cbs.goChannels {
        bridgeConn := cbs.connect()
        cbs.connections[i] = *bridgeConn
    }
}

func (cbs *ChanBridgeSender) Start() {
    for goChanIndex, bridgeConn := range cbs.connections {
        go func() {
            for message := range cbs.goChannels[goChanIndex] {
                bridgeMessage := &BridgeMessage{
                    msg:            *message,
                    goChanIndex:    goChanIndex,
                    bridgeChan:     bridgeConn.remoteSender,
                }
                var err error
                if err != nil {
                    log.Fatal(err)
                }
                err = bridgeConn.sender.Send(*bridgeMessage)
                if err != nil {
                    log.Fatal(err)
                }
            }
        }()
    }
}

type ChanBridgeReceiver struct {
    goChannels      []chan *Message   // target go channels for producer messages
    listenUrl       string
}

func NewChanBridgeReceiver(goChannels []chan *Message, listenUrl string) *ChanBridgeReceiver {
    return &ChanBridgeReceiver{
        goChannels: goChannels,
        listenUrl:  listenUrl,
    }
}

func (cbr *ChanBridgeReceiver) Listen() {
    var listener net.Listener
    var err error
    listener, err = net.Listen("tcp", cbr.listenUrl)
    if err != nil {
        log.Fatal(err)
    }
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

                go func() {
                    for {
                        bridgeMessage := &BridgeMessage{}
                        err := receiver.Receive(bridgeMessage)
                        if err != nil {
                            log.Print(err)
                            break
                        }
                        cbr.goChannels[bridgeMessage.goChanIndex] <- &(bridgeMessage.msg)
                    }
                }()
            }
        }()
    }
}
