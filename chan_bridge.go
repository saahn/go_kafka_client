package go_kafka_client

import (
    "log"
    "net"
    "os"
    "github.com/docker/libchan"
    "github.com/docker/libchan/spdy"
    "github.com/dmcgowan/msgpack"
    "crypto/tls"
)

var useBridgeMessage bool = true

type ChanBridge struct {
    sender      *ChanBridgeSender
    receiver    *ChanBridgeReceiver
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
    Transport    libchan.Transport
    Sender       libchan.Sender     // TODO: remove? no longer used.
    Receiver     libchan.Receiver   // TODO: remove? no longer used.
    //RemoteSender libchan.Sender   // TODO: remove? not used, since receiver does not send anything back
}

type BridgeMessage struct {
    Msg         Message
    Seq         int
    //BridgeChan  libchan.Sender
}

func (m Message) MarshalMsgpack() ([]byte, error) {
    bytesWritten, err := msgpack.Marshal(m.Key, m.Value, m.DecodedKey, m.DecodedValue, m.Topic, m.Partition, m.Offset, m.HighwaterMarkOffset)
    return bytesWritten, err
}

func (m *Message) UnmarshalMsgpack(data []byte) error {
    err := msgpack.Unmarshal(data, &m.Key, &m.Value, &m.DecodedKey, &m.DecodedValue, &m.Topic, &m.Partition, &m.Offset, &m.HighwaterMarkOffset)
    return err
}

func (bm BridgeMessage) MarshalMsgpack() ([]byte, error) {
    log.Printf("marshalling a BridgeMessage: %+v", bm)
    bytesWritten, err := msgpack.Marshal(bm.Msg, bm.Seq)
    log.Printf("MarshalMsgpack wrote %v and got err: %+v", bytesWritten, err)
    log.Printf("marshalled BridgeMessage bytes casted to string: %+v", string(bytesWritten))
    return bytesWritten, err
}

func (bm *BridgeMessage) UnmarshalMsgpack(data []byte) error {
    log.Printf("Data to be unmarshalled into a BridgeMessage: %+v", data)
    err := msgpack.Unmarshal(data, &bm.Msg, &bm.Seq)
    log.Printf("The unmarshalled BridgeMessage: %+v", *bm)
    return err
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
    useTLS := os.Getenv("USE_TLS")
    log.Printf("'USE_TLS' env value: %v", useTLS)
    if useTLS != "" {
        log.Print("Starting client with TLS support")
        client, err = tls.Dial("tcp", cbs.remoteUrl, &tls.Config{InsecureSkipVerify: true})
    } else {
        client, err = net.Dial("tcp", cbs.remoteUrl)
    }
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

    receiver, _ := libchan.Pipe()
    return &BridgeConn{
        Transport:      transport,
        Sender:         sender,
        Receiver:       receiver,
        //RemoteSender:   remoteSender,
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
        log.Printf("In cbs.connections loop. goChanIndex: %v", goChanIndex)
        // TODO: Remove debugging condition
        go func() {
            for message := range cbs.goChannels[goChanIndex] {
                bridgeMessage := &BridgeMessage{
                    Msg:            *message,
                    Seq:            30,
                    //BridgeChan:     bridgeConn.RemoteSender,
                }
                var err error
                log.Printf("original gochan Message: %+v", message)
                log.Printf("Sending BridgeMessage: %+v", *bridgeMessage)
                sender, err := bridgeConn.Transport.NewSendChannel()
                if err != nil {
                    log.Fatal(err)
                }
                err = sender.Send(bridgeMessage)
                if err != nil {
                    log.Fatal(err)
                }
                sender.Close()  // TODO: maintain stream connection rather than close per message
                log.Print("--- closed send channel")
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
    cert := os.Getenv("TLS_CERT")
    key := os.Getenv("TLS_KEY")
    log.Printf("'TLS_CERT' env value: %v", cert)
    log.Printf("'TLS_KEY' env value: %v", key)

    var listener net.Listener
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

        listener, err = tls.Listen("tcp", cbr.listenUrl, tlsConfig)
        if err != nil {
            log.Fatal(err)
        }
    } else {
        var err error
        listener, err = net.Listen("tcp", cbr.listenUrl)
        if err != nil {
            log.Fatal(err)
        }
    }

    for {
        c, err := listener.Accept()
        log.Print("========= in listener.Accept =========")
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
            log.Print("In new receiver goroutine")
            for {
                receiver, err := t.WaitReceiveChannel()
                log.Print("----- new channel created by remote sender")
                if err != nil {
                    log.Printf("Error in ChanBridgeReceiver.Listen: %+v", err)
                    break
                }

                go func() {
                    log.Print(">>>>> Receiving")
                    bridgeMessage := &BridgeMessage{}
                    err := receiver.Receive(bridgeMessage)
                    if err != nil {
                        log.Print(err)
                    }
                    log.Printf("received BridgeMessage: %+v", *bridgeMessage)
                    //log.Printf("received bridgeMessage.msg is %+v of type %T", bridgeMessage.Msg, bridgeMessage.Msg)
                    log.Printf("the cbr.goChannels: %+v", cbr.goChannels)
                    log.Printf("the msg to send over goChannel: %+v", bridgeMessage.Msg)
                    i := TopicPartitionHash(&bridgeMessage.Msg)%len(cbr.goChannels)
                    cbr.goChannels[i] <- &bridgeMessage.Msg
                    log.Printf("sent msg to receiver's goChannels[%v]", i)
                }()
            }
        }()
    }
}
