package go_kafka_client

import (
    "log"
    "net"
    "github.com/docker/libchan"
    "github.com/docker/libchan/spdy"
    "github.com/dmcgowan/msgpack"
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

// TODO: delete after debugging
type SimpleMessage struct {
    Msg             string
    Seq             int
    GoChanIndex     int
}

// TODO: delete after debugging
func (sm *SimpleMessage) MarshalMsgpack() ([]byte, error) {
    log.Printf("marshalling a SimpleMessage: %+v", sm)
    bytesWritten, err := msgpack.Marshal(sm.Msg, sm.Seq, sm.GoChanIndex)
    log.Printf("MarshalMsgpack wrote %v bytes and got err: %+v", bytesWritten, err)
    log.Printf("marshalling SimpleMessage result: %+v", string(bytesWritten))
    return bytesWritten, nil
}

// TODO: delete after debugging
func (sm *SimpleMessage) UnmarshalMsgpack(data []byte) error {
    log.Printf("unmarshalling data into a SimpleMessage: %+v", data)
    err := msgpack.Unmarshal(data, &sm.Msg, &sm.Seq, &sm.GoChanIndex)
    log.Printf("the unmarshalled SimpleMessage: %+v", *sm)
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
        if useBridgeMessage {
            /* BridgeMessage */
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
        } else {
            /* SimpleMessage */
            go func() {
                for message := range cbs.goChannels[goChanIndex] {
                    simpleMessage := &SimpleMessage{
                        Msg:            string(message.Value),
                        Seq:            100,
                        GoChanIndex:    goChanIndex,
                    }
                    var err error
                    if err != nil {
                        log.Fatal(err)
                    }
                    log.Printf("original SimpleMessage message: %+v", message)
                    log.Printf("Sending SimpleMessage: %+v", *simpleMessage)
                    sender, err := bridgeConn.Transport.NewSendChannel()
                    err = sender.Send(simpleMessage)
                    if err != nil {
                        log.Fatal(err)
                    }
                    sender.Close()
                    log.Print("--- closed send channel")
                }
            }()
        }
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

                // TODO: Remove debugging condition
                if useBridgeMessage {
                    /* BridgeMessage */
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
                } else {
                    /* SimpleMessage */
                    go func() {
                        log.Print(">>>>> Receiving")
                        simpleMessage := &SimpleMessage{}
                        log.Printf("new SimpleMessage: %+v", simpleMessage)
                        err := receiver.Receive(simpleMessage)
                        log.Printf("received SimpleMessage: %+v", *simpleMessage)
                        log.Printf("received SimpleMessage.msg is %+v of type %T", simpleMessage.Msg, simpleMessage.Msg)
                        log.Printf("the SimpleMessage.Msg: %+v", &simpleMessage.Msg)
                        log.Printf("the cbr.goChannels: %+v", cbr.goChannels)

                        // Temporary hack b/c go channel expects Message
                        msg := &Message{
                            Key: []byte("foo"),
                            Value: []byte(simpleMessage.Msg),
                            DecodedValue: simpleMessage.Msg,
                            Topic: "bridge_test1",
                            Partition: 0,
                            Offset: 1,
                            HighwaterMarkOffset: 2,
                        }
                        log.Printf("the msg to send over goChannel: %+v", msg)
                        cbr.goChannels[0] <- msg
                        log.Printf("the chan_bridge_receiver's gochannels: %+v", cbr.goChannels)
                        if err != nil {
                            log.Print(err)
                        }
                    }()
                }
            }
        }()
    }
}
