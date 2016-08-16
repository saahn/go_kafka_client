package go_kafka_client

import (
    "log"
    "net"
    "github.com/docker/libchan"
    "github.com/docker/libchan/spdy"
    "bytes"
    "fmt"
)

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
    Sender       libchan.Sender
    Receiver     libchan.Receiver
    //RemoteSender libchan.Sender // TODO: remove? currently not used, since receiver does not send anything back
}

type BridgeMessage struct {
    Msg         Message
    Seq         int
    GoChanIndex int
    //BridgeChan  libchan.Sender
}

func (bm BridgeMessage) MarshalMsgpack() ([]byte, error) {
    log.Printf("marshalling a BridgeMessage: %+v", bm)
    var b bytes.Buffer
    bytesWritten, err := fmt.Fprintln(&b, bm.Msg, bm.Seq, bm.GoChanIndex)
    log.Printf("MashalMsgpack wrote %v bytes and got err: %+v", bytesWritten, err)
    result := b.Bytes()
    log.Printf("marshalling result: %+v", result)
    return result, nil
}

func (bm *BridgeMessage) UnmarshalMsgpack(data []byte) error {
    log.Printf("unmarshalling data into a BridgeMessage: %+v", data)
    b := bytes.NewBuffer(data)
    _, err := fmt.Fscanln(b, &bm.Msg, &bm.Seq, &bm.GoChanIndex)
    log.Printf("unmarshalling result in buffer: %+v", *b)
    return err
}

// TODO: delete after debugging
type SimpleMessage struct {
    msg             string
}

// TODO: delete after debugging
func (sm *SimpleMessage) MarshalMsgpack() ([]byte, error) {
    log.Printf("marshalling a SimpleMessage: %+v", *sm)
    result := []byte(sm.msg)
    log.Printf("marshalling result: %+v", result)
    return result, nil
}

// TODO: delete after debugging
func (sm *SimpleMessage) UnmarshalMsgpack(b []byte) error {
	log.Printf("unmarshalling a SimpleMessage: %+v", *sm)
	sm.msg = string(b)
	log.Printf("unmarshalling result: %+v", *sm)
    return nil
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
        go func() {
            for message := range cbs.goChannels[goChanIndex] {
                bridgeMessage := &BridgeMessage{
                    Msg:            *message,
                    Seq:            100,
                    GoChanIndex:    goChanIndex,
                    //BridgeChan:     bridgeConn.RemoteSender,
                }
                var err error
                if err != nil {
                    log.Fatal(err)
                }
                log.Printf("original message: %+v", message)
                log.Printf("Sending msg: %+v", *bridgeMessage)
                sender, err := bridgeConn.Transport.NewSendChannel()
                err = sender.Send(bridgeMessage)
                if err != nil {
                    log.Fatal(err)
                }
                sender.Close()
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
            log.Print("In new receiver goroutine")
            for {
                log.Print(">>>>> waiting to receive...")
                receiver, err := t.WaitReceiveChannel()
                if err != nil {
                    log.Printf("Error in ChanBridgeReceiver.Listen: %+v", err)
                    break
                }

                go func() {
                    log.Print(">>>>> Receiving")
                    bridgeMessage := &BridgeMessage{}
                    log.Printf("new BridgeMessage: %+v", bridgeMessage)
                    err := receiver.Receive(bridgeMessage)
                    log.Printf("received BridgeMessage: %+v", *bridgeMessage)
                    log.Printf("received bridgeMessage.msg is %+v of type %T", bridgeMessage.Msg, bridgeMessage.Msg)
                    log.Printf("the bridgeMessage.Msg: %+v", &bridgeMessage.Msg)
                    log.Printf("the cbr.goChannels: %+v", cbr.goChannels)

                    // Temporary hack b/c unmarshalling does not work for BridgeMessage
                    msg := &Message{
                        Key: []byte("foo"),
                        Value: []byte("bar"),
                        DecodedValue: "bar",
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
        }()
    }
}
