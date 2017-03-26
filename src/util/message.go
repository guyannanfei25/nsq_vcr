package util

import (
    "time"
    "fmt"
    "logger"
    "encoding/binary"
)

const (
    minValidMsgLength = 4
    headerLength      = 4
)

type Message struct {
    arrive     time.Time
    body       []byte // raw
    serialize  []byte // header + raw
    Topic      string // for convenient
}

func NewMessage(body []byte) *Message {
    bLen := len(body)
    serialize := make([]byte, bLen + 4, bLen + 4)
    binary.BigEndian.PutUint32(serialize[:4], uint32(bLen))
    logger.Debugf("Got msg body len[%d], msg serialize len[%d]\n", len(body), len(serialize))
    copy(serialize[4:], body)
    // logger.Debugf("msg serialize[%v]\n", serialize)


    return &Message{
        arrive: time.Now(),
        body:   body,
        serialize: serialize,
    }
}

// decode deserializes data: header(len(raw data)4 byte bigendia) + raw_data
// to Message.
func DecodeMessage(b []byte) (*Message, error) {
    bLen := len(b)
    if bLen < minValidMsgLength {
        logger.Errorf("invalid message buffer size (%d)\n", bLen)
        return nil, fmt.Errorf("invalid message buffer size (%d)", bLen)
    }

    msgLen := binary.BigEndian.Uint32(b[:4])
    if bLen != (int(msgLen) + minValidMsgLength) {
        logger.Errorf("invalid message buffer header show len[%d] not equal real[%d]\n", (msgLen + 4), bLen)

        return nil, fmt.Errorf("invalid message buffer header show len[%d] not equal real[%d]", (msgLen + 4), bLen)
    }

    var msg Message
    msg.body = b[4:]
    logger.Debugf("DecodeMessage success, msg len[%d]\n", msgLen)

    return &msg, nil
}

func (m *Message) RawBytes() []byte {
    return m.body
}

// header + rawbytes
func (m *Message) Serialize() []byte {
    return m.serialize
}
