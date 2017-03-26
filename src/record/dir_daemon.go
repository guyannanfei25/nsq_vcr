package record

import (
    "util"
    "logger"
    "sync"
    "sync/atomic"
    "bytes"
    "os"
    "io"
    "compress/gzip"
    "path/filepath"
    "time"
    "strings"
    "fmt"

    nsq      "github.com/nsqio/go-nsq"
)

// every DirDaemon monitor a dir for a topic with a channel 
// 一个三元组(dirname, topic, channel)决定一个 DirDaemon
type DirDaemon struct {
    topic      string
    channel    string
    consumer   *nsq.Consumer
    timeOut    int // seconds
    dirname    string
    timePattern string
    maxSizePerFile int
    lookupds   []string
    maxInFlight    int
    routeChan  chan *nsq.Message

    msgHolder  []*util.Message
    content    bytes.Buffer
    isGz       bool
    msgNum     uint64 // receive msg num
    memSize    uint64 // hold buffer size

    notify     chan bool // notify to close
    wg         sync.WaitGroup

    // file info
    out          *os.File
    writer       io.Writer
    gzipWriter   *gzip.Writer
    filesize     int64
	lastOpenTime time.Time
	lastFilename string
    rotateInterval   time.Duration // default 60s
    rotateSize       int64         // default 300MB = 300 * 1024 * 1024B
    filenameFormat   string
    compressionLevel int // default DefaultCompression

}

func NewDirDaemon(notify chan bool, dirname, topic, channel, timePattern, filenameFormat string,
     timeOut, maxSizePerFile, maxInFlight int, isGz bool,
     lookupds []string) *DirDaemon {

    if dirname == "" || topic == "" || channel == "" || timePattern == "" {
        logger.Debugf("dirname[%s] topic[%s] channel[%s] or timePattern[%s] is nil\n", 
        dirname, topic, channel, timePattern)
        return nil
    }

    if len(lookupds) == 0 {
        logger.Errorf("NewDirDaemon got no lookupds, please check!!!\n")
        return nil
    }

    dirDaemon := &DirDaemon{
        topic: topic,
        channel: channel,
        timeOut: timeOut,
        dirname: dirname,
        routeChan: make(chan *nsq.Message),
        timePattern: timePattern,
        maxSizePerFile: maxSizePerFile,
        isGz: isGz,
        notify: notify,
        maxInFlight: maxInFlight,
        filenameFormat: filenameFormat,
        rotateInterval: 60 * time.Second,
        rotateSize: int64(maxSizePerFile),
        compressionLevel: gzip.DefaultCompression,
    }

    dirDaemon.filenameFormatConv()

    dirDaemon.content.Reset()
    atomic.StoreUint64(&dirDaemon.msgNum, 0)

    config := nsq.NewConfig()
    config.MaxInFlight = maxInFlight
    consumer, err := nsq.NewConsumer(topic, channel, config)
    if err != nil {
        logger.Errorf("NewDirDaemon NewConsumer err[%s]\n", err)
        return nil
    }

    consumer.AddHandler(nsq.HandlerFunc(func(m *nsq.Message) error {
        m.DisableAutoResponse()
        dirDaemon.routeChan <- m
        return nil
    }))

    if err := consumer.ConnectToNSQLookupds(lookupds); err != nil {
        logger.Errorf("NewDirDaemon consumer ConnectToNSQLookupds [%v] err[%s]\n",
        lookupds, err)
        return nil
    }

    dirDaemon.consumer = consumer

    logger.Debugf("New DirDaemon dirname[%s] topic[%s] channel[%s] success\n", 
    dirname, topic, channel)
    return dirDaemon
}

// must call after init
func (d *DirDaemon) filenameFormatConv() {
    fNameP1 := strings.Replace(d.filenameFormat, "write_dirs", d.dirname, -1)
    fNameP2 := strings.Replace(fNameP1, "topic", d.topic, -1)
    fNameP3 := strings.Replace(fNameP2, "channel", d.channel, -1)
    d.filenameFormat = fNameP3
}

// for debug
func (d *DirDaemon) String() string {
    return fmt.Sprintf("dir{%s}/topic{%s}/channel{%s}", d.dirname, d.topic, 
    d.channel)
}

func (d *DirDaemon) Process() {
    // TODO: can configure
    ticker := time.NewTicker(time.Duration(30) * time.Second)
    for {
        select {
        case msg := <- d.routeChan:
            // msg := util.NewMessage(body)
            // if msg == nil {
                // logger.Errorf("Receive from nsqd body[%v], cannot decode\n", body)
                // continue
            // }
            // logger.Debugf("%s Now Process msg[%v]\n", d, msg)
            d.coreProcess(msg)
        case <- time.After(time.Duration(d.timeOut) * time.Second):
            logger.Debugf("After [%d]s, %s/%s/%s has receive nothing\n",
            d.timeOut, d.dirname, d.topic, d.channel)
            continue
        case <- d.notify:
            logger.Debugf("Receive end cmd, exiting...")
            goto Exit
        case <- ticker.C:
            if d.needsFileRotate() {
                d.updateFile()
            }
        }
    }
Exit:
    ticker.Stop()
    d.Close()
}

func (d *DirDaemon) coreProcess(nMsg *nsq.Message) error {
    if d.needsFileRotate() {
        d.updateFile()
    }

    msg := util.NewMessage(nMsg.Body)
    if msg == nil {
        logger.Errorf("Receive from nsqd body[%v], cannot decode\n", nMsg.Body)
    }

    atomic.AddUint64(&d.msgNum, 1)
    _, err := d.writer.Write(msg.Serialize())
    if err != nil {
        logger.Fatalf("Error: Writing Message to disk err[%s]\n", err)
        // TODO
        return err
    }

    nMsg.Finish()

    return nil
}

func (d *DirDaemon) Write(p []byte) (n int, err error) {
    atomic.AddInt64(&d.filesize, int64(len(p)))
    return d.out.Write(p)
}

func (d *DirDaemon) Sync() error {
    var err error
    if d.gzipWriter != nil {
        d.gzipWriter.Close()
        err = d.out.Sync()
        logger.Errorf("gzipWriter close err[%s]\n", err)
        d.gzipWriter, _ = gzip.NewWriterLevel(d, d.compressionLevel)
        d.writer = d.gzipWriter
    } else {
        err = d.out.Sync()
        logger.Errorf("Sync err[%s]\n", err)
    }
    return err
}

func (d *DirDaemon) calculateCurrentFilename() string {
    timeStr := time.Now().Format(d.timePattern)
    return strings.Replace(d.filenameFormat, "time-pattern", timeStr, -1)
}

func (d *DirDaemon) needsFileRotate() bool {
    if d.out == nil {
        return true
    }

    // filename := d.calculateCurrentFilename()
    if d.rotateInterval > 0 {
        if s := time.Since(d.lastOpenTime); s > d.rotateInterval {
            logger.Debugf("%s since last open, need rotate", s)
            return true
        }
    }

    if d.rotateSize > 0 && d.filesize > d.rotateSize {
        logger.Debugf("%s current %d bytes, need rotate", d.out.Name(), d.filesize)
        return true
    }

    return false
}

func (d *DirDaemon) updateFile() {
    d.rotate()

    filename := d.calculateCurrentFilename()
    if filename == d.lastFilename {
        // TODO should not happend
        logger.Errorf("Should not happend filename same to lastFilename[%s]\n",
        d.lastFilename)
    }

    d.lastFilename = filename
    d.lastOpenTime = time.Now()

    dir, _ := filepath.Split(filename)
    if dir != "" {
        err := os.MkdirAll(dir, 0770)
        if err != nil {
            logger.Fatalf("Create dir[%s] err[%s]\n", dir, err)
        }
    }

    openFlag := os.O_WRONLY | os.O_CREATE
    if d.isGz {
        openFlag |= os.O_EXCL
    } else {
        openFlag |= os.O_APPEND
    }
    var err error
    d.out, err = os.OpenFile(filename, openFlag, 0666)
    if err != nil {
        if os.IsExist(err) {
            logger.Debugf("File already exists: %s\n", filename)
        }
        logger.Fatalf("Unable to open file[%s] err[%s]\n", filename, err)
    }

    logger.Debugf("Opening file[%s]\n", filename)
    fi, err := d.out.Stat()
    if err != nil {
        logger.Fatalf("Unable to stat file[%s] err[%s]\n", filename, err)
    }

    // TODO: filesize must zero, check
    d.filesize = fi.Size()
    logger.Debugf("Rotate file[%s] size[%d]\n", d.lastFilename, d.filesize)

    if d.isGz {
        d.gzipWriter, _ = gzip.NewWriterLevel(d, d.compressionLevel)
        d.writer = d.gzipWriter
    } else {
        d.writer = d
    }
}

func (d *DirDaemon) rotate() {
    if d.out != nil {
        d.out.Sync()
        if d.gzipWriter != nil {
            d.gzipWriter.Close()
        }
        d.out.Close()
        d.out = nil
    }

    logger.Debugf("%s rotate, get msg num[%d], filesize[%d]\n", d, d.msgNum, d.filesize)

    // generate final file name with msg num
    nameWithMsgNum := strings.Replace(d.lastFilename, "msg-num",
    fmt.Sprintf("%d", d.msgNum), -1)
    logger.Debugf("Now %s rename %s to %s\n", d, d.lastFilename, nameWithMsgNum)
    util.AtomicRename(d.lastFilename, nameWithMsgNum)

    atomic.StoreUint64(&d.msgNum, 0)
}

func (d *DirDaemon) Close() {
    d.consumer.Stop()
    // before close d.routeChan must ensure nsq.consumer stop
    <- d.consumer.StopChan
    logger.Debugf("nsq consumer StopChan can read\n")

    // close(d.routeChan)

    // read left msg
    // for body := range d.routeChan {
        // msg := util.NewMessage(body)
        // atomic.AddUint64(&d.msgNum, 1)
        // if msg == nil {
            // logger.Errorf("Receive from nsqd body[%v], cannot decode\n", body)
            // continue
        // }
    // }
    d.rotate()
    logger.Debugf("DirDaemon dirname[%s] topic[%s] channel[%s] Exit!\n",
    d.dirname, d.topic, d.channel)
}
