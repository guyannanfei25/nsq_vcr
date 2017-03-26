package record

import (
    "util"
    "logger"
    "sync"
    "os"
    "os/signal"
    "syscall"

    sj      "go-simplejson"
    // nsq      "github.com/nsqio/go-nsq"
)

type Record struct {
    name       string
    notify     chan bool // close notify
    
    writerDirs []string
    lookupds   []string
    topics     []string
    channel    string
    // consumer   []*nsq.Consumer

    dirDaemons []*DirDaemon
    sig        chan os.Signal // cap systel signal

    wg         *sync.WaitGroup
}

func Main(ctx *sj.Json) {
    r := NewRecord(ctx)
    signal.Notify(r.sig, syscall.SIGINT, syscall.SIGTERM)
    r.Process()

    logger.Debugf("Record Main exit\n")
}

func NewRecord(ctx *sj.Json) *Record {
    name := ctx.Get("main").Get("name").MustString("DefaultRecord")
    writerDirs := ctx.Get("main").Get("write_dirs").MustStringArray()
    lookupds, err := util.GetLookupdAddrs(ctx)
    if err != nil {
        logger.Fatalf("Get lookup addrs err[%s]\n", err)
    }
    topics := ctx.Get("main").Get("nsq").Get("backup_topics").MustStringArray()
    channel := ctx.Get("main").Get("nsq").Get("channel").MustString()
    maxInFlight := ctx.Get("main").Get("nsq").Get("max-in-flight").MustInt()
    timeOut := ctx.Get("main").Get("nsq").Get("timeout_sec").MustInt()
    isGz := ctx.Get("main").Get("is_gz").MustBool(true)
    timePattern := ctx.Get("main").Get("time-pattern").MustString("2016-01-02-13-04-05.000")
    filenameFormat := ctx.Get("main").Get("file_name_pattern").MustString()
    maxSizePerFile := ctx.Get("main").Get("max-size-per-file-m").MustInt(300)
    maxSizePerFile = maxSizePerFile * 1024 * 1024

    record := &Record{
        name: name,
        notify: make(chan bool),
        sig: make(chan os.Signal),
        writerDirs: writerDirs,
        lookupds: lookupds,
        topics: topics,
        channel: channel,
        wg: new(sync.WaitGroup),
    }

    dirDaemons := make([]*DirDaemon, 0, 10)
    for _, dir := range writerDirs {
        for _, topic := range topics {
            dirDaemon := NewDirDaemon(record.notify, dir, topic, channel, 
            timePattern, filenameFormat, timeOut, maxSizePerFile, maxInFlight, 
            isGz, lookupds)

            dirDaemons = append(dirDaemons, dirDaemon)
        }
    }

    record.dirDaemons = dirDaemons
    return record
}

func (r *Record) Process() {
    logger.Debugf("Record[%s] start processing...\n", r.name)

    // sig
    r.wg.Add(1)
    go func() {
        <- r.sig
        logger.Debugf("Get quit sig\n")
        r.Close()
        r.wg.Done()
    }()

    for _, dirDaemon := range r.dirDaemons {
        r.wg.Add(1)
        go func(dirDaemon *DirDaemon) {
            defer r.wg.Done()

            logger.Debugf("DirDaemon[%s] start processing\n", dirDaemon)
            dirDaemon.Process()
            logger.Debugf("DirDaemon[%s] end processing\n", dirDaemon)
        }(dirDaemon)
    }
    r.wg.Wait()
    logger.Debugf("Record[%s] exit Process\n", r.name)
}

func (r *Record) Close() {
    logger.Debugf("Record[%s] start ending\n", r.name)
    close(r.notify)
}
