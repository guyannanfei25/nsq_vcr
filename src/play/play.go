package play

import (
    "util"
    "logger"
    "sync"
    "os"
    "time"
    "os/signal"
    "syscall"

    sj      "go-simplejson"
    nsq      "github.com/nsqio/go-nsq"
)

type Play struct {
    name     string
    notify   chan bool
    msgChan  chan *util.Message

    monitorDirs  []string
    nsqdAddrs    []string
    topics       []string
    producers    []*nsq.Producer
    dirDaemons   []*DirDaemon
    dirDaeWg     *sync.WaitGroup

    sig          chan os.Signal // cap systel signal

    wg           *sync.WaitGroup
}

func Main(ctx *sj.Json) {
    logger.Debugf("Play Main start\n")
    p := NewPlay(ctx)
    if p == nil {
        logger.Debugf("New Play is nil, check your conf")
        return
    }
    signal.Notify(p.sig, syscall.SIGINT, syscall.SIGTERM)
    p.Process()

    logger.Debugf("Play Main exit\n")
}

func NewPlay(ctx *sj.Json) *Play {
    name := ctx.Get("main").Get("name").MustString("DefaultPlay")
    nsqdAddrs := ctx.Get("main").Get("nsq").Get("nsqd_addrs").MustStringArray()
    if len(nsqdAddrs) < 1 {
        logger.Errorf("No nsqd addr found\n")
        return nil
    }

    producers := make([]*nsq.Producer, 0, 5)
    config := nsq.NewConfig()
    for _, nsqdAddr := range nsqdAddrs {
        producer, err := nsq.NewProducer(nsqdAddr, config)
        if err != nil {
            logger.Errorf("%s new Producer[%s] err[%s]\n", name, nsqdAddr, err)
            return nil
        }

        producers = append(producers, producer)
    }

    monitorInfo := ctx.Get("main").Get("monitor_info").MustJsonArray()
    if len(monitorInfo) < 1 {
        logger.Debugf("No monitor_info found\n")
        return nil
    }

    play := &Play{
        name: name,
        nsqdAddrs: nsqdAddrs,
        sig: make(chan os.Signal),
        wg:  new(sync.WaitGroup),
        notify: make(chan bool),
        msgChan: make(chan *util.Message, 5),
        producers: producers,
        dirDaeWg: new(sync.WaitGroup),
    }

    var dirDaemons []*DirDaemon
    for _, mi := range monitorInfo {
        topic := mi.Get("topic").MustString()
        monitorDirs := mi.Get("monitor_dirs").MustStringArray()

        for _, mdir := range monitorDirs {
            dirDaemon := NewDirDaemon(topic, mdir, play.notify, play.msgChan)
            dirDaemons = append(dirDaemons, dirDaemon)
        }
    }

    play.dirDaemons = dirDaemons

    logger.Debugf("New Play success\n")
    return play
}

func (p *Play) Process() {
    logger.Debugf("%s start Process\n", p.name)

    // sig
    p.wg.Add(1)
    go func() {
        <- p.sig
        logger.Debugf("Got exit signal, now start exiting\n")
        p.Close()
        p.wg.Done()
    }()

    p.StartProducers()

    for _, dirDaemon := range p.dirDaemons {
        p.wg.Add(1)
        p.dirDaeWg.Add(1)
        go func(dirDaemon *DirDaemon){
            defer p.wg.Done()
            logger.Debugf("%s start Process\n", dirDaemon)
            dirDaemon.Process()
            logger.Debugf("%s end Process\n", dirDaemon)
            p.dirDaeWg.Done()
        }(dirDaemon)
    }

    p.wg.Wait()
    logger.Debugf("%s end Process\n", p.name)
}

func (p *Play) StartProducers() {
    logger.Debugf("%s Start producers\n", p.name)
    for _, producer := range p.producers {
        p.wg.Add(1)
        go func(producer *nsq.Producer){
            logger.Debugf("now start producer[%s]\n", producer)
            PRODUCERLOOP:
            for {
                select {
                case msg, ok := <- p.msgChan:
                    if !ok {
                        logger.Debugf("msgChan has been closed, exit\n")
                        break PRODUCERLOOP
                    }
                    logger.Debugf("Send msq to producer[%s]\n", producer)
                    err := producer.Publish(msg.Topic, msg.RawBytes())
                    if err != nil {
                        // TODO: retry
                        logger.Errorf("Publish to nsqd[%s] err[%s]\n", producer, err)
                        continue
                    }
                case <- time.After(3 * time.Second):
                    logger.Debugf("After 3s, producer[%s] get nothing\n", producer)
                    continue
                }
            }
            logger.Debugf("producer[%s] end\n", producer)
            p.wg.Done()
        }(producer)
    }

    logger.Debugf("%s start Producers success\n", p.name)
}

func (p *Play) Close() {
    logger.Debugf("%s start exiting\n", p.name)
    close(p.notify)

    p.dirDaeWg.Wait()
    logger.Debugf("All DirDaemons have exit, now can safely close mysqChan\n")
    close(p.msgChan)
}
