package play

import (
    "util"
    "fmt"
    "time"
    "logger"
    "os"
    "strings"
    "path/filepath"
    "sort"
    "io"
    "bufio"
    "compress/gzip"
    "encoding/binary"
)

// every DirDaemon monitor a dir for a topic
// monitor dirdaemon
type DirDaemon struct {
    topic             string
    dirname           string
    lastProcessFile   string
    checkInterval     time.Duration // default 30s
    msgChan           chan *util.Message
    notify            chan bool

    isGz              bool
    validFilePattern  string
}

// TODO: valid file check
func NewDirDaemon(topic, dirname string, notify chan bool,
                msgChan chan *util.Message) *DirDaemon {
    dirDaemon := &DirDaemon{
        topic: topic,
        dirname: dirname,
        checkInterval: 30 * time.Second,
        msgChan: msgChan,
        lastProcessFile: "",
        notify: notify,
    }

    return dirDaemon
}

func (d *DirDaemon) String() string {
    return fmt.Sprintf("DirDaemon:dirname{%s}/topic{%s}", d.dirname, d.topic)
}

func (d *DirDaemon) Process() {
    PROCESSLOOP:
    for {
        d.coreProcess()
        select {
        case <- d.notify:
            logger.Debugf("%s Get exit notify, now exiting\n", d)
            break PROCESSLOOP
        case <- time.After(d.checkInterval):
            logger.Debugf("%s Sleep %s s, now begin check again\n", d, d.checkInterval)
            continue
        }
    }

    logger.Debugf("Now exit %s Process\n", d)
}

func (d *DirDaemon) coreProcess() error {
    fileList, err := d.getFileList()
    if err != nil {
        logger.Errorf("%s getFileList err[%s]\n", d, err)
        return err
    }

    for _, file := range fileList {
        if d.lastProcessFile != "" {
            if file <= d.lastProcessFile {
                logger.Errorf("ERROR: Should not Happend!!!!!\n")
                continue
            }
        }

        startTime := time.Now()
        d.parseFile(file)
        cost := time.Since(startTime).Seconds()
        logger.Debugf("%s process file[%s] cost [%f]s\n", d, file, cost)
    }
    return nil
}

func (d *DirDaemon) getFileList() ([]string, error) {
    // TODO: wheather hold a mutex lock
    dirFp, err := os.Open(d.dirname)
    if err != nil {
        logger.Errorf("Open dir[%s] err[%s]\n", d.dirname, err)
        return nil, err
    }
    defer dirFp.Close()

    fis, err := dirFp.Readdir(-1)
    if err != nil {
        logger.Debugf("Read dir[%s] err[%s]\n", d.dirname, err)
        return nil, err
    }

    var ret []string

    for _, fi := range fis {
        if fi.Name() == "." || fi.Name() == ".." {
            continue
        }

        if fi.IsDir() {
            logger.Debugf("%s is dir, continue\n", fi.Name())
            continue
        }

        // skip contain msg-num file, because not complete file
        if strings.Index(fi.Name(), "msg-num") != -1 {
            continue
        }

        if !validFile(fi.Name()) {
            logger.Debugf("%s file[%s] not valid, skip\n", d, fi.Name())
            continue
        }

        // skip empty file
        if fi.Size() == 0 {
            logger.Debugf("Skip empty file[%s]\n", fi.Name())
            continue
        }

        ret = append(ret, filepath.Base(fi.Name()))
    }

    sort.Strings(ret)
    logger.Debugf("%s get %d files this time\n", d, len(ret))

    return ret, nil
}

// TODO
func validFile(fileName string) bool {
    return true
}

// relative path
func (d *DirDaemon) parseFile(fileName string) error {
    fullPath := filepath.Join(d.dirname, fileName)
    logger.Debugf("Now parse file[%s]\n", fullPath)

    fp, err := os.OpenFile(fullPath, os.O_RDONLY, 0600)
    if err != nil {
        logger.Errorf("Open file[%s] err[%s]\n", fullPath, err)
        return err
    }
    defer fp.Close()

    var reader *bufio.Reader
    var ioReader io.Reader = fp
    if strings.HasSuffix(fullPath, ".gz") {
        greader, err := gzip.NewReader(fp)
        if err != nil {
            logger.Errorf("gzip NewReader from file[%s] err[%s]\n", fullPath, err)
            return err
        }
        defer greader.Close()
        ioReader = greader
    }

    reader = bufio.NewReader(ioReader)
    for {
        var msgLen uint32
        err := binary.Read(reader, binary.BigEndian, &msgLen)
        if err != nil {
            if err == io.EOF {
                logger.Debugf("Process file[%s] done\n", fullPath)
                goto Finish
            }
            logger.Errorf("Process file[%s] err[%s]\n", fullPath, err)
            return err
        }

        logger.Debugf("Got msg len[%d] from file[%s]\n", msgLen, fullPath)
        readBuf := make([]byte, msgLen)
        _, err = io.ReadFull(reader, readBuf)
        if err != nil {
            logger.Errorf("Read msg len[%d] from file[%s] err[%s]\n", msgLen,
            fullPath, err)
            return err
        }

        msg := util.NewMessage(readBuf)
        if msg == nil {
            logger.Errorf("Convert byte[%v] to Message err\n", readBuf)
            // TODO:
            continue
        }

        msg.Topic = d.topic
        SENDLOOP:
        for {
            select {
            case d.msgChan <- msg:
                logger.Debugf("%s Send msg success\n", d)
                break SENDLOOP
            case <- time.After(3 * time.Second):
                logger.Debugf("%s send msg timeout, retry\n", d)
            }
        }
    }

Finish:
    logger.Debugf("Finish process file[%s]\n", fullPath)
    // move to done dir
    dstDir := filepath.Join(d.dirname, "done", fileName + ".done")

    // mkdir
    dir, _ := filepath.Split(dstDir)
    if dir != "" {
        err := os.MkdirAll(dir, 0770)
        if err != nil {
            logger.Errorf("Mkdir [%s] err[%s]\n", dir, err)
        }
    }

    logger.Debugf("Now move file[%s] to file[%s]\n", fullPath, dstDir)
    util.AtomicRename(fullPath, dstDir)
    return nil
}
