package main

import (
    "logger"
    "flag"
    // "sync"
    // "time"
    "os"
    "fmt"
    // "io/ioutil"
    "common"
    "play"
    "util"
)

var conf = flag.String("f", "etc/play.json", "conf path")

// play program will dump data in disk to nsqd,
// data in disk format is: header(len(raw data), bigendia) + raw_data
func main() {
    if len(os.Args) != 3 {
        fmt.Fprintf(os.Stderr, "Usage: %s -f conf_path\n", os.Args[0])
        os.Exit(-1)
    }

    flag.Parse()
    logger.Debugf("Got conf path: [%s]\n", *conf)

    ctx, err := common.ReadConf(*conf)
    if err != nil {
        logger.Errorf("Parse conf[%s] err[%s], please check!!!\n", *conf, err)
        os.Exit(-2)
    }

    if ctx == nil {
        logger.Errorf("Conf[%s] to Json is nil, please Check!!!\n", *conf)
        return;
    }

    if err := util.InitMisc(ctx); err != nil {
        logger.Errorf("initMisc err[%s]\n", err)
        return
    }

    play.Main(ctx)

    logger.Debugf("record process end\n")
}

