// dry: Don't repeat yourself
package util

import (
    "logger"
    "common"

    sj      "go-simplejson"
    gcom    "github.com/guyannanfei25/go_common"
)

func InitMisc(ctx *sj.Json) error {
    if err := common.InitLog(ctx); err != nil {
        logger.Errorf("InitLog err[%s], please Check!!!\n", err)
        return err;
    }

    pidFile := ctx.Get("main").Get("pid_file").MustString()
    if err := gcom.InitPidFile(pidFile); err != nil {
        logger.Errorf("InitPidFile err[%s]\n", err)
        return err
    }

    maxProc := ctx.Get("runtime").Get("max_proc").MustInt()
    gcom.InitRunProcs(maxProc)

    // gc info
    // if use mem > max_mem M will force gc
    max_mem := ctx.Get("gc").Get("max_mem_m").MustInt()
    // check mem interval seconds
    check_interval := ctx.Get("gc").Get("check_interval_s").MustInt()
    go gcom.IntervalGC(max_mem, check_interval)

    return nil
}
