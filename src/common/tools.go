package common

import (
    "logger"
    "io/ioutil"
    sj      "go-simplejson"
)

// read conf conver to sj
func ReadConf(fileName string) (*sj.Json, error) {
    content, err := ioutil.ReadFile(fileName)
    if err != nil {
        logger.Errorf("Open conf[%s] err[%s]\n", fileName, err)
        return nil, err
    }

    ctx, err := sj.NewJson(content)
    if err != nil {
        logger.Errorf("Convert conf[%s] to json err[%s]\n", fileName, err)
        return nil, err
    }

    logger.Debugf("Parse conf[%s] to json success.\n", fileName)
    return ctx, nil
}

func InitLog(ctx *sj.Json) error {
    logDir := ctx.Get("log").Get("log_dir").MustString()                       
    logName := ctx.Get("log").Get("log_name").MustString()                     
    logLevel := ctx.Get("log").Get("log_level").MustInt()                      
    if logLevel < 0 {                                                           
        logLevel = 0                                                            
    }                                                                                                 
    if logLevel > 6 {                                                           
        logLevel = 6                                                            
    }                                                                           
    logger.SetRollingDaily(logDir, logName)                                     
    logger.SetLevel(logger.LEVEL(logLevel))                                     
                                                                                
    // 关闭Console输出                                                          
    logger.SetConsole(false)                                                    
    logger.Debugf("InitLog success, logDir: %s, logName: %s, logLevel: %v", logDir, logName, logLevel)
    return nil                                                                  
}
