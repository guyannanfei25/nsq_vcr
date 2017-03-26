package util

import (
	"common"
	"logger"
	"os"
	"strings"
    "fmt"

	sj "go-simplejson"
)

const (
	IDCFromHost  = iota // same with run host
	GLOBAL              // use global lable
	IDCSpecified        // use some specified idc
)

// return lookupds due to different category
func GetLookupdAddrs(ctx *sj.Json) ([]string, error) {
	var lookupdAddrs []string
	lookupdConf := ctx.Get("main").Get("nsq").Get("lookupd_conf").MustString("/tmp/data/nsqlog/nsq.json")
	category := ctx.Get("main").Get("nsq").Get("lookupd_category").MustInt(0)
	idc := ctx.Get("main").Get("nsq").Get("idc_specified").MustString()

	lookupdJson, err := common.ReadConf(lookupdConf)
	if err != nil {
		logger.Errorf("Get Lookupd addr err[%s]\n", err)
		return nil, err
	}

	var idcSection string
	switch category {
	case IDCFromHost:
		idcSection = GetIDCFromHost()
	case GLOBAL:
		idcSection = "global"
	case IDCSpecified:
		idcSection = idc
	}

	if idcSection == "" {
		logger.Errorf("lookupd conf[%s], idc category[%d], but get idc empty", lookupdConf, category)
		return nil, fmt.Errorf("lookupd conf[%s], idc category[%d], but get idc empty", lookupdConf, category)
	}

	lookupdAddrs = lookupdJson.Get("nsqloopupd_address").Get(idcSection).MustStringArray()
	logger.Debugf("Get lookupd addr[%v] from conf[%s], lookupd idc is [%s]",
		lookupdAddrs, lookupdConf, idcSection)

	return lookupdAddrs, nil
}

func GetIDCFromHost() string {
	hostname, err := os.Hostname()
	if err != nil {
		logger.Errorf("Get machine Hostname err[%s]\n", err)
		return ""
	}

	fields := strings.Split(hostname, ".")
	if len(fields) < 3 {
		logger.Errorf("hostname[%s] Split by . fields[%v] < 3\n", hostname, fields)
		return ""
	}

	logger.Debugf("Get From hostname[%s] idc[%s]\n", hostname, fields[2])
	return fields[2]
}
