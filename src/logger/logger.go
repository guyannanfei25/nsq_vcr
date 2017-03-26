package logger

import (
	"fmt"
	"log"
	"os"
	"runtime"
	"strconv"
	"sync"
	"time"
)

const (
	_VER string = "1.0.2"
)

type LEVEL int32

var logLevel LEVEL = 1
var maxFileSize int64
var maxFileCount int32
var dailyRolling bool = true
var consoleAppender bool = true
var RollingFile bool = false
var logObj *_FILE

const DATEFORMAT = "2006-01-02"

type UNIT int64

const (
	_       = iota
	KB UNIT = 1 << (iota * 10)
	MB
	GB
	TB
)

const (
	ALL LEVEL = iota
	DEBUG
	INFO
	WARN
	ERROR
	FATAL
	OFF
)

type _FILE struct {
	dir      string
	filename string
	_suffix  int
	isCover  bool
	_date    *time.Time
	mu       *sync.RWMutex
	logfile  *os.File
	lg       *log.Logger
}

func SetConsole(isConsole bool) {
	consoleAppender = isConsole
}

func SetLevel(_level LEVEL) {
	logLevel = _level
}

func SetRollingFile(fileDir, fileName string, maxNumber int32, maxSize int64, _unit UNIT) {
	maxFileCount = maxNumber
	maxFileSize = maxSize * int64(_unit)
	RollingFile = true
	dailyRolling = false
	logObj = &_FILE{dir: fileDir, filename: fileName, isCover: false, mu: new(sync.RWMutex)}
	logObj.mu.Lock()
	defer logObj.mu.Unlock()
	for i := 1; i <= int(maxNumber); i++ {
		if isExist(fileDir + "/" + fileName + "." + strconv.Itoa(i)) {
			logObj._suffix = i
		} else {
			break
		}
	}
	if !logObj.isMustRename() {
		logObj.logfile, _ = os.OpenFile(fileDir+"/"+fileName, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0666)
		logObj.lg = log.New(logObj.logfile, "", log.Ldate|log.Ltime|log.Lshortfile)
	} else {
		logObj.rename()
	}
	go fileMonitor()
}

func SetRollingDaily(fileDir, fileName string) {
	RollingFile = false
	dailyRolling = true
	t, _ := time.Parse(DATEFORMAT, time.Now().Format(DATEFORMAT))
	logObj = &_FILE{dir: fileDir, filename: fileName, _date: &t, isCover: false, mu: new(sync.RWMutex)}
	logObj.mu.Lock()
	defer logObj.mu.Unlock()

	if !logObj.isMustRename() {
		logObj.logfile, _ = os.OpenFile(fileDir+"/"+fileName, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0666)
		logObj.lg = log.New(logObj.logfile, "", log.Ldate|log.Ltime|log.Lshortfile)
	} else {
		logObj.rename()
	}
}

func console(s ...interface{}) {
	if consoleAppender {
		_, file, line, _ := runtime.Caller(2)
		short := file
		for i := len(file) - 1; i > 0; i-- {
			if file[i] == '/' {
				short = file[i+1:]
				break
			}
		}
		file = short
		s = append([]interface{}{fmt.Sprintf("%s:%d ", file, line)}, s...)
		log.Println(s...)
	}
}

func consolef(format string, s ...interface{}) {
	if consoleAppender {
		_, file, line, _ := runtime.Caller(2)
		short := file
		for i := len(file) - 1; i > 0; i-- {
			if file[i] == '/' {
				short = file[i+1:]
				break
			}
		}
		file = short
		log.Printf(fmt.Sprintf("%s:%d ", file, line)+format, s...)
	}
}

func catchError() {
	if err := recover(); err != nil {
		log.Println("err", err)
	}
}

func Debug(v ...interface{}) {
	if dailyRolling {
		fileCheck()
	}
	defer catchError()
	if logObj != nil {
		logObj.mu.RLock()
		defer logObj.mu.RUnlock()
	}

	if logLevel <= DEBUG {
		if logObj != nil {
			logObj.lg.Output(2, "[DEBUG] "+fmt.Sprintln(v...))
		}
		v = append([]interface{}{"[DEBUG]"}, v...)
		console(v...)
	}
}
func Info(v ...interface{}) {
	if dailyRolling {
		fileCheck()
	}
	defer catchError()
	if logObj != nil {
		logObj.mu.RLock()
		defer logObj.mu.RUnlock()
	}
	if logLevel <= INFO {
		if logObj != nil {
			logObj.lg.Output(2, "[INFO ] "+fmt.Sprintln(v...))
		}
		v = append([]interface{}{"[INFO ]"}, v...)
		console(v...)
	}
}
func Warn(v ...interface{}) {
	if dailyRolling {
		fileCheck()
	}
	defer catchError()
	if logObj != nil {
		logObj.mu.RLock()
		defer logObj.mu.RUnlock()
	}

	if logLevel <= WARN {
		if logObj != nil {
			logObj.lg.Output(2, "[WARN ] "+fmt.Sprintln(v...))
		}
		v = append([]interface{}{"[WARN ]"}, v...)
		console(v...)
	}
}
func Error(v ...interface{}) {
	if dailyRolling {
		fileCheck()
	}
	defer catchError()
	if logObj != nil {
		logObj.mu.RLock()
		defer logObj.mu.RUnlock()
	}
	if logLevel <= ERROR {
		if logObj != nil {
			logObj.lg.Output(2, "[ERROR] "+fmt.Sprintln(v...))
		}
		v = append([]interface{}{"[ERROR]"}, v...)
		console(v...)
	}
}
func Fatal(v ...interface{}) {
	if dailyRolling {
		fileCheck()
	}
	defer catchError()
	if logObj != nil {
		logObj.mu.RLock()
		defer logObj.mu.RUnlock()
	}
	if logLevel <= FATAL {
		if logObj != nil {
			logObj.lg.Output(2, "[FATAL] "+fmt.Sprintln(v...))
		}
		v = append([]interface{}{"[FATAL]"}, v...)
		console(v...)
	}
}

func DebugSub(sub string, v ...interface{}) {
	if dailyRolling {
		fileCheck()
	}
	defer catchError()
	if logObj != nil {
		logObj.mu.RLock()
		defer logObj.mu.RUnlock()
	}

	if logLevel <= DEBUG {
		if logObj != nil {
			logObj.lg.Output(2, sub+" [DEBUG] "+fmt.Sprintln(v...))
		}
		v = append([]interface{}{sub + " [DEBUG]"}, v...)
		console(v...)
	}
}
func InfoSub(sub string, v ...interface{}) {
	if dailyRolling {
		fileCheck()
	}
	defer catchError()
	if logObj != nil {
		logObj.mu.RLock()
		defer logObj.mu.RUnlock()
	}
	if logLevel <= INFO {
		if logObj != nil {
			logObj.lg.Output(2, sub+" [INFO ] "+fmt.Sprintln(v...))
		}
		v = append([]interface{}{sub + " [INFO ]"}, v...)
		console(v...)
	}
}
func WarnSub(sub string, v ...interface{}) {
	if dailyRolling {
		fileCheck()
	}
	defer catchError()
	if logObj != nil {
		logObj.mu.RLock()
		defer logObj.mu.RUnlock()
	}

	if logLevel <= WARN {
		if logObj != nil {
			logObj.lg.Output(2, sub+" [WARN ] "+fmt.Sprintln(v...))
		}
		v = append([]interface{}{sub + " [WARN ]"}, v...)
		console(v...)
	}
}
func ErrorSub(sub string, v ...interface{}) {
	if dailyRolling {
		fileCheck()
	}
	defer catchError()
	if logObj != nil {
		logObj.mu.RLock()
		defer logObj.mu.RUnlock()
	}
	if logLevel <= ERROR {
		if logObj != nil {
			logObj.lg.Output(2, sub+" [ERROR] "+fmt.Sprintln(v...))
		}
		v = append([]interface{}{sub + " [ERROR]"}, v...)
		console(v...)
	}
}
func FatalSub(sub string, v ...interface{}) {
	if dailyRolling {
		fileCheck()
	}
	defer catchError()
	if logObj != nil {
		logObj.mu.RLock()
		defer logObj.mu.RUnlock()
	}
	if logLevel <= FATAL {
		if logObj != nil {
			logObj.lg.Output(2, sub+" [FATAL] "+fmt.Sprintln(v...))
		}
		v = append([]interface{}{sub + " [FATAL]"}, v...)
		console(v...)
	}
}

func Debugf(format string, v ...interface{}) {
	if dailyRolling {
		fileCheck()
	}
	defer catchError()
	if logObj != nil {
		logObj.mu.RLock()
		defer logObj.mu.RUnlock()
	}

	if logLevel <= DEBUG {
		if logObj != nil {
			logObj.lg.Output(2, "[DEBUG] "+fmt.Sprintf(format, v...))
		}
		consolef("[DEBUG] "+format, v...)
	}
}
func Infof(format string, v ...interface{}) {
	if dailyRolling {
		fileCheck()
	}
	defer catchError()
	if logObj != nil {
		logObj.mu.RLock()
		defer logObj.mu.RUnlock()
	}
	if logLevel <= INFO {
		if logObj != nil {
			logObj.lg.Output(2, "[INFO ] "+fmt.Sprintf(format, v...))
		}
		consolef("[INFO] "+format, v...)
	}
}
func Warnf(format string, v ...interface{}) {
	if dailyRolling {
		fileCheck()
	}
	defer catchError()
	if logObj != nil {
		logObj.mu.RLock()
		defer logObj.mu.RUnlock()
	}

	if logLevel <= WARN {
		if logObj != nil {
			logObj.lg.Output(2, "[WARN ] "+fmt.Sprintf(format, v...))
		}
		consolef("[WARN] "+format, v...)
	}
}
func Errorf(format string, v ...interface{}) {
	if dailyRolling {
		fileCheck()
	}
	defer catchError()
	if logObj != nil {
		logObj.mu.RLock()
		defer logObj.mu.RUnlock()
	}
	if logLevel <= ERROR {
		if logObj != nil {
			logObj.lg.Output(2, "[ERROR] "+fmt.Sprintf(format, v...))
		}
		consolef("[ERROR] "+format, v...)
	}
}
func Fatalf(format string, v ...interface{}) {
	if dailyRolling {
		fileCheck()
	}
	defer catchError()
	if logObj != nil {
		logObj.mu.RLock()
		defer logObj.mu.RUnlock()
	}
	if logLevel <= FATAL {
		if logObj != nil {
			logObj.lg.Output(2, "[FATAL] "+fmt.Sprintf(format, v...))
		}
		consolef("[FATAL] "+format, v...)
	}
}

func DebugSubf(sub, format string, v ...interface{}) {
	if dailyRolling {
		fileCheck()
	}
	defer catchError()
	if logObj != nil {
		logObj.mu.RLock()
		defer logObj.mu.RUnlock()
	}

	if logLevel <= DEBUG {
		if logObj != nil {
			logObj.lg.Output(2, sub+" [DEBUG] "+fmt.Sprintf(format, v...))
		}
		consolef(sub+" [DEBUG] "+format, v...)
	}
}
func InfoSubf(sub, format string, v ...interface{}) {
	if dailyRolling {
		fileCheck()
	}
	defer catchError()
	if logObj != nil {
		logObj.mu.RLock()
		defer logObj.mu.RUnlock()
	}
	if logLevel <= INFO {
		if logObj != nil {
			logObj.lg.Output(2, sub+" [INFO ] "+fmt.Sprintf(format, v...))
		}
		consolef(sub+" [INFO] "+format, v...)
	}
}
func WarnSubf(sub, format string, v ...interface{}) {
	if dailyRolling {
		fileCheck()
	}
	defer catchError()
	if logObj != nil {
		logObj.mu.RLock()
		defer logObj.mu.RUnlock()
	}

	if logLevel <= WARN {
		if logObj != nil {
			logObj.lg.Output(2, sub+" [WARN ] "+fmt.Sprintf(format, v...))
		}
		consolef(sub+" [WARN] "+format, v...)
	}
}
func ErrorSubf(sub, format string, v ...interface{}) {
	if dailyRolling {
		fileCheck()
	}
	defer catchError()
	if logObj != nil {
		logObj.mu.RLock()
		defer logObj.mu.RUnlock()
	}
	if logLevel <= ERROR {
		if logObj != nil {
			logObj.lg.Output(2, sub+" [ERROR] "+fmt.Sprintf(format, v...))
		}
		consolef(sub+" [ERROR] "+format, v...)
	}
}
func FatalSubf(sub, format string, v ...interface{}) {
	if dailyRolling {
		fileCheck()
	}
	defer catchError()
	if logObj != nil {
		logObj.mu.RLock()
		defer logObj.mu.RUnlock()
	}
	if logLevel <= FATAL {
		if logObj != nil {
			logObj.lg.Output(2, sub+" [FATAL] "+fmt.Sprintf(format, v...))
		}
		consolef(sub+" [FATAL] "+format, v...)
	}
}

func DebugSIf(sub string, id int, format string, v ...interface{}) {
	if dailyRolling {
		fileCheck()
	}
	defer catchError()
	if logObj != nil {
		logObj.mu.RLock()
		defer logObj.mu.RUnlock()
	}

	if logLevel <= DEBUG {
		if logObj != nil {
			logObj.lg.Output(2, fmt.Sprintf("%s:%d", sub, id)+" [DEBUG] "+fmt.Sprintf(format, v...))
		}
		consolef(fmt.Sprintf("%s:%d", sub, id)+" [DEBUG] "+format, v...)
	}
}
func InfoSIf(sub string, id int, format string, v ...interface{}) {
	if dailyRolling {
		fileCheck()
	}
	defer catchError()
	if logObj != nil {
		logObj.mu.RLock()
		defer logObj.mu.RUnlock()
	}
	if logLevel <= INFO {
		if logObj != nil {
			logObj.lg.Output(2, fmt.Sprintf("%s:%d", sub, id)+" [INFO ] "+fmt.Sprintf(format, v...))
		}
		consolef(fmt.Sprintf("%s:%d", sub, id)+" [INFO] "+format, v...)
	}
}
func WarnSIf(sub string, id int, format string, v ...interface{}) {
	if dailyRolling {
		fileCheck()
	}
	defer catchError()
	if logObj != nil {
		logObj.mu.RLock()
		defer logObj.mu.RUnlock()
	}

	if logLevel <= WARN {
		if logObj != nil {
			logObj.lg.Output(2, fmt.Sprintf("%s:%d", sub, id)+" [WARN ] "+fmt.Sprintf(format, v...))
		}
		consolef(fmt.Sprintf("%s:%d", sub, id)+" [WARN] "+format, v...)
	}
}
func ErrorSIf(sub string, id int, format string, v ...interface{}) {
	if dailyRolling {
		fileCheck()
	}
	defer catchError()
	if logObj != nil {
		logObj.mu.RLock()
		defer logObj.mu.RUnlock()
	}
	if logLevel <= ERROR {
		if logObj != nil {
			logObj.lg.Output(2, fmt.Sprintf("%s:%d", sub, id)+" [ERROR] "+fmt.Sprintf(format, v...))
		}
		consolef(fmt.Sprintf("%s:%d", sub, id)+" [ERROR] "+format, v...)
	}
}
func FatalSIf(sub string, id int, format string, v ...interface{}) {
	if dailyRolling {
		fileCheck()
	}
	defer catchError()
	if logObj != nil {
		logObj.mu.RLock()
		defer logObj.mu.RUnlock()
	}
	if logLevel <= FATAL {
		if logObj != nil {
			logObj.lg.Output(2, fmt.Sprintf("%s:%d", sub, id)+" [FATAL] "+fmt.Sprintf(format, v...))
		}
		consolef(fmt.Sprintf("%s:%d", sub, id)+" [FATAL] "+format, v...)
	}
}

func (f *_FILE) isMustRename() bool {
	if dailyRolling {
		t, _ := time.Parse(DATEFORMAT, time.Now().Format(DATEFORMAT))
		if t.After(*f._date) {
			return true
		}
	} else {
		if maxFileCount > 1 {
			if fileSize(f.dir+"/"+f.filename) >= maxFileSize {
				return true
			}
		}
	}
	return false
}

func (f *_FILE) rename() {
	if dailyRolling {
		fn := f.dir + "/" + f.filename + "." + f._date.Format(DATEFORMAT)
		if !isExist(fn) && f.isMustRename() {
			if f.logfile != nil {
				f.logfile.Close()
			}
			err := os.Rename(f.dir+"/"+f.filename, fn)
			if err != nil {
				f.lg.Println("rename err", err.Error())
			}
			t, _ := time.Parse(DATEFORMAT, time.Now().Format(DATEFORMAT))
			f._date = &t
			f.logfile, _ = os.Create(f.dir + "/" + f.filename)
			f.lg = log.New(logObj.logfile, "", log.Ldate|log.Ltime|log.Lshortfile)
		}
	} else {
		f.coverNextOne()
	}
}

func (f *_FILE) nextSuffix() int {
	return int(f._suffix%int(maxFileCount) + 1)
}

func (f *_FILE) coverNextOne() {
	f._suffix = f.nextSuffix()
	if f.logfile != nil {
		f.logfile.Close()
	}
	if isExist(f.dir + "/" + f.filename + "." + strconv.Itoa(int(f._suffix))) {
		os.Remove(f.dir + "/" + f.filename + "." + strconv.Itoa(int(f._suffix)))
	}
	os.Rename(f.dir+"/"+f.filename, f.dir+"/"+f.filename+"."+strconv.Itoa(int(f._suffix)))
	f.logfile, _ = os.Create(f.dir + "/" + f.filename)
	f.lg = log.New(logObj.logfile, "", log.Ldate|log.Ltime|log.Lshortfile)
}

func fileSize(file string) int64 {
	fmt.Println("fileSize", file)
	f, e := os.Stat(file)
	if e != nil {
		fmt.Println(e.Error())
		return 0
	}
	return f.Size()
}

func isExist(path string) bool {
	_, err := os.Stat(path)
	return err == nil || os.IsExist(err)
}

func fileMonitor() {
	timer := time.NewTicker(1 * time.Second)
	for {
		select {
		case <-timer.C:
			fileCheck()
		}
	}
}

func fileCheck() {
	defer func() {
		if err := recover(); err != nil {
			log.Println(err)
		}
	}()
	if logObj != nil && logObj.isMustRename() {
		logObj.mu.Lock()
		defer logObj.mu.Unlock()
		logObj.rename()
	}
}
