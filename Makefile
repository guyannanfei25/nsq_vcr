CURDIR:=$(shell pwd)

all: record play

record:
	export GOPATH=`pwd`:${GOPATH}; go build -o ${CURDIR}/bin/record ${CURDIR}/src/main/record.go

play:
	export GOPATH=`pwd`:${GOPATH}; go build -o ${CURDIR}/bin/play ${CURDIR}/src/main/play.go

clean:
	rm -fv ${CURDIR}/bin/record ${CURDIR}/bin/play
