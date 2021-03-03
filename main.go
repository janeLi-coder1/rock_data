package main

import (
	"encoding/csv"
	"flag"
	"io"
	"math/rand"
	"os"
	"sync"
	"time"
)

var (
	PipePool   []chan []string
	DoneSignal chan bool
	wg         *sync.WaitGroup
)

type args struct {
	filePath         string
	shouldCleanTable bool
}

// read file path from command line argument
func GetCommandLineArgs() (*args, error) {
	filePathPtr := flag.String("file", "", "file to be processed")
	cleanTable := flag.Bool("clean", false, "should clean table?")
	flag.Parse()
	if len(*filePathPtr) == 0 {
		return nil, ErrInvalidFilePath
	}

	return &args{
		filePath:         *filePathPtr,
		shouldCleanTable: *cleanTable,
	}, nil
}

// read file content and push to corresponding pipeline
func ReadRawFile(path string, pipelineNumber int) {

	begin := time.Now()

	fileHandler, err := os.Open(path)
	if err != nil {
		FatalF(err.Error())
	}

	defer func() {
		fileHandler.Close()
		wg.Done()
	}()
	r := csv.NewReader(fileHandler)

	count := 0
	for {
		count += 1
		row, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			ErrorF("read line: %s from csv error: %s", count, err)
			continue
		}
		if count == 1 {
			continue
		}
		pushToPipe(row)
	}

	InfoF("read file over, count %d lines", count-1)
	InfoF("read file cost: %dms", time.Since(begin).Milliseconds())
	for pipelineNumber > 0 {
		DoneSignal <- true
		pipelineNumber--
	}
}

// push text to pipe
func pushToPipe(row []string) {
	index := rand.Intn(len(PipePool))
	getNthPipe(index) <- row
	DebugF("push text: %#v to pipe: %d", row, index)
}

func getNthPipe(index int) chan []string {
	if len(PipePool) > index {
		return PipePool[index]
	}
	ErrorF("invalid index to get pipe: %d", index)
	return nil
}

func initPipePool(pipelineNumber, pipeCapacity int) {
	if PipePool != nil {
		return
	}

	if pipelineNumber <= 0 {
		FatalF("invalid pipelineNumber: %d", pipelineNumber)
	}
	if pipeCapacity <= 0 {
		FatalF("invalid pipeCapacity: %d", pipeCapacity)
	}

	for count := 0; count < pipelineNumber; count++ {
		pipe := make(chan []string, pipeCapacity)
		PipePool = append(PipePool, pipe)
	}
}

func initDoneSignal(pipelineNumber int) {
	DoneSignal = make(chan bool, pipelineNumber)
}

func initWaitGroup() {
	wg = &sync.WaitGroup{}
}

func main() {
	args, err := GetCommandLineArgs()
	if err != nil {
		flag.Usage()
		return
	}

	config, err := GetConfig()
	if err != nil {
		InitLogger(true)
		FatalF("get configuration file error: " + err.Error())
		return
	}
	InitLogger(config.DebugSwitch)
	initPipePool(config.PipelineNumber, config.PipeCapacity)
	initDoneSignal(config.PipelineNumber)
	initWaitGroup()

	if args.shouldCleanTable {
		err = CleanTable(config.Database)
		if err != nil {
			FatalF("%s", err)
			return
		}
	}

	InfoF("Pipeline number: %d, each pipe capacity: %d", config.PipelineNumber, config.PipeCapacity)
	begin := time.Now()

	wg.Add(1)
	go ReadRawFile(args.filePath, config.PipelineNumber)

	for _, ch := range PipePool {
		go WriteRecordToDbThroughChannel(ch, config.Database)
	}

	wg.Wait()
	close(DoneSignal)

	InfoF("all cost: %dms", time.Since(begin).Milliseconds())
}
