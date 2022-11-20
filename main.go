package main

import (
	"bufio"
	"flag"
	"fmt"
	"github.com/bygui86/multi-profile/v2"
	"log"
	"math"
	_ "net/http/pprof"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var Exists struct{}

type Task interface {
	Process(ctx WorkContext)
}

type WorkContext interface {
	Name() string
	Workers() []Worker
	WorkerGroup() *sync.WaitGroup
	InputQueue() chan Task
	OutputQueue() chan Task
	Increment()
	Count() uint64

	SetName(newValue string)
	SetWorkers(newValue []Worker)
	SetWorkerGroup(newValue *sync.WaitGroup)
	SetInputQueue(newValue chan Task)
	SetOutputQueue(newValue chan Task)
	Close(closeInput bool, closeOutput bool, verbose bool, stats bool)
}

type BaseWorkContext struct {
	_Name        string
	_Workers     []Worker
	_WorkerGroup *sync.WaitGroup
	_InputQueue  chan Task
	_OutputQueue chan Task
	_Count       uint64
}

func (d *BaseWorkContext) Name() string {
	return d._Name
}

func (d *BaseWorkContext) Workers() []Worker {
	return d._Workers
}

func (d *BaseWorkContext) WorkerGroup() *sync.WaitGroup {
	return d._WorkerGroup
}

func (d *BaseWorkContext) InputQueue() chan Task {
	return d._InputQueue
}

func (d *BaseWorkContext) OutputQueue() chan Task {
	return d._OutputQueue
}

func (d *BaseWorkContext) Count() uint64 {
	return d._Count
}

func (d *BaseWorkContext) Increment() {
	atomic.AddUint64(&d._Count, 1)
}

func (d *BaseWorkContext) SetName(newValue string) {
	d._Name = newValue
}

func (d *BaseWorkContext) SetWorkers(newValue []Worker) {
	d._Workers = newValue
}

func (d *BaseWorkContext) SetWorkerGroup(newValue *sync.WaitGroup) {
	d._WorkerGroup = newValue
}

func (d *BaseWorkContext) SetInputQueue(newValue chan Task) {
	d._InputQueue = newValue
}

func (d *BaseWorkContext) SetOutputQueue(newValue chan Task) {
	d._OutputQueue = newValue
}

func (d *BaseWorkContext) Close(closeInput bool, closeOutput bool, verbose bool, stats bool) {

	closeTaskQueue := func(queueName string, queue chan Task, verbose bool) {
		if verbose {
			fmt.Printf("Closing %v for %v.\n", queueName, d.Name())
		}
		close(queue)
	}

	if closeInput {
		closeTaskQueue("input queue", d.InputQueue(), verbose)
	}

	if verbose {
		fmt.Printf("Waiting for %v... ", d.Name())
	}
	d.WorkerGroup().Wait()
	if verbose {
		fmt.Println("Done!")
	}

	if closeOutput {
		closeTaskQueue("output queue", d.OutputQueue(), verbose)
	}

	if stats {
		fmt.Printf("[stats] %v processed %v tasks\n", d.Name(), d.Count())
	}

}

type DefaultWorkContext = BaseWorkContext

type Worker struct {
	WorkContext WorkContext
	Runner      func(*Worker)
}

func (w *Worker) Run() {
	w.Runner(w)
}

func NewDefaultWorker(context WorkContext) Worker {

	return Worker{
		WorkContext: context,
		Runner: func(w *Worker) {
			for task := range w.WorkContext.InputQueue() {
				task.Process(w.WorkContext)
				context.Increment()
			}
		},
	}
}

func RunNWorkers(name string, workerCount int, taskQueueSize int, inputQueue chan Task, outputQueue chan Task, ctx WorkContext) WorkContext {

	if name == "" {
		name = "default"
	}
	if workerCount <= 0 {
		workerCount = runtime.NumCPU()
	}
	if taskQueueSize <= 0 {
		workerCount = 1024
	}
	if inputQueue == nil {
		inputQueue = make(chan Task, taskQueueSize)
	}
	if outputQueue == nil {
		outputQueue = make(chan Task, taskQueueSize)
	}

	if ctx == nil {
		ctx = new(DefaultWorkContext)
	}
	ctx.SetName(name)
	ctx.SetWorkers(make([]Worker, workerCount))
	ctx.SetWorkerGroup(new(sync.WaitGroup))
	ctx.SetInputQueue(inputQueue)
	ctx.SetOutputQueue(outputQueue)

	workers := ctx.Workers()
	for i := 0; i < workerCount; i++ {
		workers[i] = NewDefaultWorker(ctx)
		go func(w *Worker) {
			w.WorkContext.WorkerGroup().Add(1)
			w.Run()
			w.WorkContext.WorkerGroup().Done()
		}(&workers[i])
	}

	return ctx
}

type FileTask struct {
	FilePath  string
	Separator string
	KeepIndex int
}

func (ft FileTask) Process(ctx WorkContext) {

	file, err := os.Open(ft.FilePath)
	if err != nil {
		log.Fatal(err)
	}
	defer func(file *os.File) {
		_ = file.Close()
	}(file)

	count := 0
	sc := bufio.NewScanner(file)
	for sc.Scan() {
		ctx.OutputQueue() <- SplitTask{
			Input:     sc.Text(),
			Separator: ft.Separator,
			KeepIndex: ft.KeepIndex,
		}
		count++
	}
	if err := sc.Err(); err != nil {
		log.Fatalf("Scan file %v error: %v", ft.FilePath, err)
		return
	}
}

type SplitTask struct {
	Input     string
	Separator string
	KeepIndex int
}

func (st SplitTask) Process(ctx WorkContext) {
	shards := strings.Split(st.Input, st.Separator)
	if len(shards) > st.KeepIndex {
		result := shards[st.KeepIndex]
		ctx.OutputQueue() <- CollectTask{
			Prefix: result,
		}
	}
}

type CollectWorkContext struct {
	BaseWorkContext
	ResultSet map[string]struct{}
	mutex     *sync.Mutex
}

type CollectTask struct {
	Prefix string
}

func (c CollectTask) Process(ctx WorkContext) {

	if collectCtx, ok := ctx.(*CollectWorkContext); ok {
		collectCtx.mutex.Lock()
		collectCtx.ResultSet[c.Prefix] = Exists
		collectCtx.mutex.Unlock()
	}
}

var inputDirFlag = flag.String("input", "./input", "input directory")
var outputFilePathFlag = flag.String("output", "./result.out", "output file location")
var statsFlag = flag.Bool("stats", false, "true to print statistics at the end")
var verboseFlag = flag.Bool("verbose", false, "true to print status")
var profilerOutputPathFlag = flag.String("profile", "", "set profiler output directory to profile program, leave blank for no profiling")

func main() {

	flag.Parse()
	var inputDir string = *inputDirFlag
	var stats bool = *statsFlag
	var verbose bool = *verboseFlag
	var profilerOutputPath string = *profilerOutputPathFlag
	var outputFilePath string = *outputFilePathFlag

	outputFile, err := os.OpenFile(outputFilePath, os.O_CREATE|os.O_WRONLY, os.ModePerm)
	if err != nil {
		log.Fatalf("error opening output file! err %v", err)
		return
	}

	defer func(outputFile *os.File) {
		err = outputFile.Close()
		if err != nil {
			log.Fatalf("error closing output file! err %v", err)
		}
	}(outputFile)

	if profilerOutputPath != "" {
		conf := &profile.Config{
			Path:           profilerOutputPath,
			MemProfileType: profile.DefaultMemProfileType,
		}
		defer profile.CPUProfile(conf).Start().Stop()
		defer profile.MutexProfile(conf).Start().Stop()
		defer profile.TraceProfile(conf).Start().Stop()
		defer profile.MemProfile(conf).Start().Stop()
		defer profile.BlockProfile(conf).Start().Stop()
		defer profile.GoroutineProfile(conf).Start().Stop()
		defer profile.ThreadCreationProfile(conf).Start().Stop()
	}

	current := time.Now()

	files, err := os.ReadDir(inputDir)
	if err != nil {
		log.Fatal(err)
	}

	ioTime := time.Now().Sub(current)
	current = time.Now()

	nCpu := runtime.NumCPU()
	baseSize := 4096 * 2 * 2 * 2
	injectors := RunNWorkers("injectors", int(math.Max(float64(len(files)), float64(nCpu))), baseSize, nil, nil, nil)
	splitters := RunNWorkers("splitters", 10*nCpu, 1024*baseSize, injectors.OutputQueue(), nil, nil)
	collectors := RunNWorkers("collectors", 2, baseSize, splitters.OutputQueue(), nil, &CollectWorkContext{
		ResultSet: make(map[string]struct{}),
		mutex:     new(sync.Mutex),
	}).(*CollectWorkContext)

	setupTime := time.Now().Sub(current)
	current = time.Now()

	for _, f := range files {
		path := filepath.Join(inputDir, f.Name())
		injectors.InputQueue() <- FileTask{
			FilePath:  path,
			Separator: "|",
			KeepIndex: 1,
		}
	}

	baseInject := time.Now().Sub(current)
	current = time.Now()

	injectors.Close(true, true, verbose, stats)
	injectorsTime := time.Now().Sub(current)
	current = time.Now()
	splitters.Close(false, true, verbose, stats)
	splittersTime := time.Now().Sub(current)
	current = time.Now()
	collectors.Close(false, true, verbose, stats)
	collectorsTime := time.Now().Sub(current)
	current = time.Now()

	closeTime := injectorsTime + splittersTime + collectorsTime

	for k := range collectors.ResultSet {
		_, err := outputFile.WriteString(k + "\n")
		if err != nil {
			log.Printf("error writing to output file! aborting... err %v", err)
			break
		}
	}

	dumpTime := time.Now().Sub(current)
	current = time.Now()

	if stats {
		fmt.Printf("ioTime : %v\n", ioTime)
		fmt.Printf("setupTime : %v\n", setupTime)
		fmt.Printf("baseInject : %v\n", baseInject)
		fmt.Printf("closeTime : %v\n", closeTime)
		fmt.Println("	injectors :")
		fmt.Printf("		time : %v\n", injectorsTime)
		fmt.Printf("		avg/it : %v\n", time.Nanosecond*time.Duration(injectorsTime.Nanoseconds()/int64(injectors.Count())))
		fmt.Println("	splitters :")
		fmt.Printf("		time : %v\n", splittersTime)
		fmt.Printf("		avg/it : %v\n", time.Nanosecond*time.Duration(splittersTime.Nanoseconds()/int64(splitters.Count())))
		fmt.Println("	collectors :")
		fmt.Printf("		time : %v\n", collectorsTime)
		fmt.Printf("		avg/it : %v\n", time.Nanosecond*time.Duration(collectorsTime.Nanoseconds()/int64(collectors.Count())))
		fmt.Printf("dumpTime : %v\n", dumpTime)
	}
}
