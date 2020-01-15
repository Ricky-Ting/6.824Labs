package mapreduce


import (
	"sort"
	"log"
	"os"
	"encoding/json"
	"io"
)


func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTask int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	//
	// doReduce manages one reduce task: it should read the intermediate
	// files for the task, sort the intermediate key/value pairs by key,
	// call the user-defined reduce function (reduceF) for each key, and
	// write reduceF's output to disk.
	//
	// You'll need to read one intermediate file from each map task;
	// reduceName(jobName, m, reduceTask) yields the file
	// name from map task m.
	//
	// Your doMap() encoded the key/value pairs in the intermediate
	// files, so you will need to decode them. If you used JSON, you can
	// read and decode by creating a decoder and repeatedly calling
	// .Decode(&kv) on it until it returns an error.
	//
	// You may find the first example in the golang sort package
	// documentation useful.
	//
	// reduceF() is the application's reduce function. You should
	// call it once per distinct key, with a slice of all the values
	// for that key. reduceF() returns the reduced value for that key.
	//
	// You should write the reduce output as JSON encoded KeyValue
	// objects to the file named outFile. We require you to use JSON
	// because that is what the merger than combines the output
	// from all the reduce tasks expects. There is nothing special about
	// JSON -- it is just the marshalling format we chose to use. Your
	// output code will look something like this:
	//
	// enc := json.NewEncoder(file)
	// for key := ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	//
	// Your code here (Part I).
	//


	// 从nMap个中间文件中读取KeyValue， 然后排序，
	// 对某一Key和与之对应的所有Value调用一次reduceF生成一个KeyValue对，
	// 最后将所有结果写入OutFile中。

	// 我的实现是将所有文件都读入内存中，然后对KeyValue对进行排序，
	// 但不知道数据量大了会怎么样，后面得去了解具体的MapReduce中的排序是怎么实现的。


	kvs := make([]KeyValue, 0, 0)

	for f := 0; f < nMap; f++ {
		file, fileErr := os.Open(reduceName(jobName, f, reduceTask))
		if fileErr != nil {
			log.Fatal("doReduce: ", fileErr)
		}

		var kv KeyValue
		var err error = nil
		dec := json.NewDecoder(file)
		for {
			err = dec.Decode(&kv)
			if err == nil {
				kvs = append(kvs, kv)
			} else if err == io.EOF {
				break
			} else {
				log.Fatal("doReduce: ", err)
			}
		}

		file.Close()
	}

	sort.Slice(kvs, func(i, j int) bool {
		return kvs[i].Key < kvs[j].Key
	})

	file, err := os.Create(outFile)
	if err != nil {
			log.Fatal("doReduce: ", err)
	}
	enc := json.NewEncoder(file)
	defer file.Close()


	for f := 0; f < len(kvs); {
		nf := f
		val := make([]string, 0, 0) 
		for nf < len(kvs) && kvs[f].Key == kvs[nf].Key {
			val = append(val, kvs[nf].Value)
			nf++
		}
		output := KeyValue{kvs[f].Key, reduceF(kvs[f].Key, val)}
		enc.Encode(&output)
		f = nf
	}
	return 

}
