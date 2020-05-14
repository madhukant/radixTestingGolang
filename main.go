package main

import (
	"fmt"
	"time"
	"flag"

	"github.com/mediocregopher/radix/v3"
	"github.com/sirupsen/logrus"
	log "github.com/sirupsen/logrus"
)

var (
	ClusterPoolFunc = radix.ClusterPoolFunc
	Logger          = logrus.New() //Instanciate logrus
	ConnDB          *radix.Cluster
	address  		= flag.String("address", "localhost:6379", "Provide master node IP address")
)

func main() {
	fmt.Println("test go")
	
	flag.Parse()

	initDB(*address, 10)
	for {
		startOps()
	}
	EmptyPool()
}

func initDB(connString string, poolNum int) error {

	log.WithFields(logrus.Fields{
		"connString": connString,
		"poolNum":    poolNum,
	}).Info("I am going to create the connection pool")

	var err error
	contSTR := []string{connString}
	customConnFunc := func(Network, contSTR string) (radix.Conn, error) {
		return radix.Dial(Network, contSTR,
			radix.DialTimeout(1*time.Minute),
		)
	}

	poolFunc := func(Network, contSTR string) (radix.Client, error) {
		return radix.NewPool(Network, contSTR, poolNum,
			radix.PoolPingInterval(5*time.Second),
			radix.PoolOnEmptyCreateAfter(0*time.Second),
			radix.PoolRefillInterval(1*time.Second),
			radix.PoolOnFullBuffer((poolNum), 1*time.Second),
			radix.PoolConnFunc(customConnFunc))

	}
	ConnDB, err = radix.NewCluster(contSTR, ClusterPoolFunc(poolFunc))

	if err != nil {
		log.WithFields(logrus.Fields{
			"Error":      err,
			"connString": connString,
			"poolNum":    poolNum,
		}).Error("Problem in creating connection pool...!!")
		return err
	}
	log.WithFields(logrus.Fields{
		"connString": connString,
		"poolNum":    poolNum,
	}).Info("Created a Redis connection pool sucessfully..!!")

	return nil
}

func startOps() {
	start := time.Now()
	err1 := ConnDB.Do(radix.Cmd(nil, "SET", "foo", "someval"))
	duration := time.Since(start)
	fmt.Println("Time to Write", duration)

	if err1 != nil {
		log.WithFields(logrus.Fields{
			"Error":     err1,
			"Operation": "Set",
			"key":       "foo",
		}).Error("Problem in setting key...!!")
	}

	var fooVal string
	err2 := ConnDB.Do(radix.Cmd(&fooVal, "GET", "foo"))
	if err2 != nil {
		log.WithFields(logrus.Fields{
			"Error":     err2,
			"Operation": "Get",
			"key":       "foo",
		}).Error("Problem in getting key...!!")
		//return
	}
	if err1 == nil && err2 == nil { 
	fmt.Println(fooVal)
	log.WithFields(logrus.Fields{
		"value": fooVal,
	})
	}
	time.Sleep(time.Second * 1)
}

// EmptyPool will close all the connections in the pool in case of a docker container crash
func EmptyPool() {
	err := ConnDB.Close()
	log.WithFields(logrus.Fields{
		"LogType": "app",
	})
	if err != nil {
		log.Errorf("I am not able to close the Pool since : %v", err)
	}
	log.Info("Sucessfully closed the connection pool")
}

