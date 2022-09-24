package dbutils

import (
	"encoding/json"
	"github.com/ethereum/go-ethereum/log"
	"io/ioutil"
	"os"
	"path/filepath"
)

type Pgconfig struct {
	Username              string `json:"username"`
	Password              string `json:"password"`
	Database              string `json:"database"`
	Port                  string `json:"port"`
	TableBalance          string `json:"tableBalance"`
	TableCall             string `json:"tableCall"`
	Endpoint              string `json:"endpoint"`
	ReWriteBatchedInserts string `json:"reWriteBatchedInserts"`
	CacheRecordNum        string `json:"cacheRecordNum"`
}

var ConfigPath string

var Pgconfigdata *Pgconfig

func Init() {
	Pgconfigdata = new(Pgconfig)
	getwd, _ := os.Getwd()
	ConfigPath = filepath.Join(getwd, "dbutils", "config.json")

	open, err := os.Open(ConfigPath)
	if err != nil {
		log.Error(err.Error())
		os.Exit(1)
	}

	all, err := ioutil.ReadAll(open)
	if err != nil {
		log.Error(err.Error())
		os.Exit(1)
	}

	err = json.Unmarshal(all, &Pgconfigdata)
	if err != nil {
		log.Error(err.Error())
		os.Exit(1)
	}

}
