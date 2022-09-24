package dbutils

import (
	"database/sql"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/log"
	_ "github.com/lib/pq"
	"math/big"
	"os"
	"strings"
	"time"
)

var (
	BlockNumber *big.Int
	BlockTime   *big.Int
	Record      [][]string
	insert_num  int = 0

	builder1 strings.Builder

	traceBuilder   strings.Builder
	traceRecordNum int
)

type CallFrame struct {
	Type    string      `json:"type"`
	From    string      `json:"from"`
	To      string      `json:"to,omitempty"`
	Value   string      `json:"value,omitempty"`
	Gas     string      `json:"gas"`
	GasUsed string      `json:"gasUsed"`
	Input   string      `json:"input"`
	Output  string      `json:"output,omitempty"`
	Error   string      `json:"error,omitempty"`
	Calls   []CallFrame `json:"calls,omitempty"`
}

func NewVar(bn *big.Int, bt uint64) {
	BlockNumber = bn
	BlockTime = new(big.Int).SetUint64(bt)
	Record = [][]string{}
}

func AppendRecord(r []string) {
	Record = append(Record, r)
	insert_num += 1
}

// 组合insert storage 的 sql
func InsertStorageCache() {

	for _, strs := range Record {
		str := "(" + BlockNumber.String() + "," + BlockTime.String() + ",'" + strs[0] + "','" + strs[1] + "','" + strs[2] + "','" + strs[3] + "','" + strs[4] + "')"
		if builder1.Len() == 0 {
			builder1.WriteString("INSERT INTO " + Pgconfigdata.TableBalance + "(block_number,block_time,contract_addr,var_key,var_value,key_op_method,value_op_method) VALUES ")
			builder1.WriteString(str)
			continue
		}
		builder1.WriteString("," + str)

	}

}

// 组合insert eth call 的 sql
func AppendEthCallRecord(blockNumber string, txHash string, txIndex string, res *CallFrame) {

	str := "(" + blockNumber + ",'" + txHash + "'," + txIndex + ",'" + res.Type + "','" + res.From + "','" + res.To + "','" + new(big.Int).SetBytes(common.FromHex(res.Value)).String() + "'," + new(big.Int).SetBytes(common.FromHex(res.Gas)).String() + "," + new(big.Int).SetBytes(common.FromHex(res.GasUsed)).String() + ")"
	if traceBuilder.Len() == 0 {
		traceBuilder.WriteString("INSERT INTO " + Pgconfigdata.TableCall + "(block_number,tx_hash,tx_index,call_type,from_addr,to_addr,transfer_value,gas,gas_used) VALUES ")
		traceBuilder.WriteString(str)
		traceRecordNum = 1
	} else {
		traceBuilder.WriteString("," + str)
		traceRecordNum++
	}

	for _, r := range res.Calls {
		value := new(big.Int).SetBytes(common.FromHex(r.Value))
		gas := new(big.Int).SetBytes(common.FromHex(r.Gas))
		gasUsed := new(big.Int).SetBytes(common.FromHex(r.GasUsed))
		if value.Cmp(new(big.Int).SetInt64(0)) == 0 && gas.Cmp(new(big.Int).SetInt64(0)) == 0 && gasUsed.Cmp(new(big.Int).SetInt64(0)) == 0 {
			continue
		}

		str = "(" + blockNumber + ",'" + txHash + "'," + txIndex + ",'" + r.Type + "','" + r.From + "','" + r.To + "','" + value.String() + "'," + gas.String() + "," + gasUsed.String() + ")"
		traceBuilder.WriteString("," + str)
		traceRecordNum++
	}
}

func assertNoError(e error) {
	if e != nil {
		panic(e)
	}
}

// 创建数据库连接,这里用的pgsql
func CreateDB() (db *sql.DB) {
	dsn := "postgres://" + Pgconfigdata.Username + ":" + Pgconfigdata.Password + "@" + Pgconfigdata.Endpoint + ":" + Pgconfigdata.Port + "/" + Pgconfigdata.Database + "?sslmode=disable"
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		log.Error(err.Error())
		os.Exit(1)
	}

	return db
}

// 插入余额记录到 数据库
func ExecuteInsertDB(db *sql.DB) {

	// 全局变量 改为局部变量，方便后面异步执行任务，和对全局变量初始化
	should_insert := insert_num
	sql := builder1.String()
	number := BlockNumber
	use := db.Stats().InUse
	log.Info("insert before", "block_number", BlockNumber, "record num", insert_num, "used connected num", use)

	if use > 40 {
		log.Error("db connect num is over 40 ", "now block number", BlockNumber)
		os.Exit(1)
	}
	if use >= 20 {
		time.Sleep(10 * time.Second)
	}

	// 异步执行入库操作
	go func() {
		t1 := time.Now()
		exce, err := db.Exec(sql)
		if err != nil {
			log.Error(err.Error())
			log.Error("insert fail", "block number ", BlockNumber)
			os.Exit(1)
		}
		affected, err := exce.RowsAffected()
		if err != nil {
			log.Error(err.Error())
			log.Error("insert fail", "block number ", BlockNumber)
			os.Exit(1)
		}
		if affected != int64(should_insert) {
			log.Error("insert num is not equeue result record")
			os.Exit(1)
		}
		t2 := time.Now()
		sub := t2.Sub(t1)
		log.Info("insert Block Height and affect num", "block_number", number, "affect_num", affected, "insert cost time", sub.Seconds())
	}()

	insert_num = 0
	builder1.Reset()

}

// 插入 trace call 记录入库
func ExecInsert(db *sql.DB) {
	// 全局变量 改为局部变量，方便后面异步执行任务，和对全局变量初始化

	if traceRecordNum == 0 {
		traceBuilder.Reset()
		traceRecordNum = 0
		return
	}

	number := BlockNumber
	use := db.Stats().InUse
	sql := traceBuilder.String()
	num := traceRecordNum
	log.Info("insert trace record before", "block_number", BlockNumber, "record num", num, "used connected num", use)

	if use > 40 {
		log.Error("db connect num is over 40 ", "now block number", BlockNumber)
		os.Exit(1)
	}
	if use >= 20 {
		time.Sleep(10 * time.Second)
	}

	// 异步执行入库操作
	go func() {
		t1 := time.Now()
		exce, err := db.Exec(sql)
		if err != nil {
			log.Error(err.Error())
			log.Error("insert trace record fail", "block number ", BlockNumber)
			os.Exit(1)
		}
		affected, err := exce.RowsAffected()
		if err != nil {
			log.Error(err.Error())
			log.Error("insert trace record fail", "block number ", BlockNumber)
			os.Exit(1)
		}
		if affected != int64(num) {
			log.Error("insert trace record num is not equeue result record", "block number ", BlockNumber)
			os.Exit(1)
		}
		t2 := time.Now()
		sub := t2.Sub(t1)
		log.Info("insert trace record's Block Height and affect num", "block_number", number, "affect_num", affected, "insert cost time", sub.Seconds())
	}()

	traceBuilder.Reset()
	traceRecordNum = 0

}
