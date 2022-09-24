package importer

import (
	"encoding/json"
	"fmt"
	"github.com/ethereum/go-ethereum-replayer/dbutils"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/consensus"
	"github.com/ethereum/go-ethereum/consensus/misc"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/params"
	"github.com/ethereum/go-ethereum/rlp"
	"math/big"
	"strconv"
)

type txTraceResult struct {
	Result interface{} `json:"result,omitempty"` // Trace results produced by the tracer
	Error  string      `json:"error,omitempty"`  // Trace failure produced by the tracer
}

type ImportProcessor struct {
	i             int
	parent, block *types.Block
	chain         []*types.Block
	statedb       *state.StateDB

	receipts types.Receipts
	allLogs  []*types.Log
	traces   []*txTraceResult

	config *params.ChainConfig
	engine consensus.Engine
}

// 不需要追踪日志的话，就用这个执行交易
func (b *ImportProcessor) Process(c *ChainReplayer) error {
	var (
		usedGas = new(uint64)
		header  = b.block.Header()
		gp      = new(core.GasPool).AddGas(b.block.GasLimit())
		cc      = &chainContext{b.engine, header, c}
	)
	if b.config.DAOForkSupport && b.config.DAOForkBlock != nil && b.config.DAOForkBlock.Cmp(b.block.Number()) == 0 {
		misc.ApplyDAOHardFork(b.statedb)
	}

	for i, tx := range b.block.Transactions() {
		b.statedb.Prepare(tx.Hash(), i)

		receipt, err := core.ApplyTransaction(b.config, cc, &header.Coinbase, gp, b.statedb, header, tx, usedGas, vm.Config{})
		if err != nil {
			return fmt.Errorf("could not apply tx %d [%v]: %w", i, tx.Hash().Hex(), err)
		}
		b.receipts = append(b.receipts, receipt)
		b.allLogs = append(b.allLogs, receipt.Logs...)

	}

	return nil
}

// 追踪日志的话，就用这个任务执行交易
func (b *ImportProcessor) DebugProcess(c *ChainReplayer) error {
	var (
		statedb     = b.statedb
		chainConfig = b.config
		usedGas     = new(uint64)
		block       = b.block
		header      = b.block.Header()
		gp          = new(core.GasPool).AddGas(b.block.GasLimit())

		cc = &chainContext{b.engine, b.parent.Header(), c}
	)
	if chainConfig.DAOForkSupport && chainConfig.DAOForkBlock != nil && chainConfig.DAOForkBlock.Cmp(block.Number()) == 0 {
		misc.ApplyDAOHardFork(b.statedb)
	}

	for i, tx := range block.Transactions() {
		tracer := newCallTracer()

		vmConf := vm.Config{
			Debug:                   true,
			Tracer:                  tracer,
			EnablePreimageRecording: true,
		}
		statedb.Prepare(tx.Hash(), i)
		receipt, err := core.ApplyTransaction(chainConfig, cc, &header.Coinbase, gp, statedb, header, tx, usedGas, vmConf)
		if err != nil {
			return fmt.Errorf("could not apply tx %d [%v]: %w", i, tx.Hash().Hex(), err)
		}

		//处理调用链路结果
		b.receipts = append(b.receipts, receipt)
		b.allLogs = append(b.allLogs, receipt.Logs...)

		//获取跟踪器结果
		result, err := tracer.GetResult()
		if err != nil {
			log.Error(err.Error())
		}

		res := new(dbutils.CallFrame)
		json.Unmarshal(result, res)

		dbutils.AppendEthCallRecord(block.Number().String(), tx.Hash().String(), strconv.Itoa(i), res)
	}
	return nil
}

func (b *ImportProcessor) Number() *big.Int {
	return new(big.Int).Set(b.block.Header().Number)
}

func (b *ImportProcessor) BaseFee() *big.Int {
	return new(big.Int).Set(b.block.Header().BaseFee)
}

func (b *ImportProcessor) TxNonce(addr common.Address) uint64 {
	if !b.statedb.Exist(addr) {
		panic("account does not exist")
	}
	return b.statedb.GetNonce(addr)
}

type fakeChainReader struct {
	config *params.ChainConfig
}

// Config returns the chain configuration.
func (cr *fakeChainReader) Config() *params.ChainConfig {
	return cr.config
}

func (cr *fakeChainReader) CurrentHeader() *types.Header                            { return nil }
func (cr *fakeChainReader) GetHeaderByNumber(number uint64) *types.Header           { return nil }
func (cr *fakeChainReader) GetHeaderByHash(hash common.Hash) *types.Header          { return nil }
func (cr *fakeChainReader) GetHeader(hash common.Hash, number uint64) *types.Header { return nil }
func (cr *fakeChainReader) GetBlock(hash common.Hash, number uint64) *types.Block   { return nil }
func (cr *fakeChainReader) GetTd(hash common.Hash, number uint64) *big.Int          { return nil }

var (
	lastCommitKey = []byte("LastCommit")
)

func ReadLastCommitNumber(db ethdb.KeyValueReader) *uint64 {
	data, _ := db.Get(lastCommitKey)
	if len(data) == 0 {
		return nil
	}
	var commit uint64
	if err := rlp.DecodeBytes(data, &commit); err != nil {
		log.Error("Invalid pivot block number in database", "err", err)
		return nil
	}
	return &commit
}

func WriteLastCommitNumber(db ethdb.KeyValueWriter, commit uint64) {
	enc, err := rlp.EncodeToBytes(commit)
	if err != nil {
		log.Crit("Failed to encode commit block number", "err", err)
	}
	if err := db.Put(lastCommitKey, enc); err != nil {
		log.Crit("Failed to store commit block number", "err", err)
	}
}

type chainContext struct {
	engine consensus.Engine
	header *types.Header
	c      *ChainReplayer
}

func (context *chainContext) Engine() consensus.Engine { return context.engine }
func (context *chainContext) GetHeader(hash common.Hash, number uint64) *types.Header {
	block, err := context.c.getBlock(big.NewInt(int64(number)))
	if err != nil {
		log.Error("method GetHeader is error , dont get header hash")
	}
	context.header = block.Header()
	//fmt.Println("block_import: ", number)
	return context.header
}
