package importer

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/ethereum/go-ethereum-replayer/dbutils"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/prque"
	"github.com/ethereum/go-ethereum/consensus"
	"github.com/ethereum/go-ethereum/consensus/ethash"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/params"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/ethereum/go-ethereum/trie"
	"math/big"
	_ "net/http/pprof"
	"os"
	"path/filepath"
	"reflect"
	"time"
	"unsafe"
)

type ChainReplayer struct {
	client        *ethclient.Client           //Rpc
	dataDir       string                      //数据存放目录
	db            ethdb.Database              //DB
	genesisBlock  *types.Block                //创世区块
	startBlock    *types.Block                //开始同步的区块
	chainConfig   *params.ChainConfig         //链配置
	engine        consensus.Engine            //共识引擎
	importNum     int                         //一次恢复多少个块
	chainReader   consensus.ChainHeaderReader //HeaderReader
	cacheConfig   *core.CacheConfig           //缓存配置
	stateCache    state.Database              //stateDb 内存
	triegc        *prque.Prque                //需要清除的state
	triesInMemory uint64                      //内存里面存的tries的个数
	dataDB        *sql.DB                     // 解析数据的入库db
}

// 任务超时时间
var duration time.Duration = 20 * time.Minute

func NewChainReplayer(rpc string, dataDir string) (*ChainReplayer, error) {
	client, err := ethclient.Dial(rpc)
	if err != nil {
		log.Error("[Replayer] new client", "error", err)
		return nil, err
	}

	return &ChainReplayer{

		client:  client,
		dataDir: dataDir,

		engine:    ethash.NewFaker(),
		importNum: 2000000, // 单次任务重跑的记录数

		triegc:        prque.New(nil),
		triesInMemory: 128,
	}, nil
}

func (c *ChainReplayer) init() error {

	//===初始化 以太坊的 leveldb 数据库
	if c.dataDir == "" {
		c.dataDir, _ = os.Getwd()
	}
	// defer os.RemoveAll(datadir)
	path := filepath.Join(c.dataDir, "geth", "chaindata")
	db, err := rawdb.NewLevelDBDatabase(path, 128, 1024, "", false)
	if err != nil {
		log.Error("[Replayer] Failed to create persistent database", "error", err)
		return err
	}

	c.db = db
	//===创世区块
	genesis := core.DefaultGenesisBlock()

	chainConfig, genesisHash, err := core.SetupGenesisBlock(db, genesis)
	if err != nil {
		log.Error("[Replayer] Failed to gen genesis block", "error", err)
		return err
	}
	c.genesisBlock = rawdb.ReadBlock(db, genesisHash, 0)
	if c.genesisBlock == nil {
		log.Error("[Replayer] Failed to read genesis block from db", "error", err)
		return err
	}
	c.chainConfig = chainConfig
	log.Info("[Replayer] Genesis block Hash:", "hash", c.genesisBlock.Hash().String())

	c.startBlock = c.genesisBlock
	commit := ReadLastCommitNumber(db)
	if commit != nil {
		// 自定义从哪个区块开始重放
		c.startBlock, err = c.getBlock(big.NewInt(int64(*commit)))
		//c.startBlock, err = c.getBlock(big.NewInt(int64(3571000)))
		if err != nil {
			log.Error("[Replayer] Failed to get block", "error", err)
			return err
		}
	}
	log.Info("[Replayer] Find last commit block", "block", c.startBlock.NumberU64())

	c.chainReader = &fakeChainReader{config: c.chainConfig}
	c.cacheConfig = &core.CacheConfig{
		TrieCleanLimit: 32,
		TrieDirtyLimit: 32,
		TrieTimeLimit:  5 * time.Minute,
		SnapshotLimit:  0, // Disable snapshot by default
	}
	//状态DB
	c.stateCache = state.NewDatabaseWithConfig(c.db, &trie.Config{Cache: 256})

	dbutils.Init()
	c.dataDB = dbutils.CreateDB()

	return nil
}

func (c *ChainReplayer) Start() error {

	// 主程序超时，程序停止
	newtimer := time.NewTimer(duration)
	chan_err := make(chan error)
	var err error

	go func(ec chan<- error) {
		err1 := c.init()
		if err1 != nil {
			chan_err <- err1
		} else {
			chan_err <- c.replay(newtimer)
		}
	}(chan_err)

	select {
	case err = <-chan_err:
		log.Error(err.Error())
		os.Exit(1)
	case <-newtimer.C:
		c.dataDB.Close()
		log.Error("db timeout minute: " + duration.String())
		log.Error(err.Error())
		os.Exit(1)
	}

	return err
}

func (c *ChainReplayer) Stop() error {
	return nil
}

func (c *ChainReplayer) replay(newtimer *time.Timer) error {

	callback := func(i int, stateDb *state.StateDB, g *ImportProcessor) {
		v := reflect.ValueOf(stateDb).Elem()
		fStateObjects := v.FieldByName("stateObjectsDirty")
		f := GetUnexportedField(fStateObjects)
		stateObjectsDirty := f.(map[common.Address]struct{})

		for addr, _ := range stateObjectsDirty {
			tempVar := []string{"0xETH", addr.String(), stateDb.GetBalance(addr).String(), "", ""}
			dbutils.AppendRecord(tempVar)
		}
	}

	config := c.chainConfig
	//blocks, receipts := make(types.Blocks, c.importNum), make([]types.Receipts, c.importNum)
	triedb := c.stateCache.TrieDB()

	importBlock := func(i int, parent *types.Block, statedb *state.StateDB) (*types.Block, types.Receipts, error) {
		//t1 := time.Now()
		blockNum := new(big.Int).Add(parent.Number(), common.Big1)

		//去链上查数据
		block, err := c.getBlock(blockNum)
		if err != nil {
			return nil, nil, fmt.Errorf("[Replayer] Get block = %v  error: %v", blockNum, err)
		}
		// 测试插入
		dbutils.NewVar(block.Number(), block.Header().Time)

		//定义区块导入
		b := &ImportProcessor{
			i: i,
			//chain:   blocks,
			block:   block,
			parent:  parent,
			statedb: statedb,
			config:  config,
			engine:  c.engine,
			traces:  make([]*txTraceResult, len(block.Transactions())),
		}
		//重放区块的交易生成新的状态
		//err = b.Process(c)
		err = b.DebugProcess(c)
		if err != nil {
			return nil, nil, fmt.Errorf("[Replayer] Replay block = %v  error: %v", blockNum, err)
		}
		//提交修改到内存
		b.engine.Finalize(c.chainReader, block.Header(), statedb, block.Transactions(), block.Uncles())
		//回调进行数据处理
		callback(i, statedb, b)
		//提交数据到DB(这里DB为 state.Database)
		//所有的数据都放到这个缓存DB里面
		root, err := statedb.Commit(config.IsEIP158(block.Header().Number))
		if err != nil {
			return nil, nil, fmt.Errorf("[Replayer] State write block = %v error: %v", blockNum, err)
		}

		//只保留最近的 32M 的 triedb
		triedb.Reference(root, common.Hash{})
		c.triegc.Push(root, -int64(block.NumberU64()))
		if current := block.NumberU64(); current > c.triesInMemory {
			var (
				nodes, imgs = triedb.Size()
				limit       = common.StorageSize(c.cacheConfig.TrieDirtyLimit) * 1024 * 1024
			)
			//fmt.Println("block number: ", blockNum, "triedb size:  ", nodes, "compare :", nodes-limit, "limit : ", limit)
			if nodes > limit || imgs > 4*1024*1024 {
				triedb.Cap(limit - ethdb.IdealBatchSize)
			}
			// 128个区块后 每个引用一次子节点
			chosen := current - c.triesInMemory
			for !c.triegc.Empty() {
				root, number := c.triegc.Pop()
				if uint64(-number) > chosen {
					c.triegc.Push(root, number)
					break
				}
				triedb.Dereference(root.(common.Hash))
			}
		}

		// 将 balance 和 storage 存入 缓存
		dbutils.InsertStorageCache()
		// 日志记录区块高度
		number := block.Number().Int64()
		if number%100 == 0 {
			// 插入 余额记录

			dbutils.ExecuteInsertDB(c.dataDB)
			// 插入 合约内部交易记录
			dbutils.ExecInsert(c.dataDB)

			//这里快照数据直接提交持久化DB
			if err := triedb.Commit(root, false, nil); err != nil {
				return nil, nil, fmt.Errorf("[Replayer] Trie write block = %v error: %v", blockNum, err)
			}
			WriteLastCommitNumber(c.db, blockNum.Uint64())
			// debug内存消耗较大的位置
			//if number == 2473200 {
			//	http.ListenAndServe("0.0.0.0:6061", nil)
			//}

		}

		// 重新设定主线程任务时间
		newtimer.Reset(duration)

		return block, b.receipts, nil
	}

	parent := c.startBlock
	for i := 0; i < c.importNum; i++ {
		statedb, err := state.New(parent.Root(), c.stateCache, nil)
		if err != nil {
			return err
		}

		block, _, err := importBlock(i, parent, statedb)
		if err != nil {
			return err
		}
		parent = block

	}
	return nil
}

//获取链上数据
func (c *ChainReplayer) getBlock(blockNum *big.Int) (*types.Block, error) {
	block, err := c.client.BlockByNumber(context.Background(), blockNum)

	if err != nil {
		log.Error("[Replayer] Failed to get block", "block", blockNum, "error", err)
		return nil, err
	}
	return block, nil
}

//打印现有的账户
func (c *ChainReplayer) printAcct() error {
	t, err := trie.NewSecure(common.Hash{}, c.startBlock.Root(), trie.NewDatabase(c.db))
	if err != nil {
		return err
	}
	it := trie.NewIterator(t.NodeIterator(nil))
	for it.Next() {
		var acc types.StateAccount
		if err := rlp.DecodeBytes(it.Value, &acc); err != nil {
			return err
		}
		addrBytes := t.GetKey(it.Key)
		if addrBytes != nil {
			addr := common.BytesToAddress(addrBytes)
			// it.Prove()
			fmt.Println("adddress", addr, "balanace", acc.Balance)
		}
	}
	return nil
}

func GetUnexportedField(field reflect.Value) interface{} {
	return reflect.NewAt(field.Type(), unsafe.Pointer(field.UnsafeAddr())).Elem().Interface()
}

// 判断地址是 合约地址还是外部地址
func getAddrType(statedb *state.StateDB, addr common.Address) int {
	object := statedb.GetOrNewStateObject(addr)
	v := reflect.ValueOf(object).Elem()
	fcode := v.FieldByName("code")
	f := GetUnexportedField(fcode)
	code := f.(state.Code)
	if code == nil {
		return 0
	}
	return 1
}
