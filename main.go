package main

import (
	"github.com/ethereum/go-ethereum-replayer/importer"
	"github.com/ethereum/go-ethereum/log"
	"os"
)

func main() {
	log.Root().SetHandler(log.LvlFilterHandler(log.LvlInfo, log.StreamHandler(os.Stderr, log.TerminalFormat(true))))
	//**定义链上数据请求的,节点连接
	rpc := "https://eth-mainnet.g.alchemy.com/v2/..................."
	datadir, _ := os.Getwd()
	replayer, err := importer.NewChainReplayer(rpc, datadir)

	if err != nil {
		log.Error(err.Error())
		return
	}
	replayer.Start()

}
