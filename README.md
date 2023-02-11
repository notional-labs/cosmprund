# Cosmos-Pruner

The goal of this project is to be able to prune a tendermint data base of blocks and an Cosmos-sdk application DB of all but the last X versions. This will allow people to not have to state sync every x days. 

This tool works with a subset of modules. While an application may have modules outside the scope of this tool , this tool will prune the default sdk module, and osmosis added module. 

## WARNING

Due to inefficiencies of iavl and the simple approach of this tool, it can take ages to prune the data of a large node.  

We are working on integrating this natively into the Cosmos-sdk and Tendermint

## How to use

Cosmprund works of a data directory that has the same structure of a normal cosmos-sdk/tendermint node. By default it will prune all but 10 blocks from tendermint, and all but 10 versions of application state. 

> Note: Application pruning can take a very long time dependent on the size of the db. 


```
# clone & build cosmprund repo
git clone https://github.com/binaryholdings/cosmprund
cd cosmprund
make build

# stop daemon/cosmovisor
sudo systemctl stop cosmovisor

# run cosmprund 
./build/cosmprund prune ~/.gaiad/data --cosmos-sdk=false
```

Flags: 

- `data-dir`: path to data directory if not default
- `blocks`: amount of blocks to keep on the node (Default 10)
- `versions`: amount of app state versions to keep on the node (Default 10)
- `app`: the application you want to prune, outside the sdk default modules. See `Supported Apps`
- `cosmos-sdk`: If pruning a non cosmos-sdk chain, like Nomic, you only want to use tendermint pruning or if you want to only prune tendermint block & state as this is generally large on machines(Default true)
- `tendermint`: If the user wants to only prune application data they can disable pruning of tendermint data. (Default true)
- `tx_index`: set to false you dont want to prune tx_index.db (default true)
- `compact`: set to false you dont want to compact dbs after prunning (default true)

#### Supported Apps:
- osmosis: Osmosis
- starname: Starname
- regen: Regen
- akash: Akash
- cosmoshub: Gaia
- sentinel: Sentinel
- emoney: E-Money
- ixo: Ixo
- juno: Juno
- sifchain: Sifchain
- likecoin: Likecoin
- kichain: Ki
- cyber: Cyber
- cheqd: Cheqd
- stargaze: Stargaze
- bandchain: Band
- chihuahua: Chihuahua
- kava: Kava
- bitcanna: BitCanna
- konstellation: Konstellation
- omniflixhub: Omniflix
- terra: Terra
- vidulum: Vidulum
- provenance: Provenance
- dig: Dig
- gravitybridge: Gravity-Bridge
- comdex: Comdex
- cerberus: Cerberus
- bitsong: BitSong
- assetmantle: AssetMantle
- fetchhub: FetchAI
- evmos: Evmos
- persistent: Persistence
- cryptoorgchain: Crypto.org
- irisnet: IRISnet
- axelar: Axelar
- umee: Umee
- terra2: Terra2
- mars: Mars Protocol
- stride: stride


### Note
To use this with RocksDB you must:

```bash
go install -ldflags '-w -s -X github.com/cosmos/cosmos-sdk/types.DBBackend=rocksdb' -tags rocksdb ./...
```
