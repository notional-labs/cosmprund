# Cosmos-Pruner

This is a fork of [cosmprund](https://github.com/binaryholdings/cosmprund) with some improvements:
- support pebbledb
- prune all stores of app state
- support blockstore prunning
- support tx_index prunning


## Usage

```
# clone & build cosmprund repo
git clone https://github.com/binaryholdings/cosmprund
cd cosmprund
make build

# run cosmprund 
./build/cosmprund prune ~/.gaiad/data --app=sei --backend=pebbledb --blocks=362880 --versions=362880 --compact=true
```

Flags: 

- `data-dir`: path to data directory if not default
- `blocks`: amount of blocks to keep on the node (Default 10)
- `versions`: amount of app state versions to keep on the node (Default 10)
- `app`: deprecated! the application you want to prune, outside the sdk default modules.
- `cosmos-sdk`: If pruning a non cosmos-sdk chain, like Nomic, you only want to use tendermint pruning or if you want to only prune tendermint block & state as this is generally large on machines(Default true)
- `tendermint`: If the user wants to only prune application data they can disable pruning of tendermint data. (Default true)
- `tx_index`: set to false you dont want to prune tx_index.db (default true)
- `compact`: set to false you dont want to compact dbs after prunning (default true)
