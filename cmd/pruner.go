package cmd

import (
	"encoding/binary"
	"fmt"
	"github.com/cockroachdb/pebble"
	storetypes "github.com/cosmos/cosmos-sdk/store/types"
	"path/filepath"
	"strconv"
	"strings"

	sdk "github.com/cosmos/cosmos-sdk/types"
	"github.com/spf13/cobra"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/tendermint/tendermint/state"
	tmstore "github.com/tendermint/tendermint/store"
	db "github.com/tendermint/tm-db"

	"github.com/binaryholdings/cosmos-pruner/internal/rootmulti"
)

// to figuring out the height to prune tx_index
var txIdxHeight int64 = 0

// load db
// load app store and prune
// if immutable tree is not deletable we should import and export current state

func pruneCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "prune [path_to_home]",
		Short: "prune data from the application store and block store",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {

			//ctx := cmd.Context()
			//errs, _ := errgroup.WithContext(ctx)
			var err error
			if tendermint {
				if err = pruneTMData(args[0]); err != nil {
					fmt.Println(err.Error())
				}
			}

			if cosmosSdk {
				err = pruneAppState(args[0])
				if err != nil {
					fmt.Println(err.Error())
				}
			}

			if tx_idx {
				err = pruneTxIndex(args[0])
				if err != nil {
					fmt.Println(err.Error())
				}
			}

			return nil
		},
	}
	return cmd
}

func pruneTxIndex(home string) error {
	fmt.Println("pruning tx_index and block")
	txIdxDB, err := openDB("tx_index", home)
	if err != nil {
		return err
	}

	defer func() {
		errClose := txIdxDB.Close()
		if errClose != nil {
			fmt.Println(errClose.Error())
		}
	}()

	pruneHeight := txIdxHeight - int64(blocks) - 10
	if pruneHeight <= 0 {
		fmt.Printf("No need to prune (pruneHeight=%d)\n", pruneHeight)
		return nil
	}

	pruneBlockIndex(txIdxDB, pruneHeight)
	fmt.Println("finished pruning block")

	pruneTxIndexTxs(txIdxDB, pruneHeight)
	fmt.Println("finished pruning tx_index")

	if compact {
		fmt.Println("compacting tx_index")
		if err := compactDB(txIdxDB); err != nil {
			fmt.Println(err.Error())
		}
	}

	return nil
}

func pruneTxIndexTxs(db db.DB, pruneHeight int64) {
	itr, itrErr := db.Iterator(nil, nil)
	if itrErr != nil {
		panic(itrErr)
	}

	defer itr.Close()

	///////////////////////////////////////////////////
	// delete index by hash and index by height

	bat := db.NewBatch()
	counter := 0

	for ; itr.Valid(); itr.Next() {
		key := itr.Key()
		value := itr.Value()

		strKey := string(key)

		if strings.HasPrefix(strKey, "tx.height") { // index by height
			strs := strings.Split(strKey, "/")
			intHeight, _ := strconv.ParseInt(strs[2], 10, 64)

			if intHeight < pruneHeight {
				//db.Delete(value)
				//db.Delete(key)
				bat.Delete(value)
				bat.Delete(key)
				counter += 2
			}
		} else {
			if len(value) == 32 { // maybe index tx by events
				strs := strings.Split(strKey, "/")
				if len(strs) == 4 { // index tx by events
					intHeight, _ := strconv.ParseInt(strs[2], 10, 64)
					if intHeight < pruneHeight {
						//db.Delete(key)
						//db.DeleteSync(key)
						bat.Delete(key)
						counter++
					}
				}
			}
		}

		if counter >= 1000 {
			bat.WriteSync()
			counter = 0
			bat.Close()
			bat = db.NewBatch()
		}
	}

	bat.WriteSync()
	bat.Close()
}

func pruneBlockIndex(db db.DB, pruneHeight int64) {
	itr, itrErr := db.Iterator(nil, nil)
	if itrErr != nil {
		panic(itrErr)
	}

	defer itr.Close()

	bat := db.NewBatch()
	counter := 0

	for ; itr.Valid(); itr.Next() {
		key := itr.Key()
		value := itr.Value()

		strKey := string(key)

		if strings.HasPrefix(strKey, "block.height") /* index block primary key*/ || strings.HasPrefix(strKey, "block_events") /* BeginBlock & EndBlock */ {
			intHeight := int64FromBytes(value)
			//fmt.Printf("intHeight: %d\n", intHeight)

			if intHeight < pruneHeight {
				//db.Delete(key)
				//db.DeleteSync(key)
				bat.Delete(key)
				counter++
			}
		}

		if counter >= 1000 {
			bat.WriteSync()
			counter = 0
			bat.Close()
			bat = db.NewBatch()
		}
	}

	bat.WriteSync()
	bat.Close()
}

func pruneAppState(home string) error {
	appDB, errDB := openDB("application", home)
	if errDB != nil {
		return errDB
	}

	defer appDB.Close()

	var err error

	//TODO: need to get all versions in the store, setting randomly is too slow
	fmt.Println("pruning application state")

	//// only mount keys from core sdk
	//// todo allow for other keys to be mounted
	//keys := types.NewKVStoreKeys(
	//	authtypes.StoreKey, banktypes.StoreKey, stakingtypes.StoreKey,
	//	minttypes.StoreKey, distrtypes.StoreKey, slashingtypes.StoreKey,
	//	govtypes.StoreKey, paramstypes.StoreKey, ibchost.StoreKey, upgradetypes.StoreKey,
	//	evidencetypes.StoreKey, ibctransfertypes.StoreKey, capabilitytypes.StoreKey,
	//)

	if app == "osmosis" {
		fmt.Println("not support osmosis AppState, exit.")
		return nil
	}

	keys := getStoreKeys(appDB)

	// TODO: cleanup app state
	appStore := rootmulti.NewStore(appDB)

	if txIdxHeight <= 0 {
		txIdxHeight = appStore.LastCommitID().Version
		fmt.Printf("[pruneAppState] set txIdxHeight=%d\n", txIdxHeight)
	}

	for _, value := range keys {
		appStore.MountStoreWithDB(storetypes.NewKVStoreKey(value), sdk.StoreTypeIAVL, nil)
	}

	err = appStore.LoadLatestVersion()
	if err != nil {
		return err
	}

	allVersions := appStore.GetAllVersions()

	v64 := make([]int64, len(allVersions))
	for i := 0; i < len(allVersions); i++ {
		v64[i] = int64(allVersions[i])
	}

	fmt.Println(len(v64))
	versionsToPrune := int64(len(v64)) - int64(versions)
	fmt.Printf("[pruneAppState] versionsToPrune=%d\n", versionsToPrune)
	if versionsToPrune <= 0 {
		fmt.Printf("[pruneAppState] No need to prune (%d)\n", versionsToPrune)
	} else {
		appStore.PruneHeights = v64[:versionsToPrune]
		appStore.PruneStores()
	}

	if compact {
		fmt.Println("compacting application state")
		if err := compactDB(appDB); err != nil {
			fmt.Println(err.Error())
		}
	}

	return nil
}

// pruneTMData prunes the tendermint blocks and state based on the amount of blocks to keep
func pruneTMData(home string) error {
	blockStoreDB, errDBBlock := openDB("blockstore", home)
	if errDBBlock != nil {
		return errDBBlock
	}

	blockStore := tmstore.NewBlockStore(blockStoreDB)
	defer blockStore.Close()

	// Get StateStore
	stateDB, errDBBState := openDB("state", home)
	if errDBBState != nil {
		return errDBBState
	}

	var err error

	stateStore := state.NewStore(stateDB)
	defer stateStore.Close()

	base := blockStore.Base()

	pruneHeight := blockStore.Height() - int64(blocks)
	fmt.Printf("[pruneTMData] pruneHeight=%d\n", pruneHeight)
	if pruneHeight <= 0 {
		fmt.Println("[pruneTMData] No need to prune")
		return nil
	}

	if txIdxHeight <= 0 {
		txIdxHeight = blockStore.Height()
		fmt.Printf("[pruneTMData] set txIdxHeight=%d\n", txIdxHeight)
	}

	fmt.Println("pruning block store")

	// prune block store
	// prune one by one instead of range to avoid `panic: pebble: batch too large: >= 4.0 G` issue
	// (see https://github.com/notional-labs/cosmprund/issues/11)
	for pruneBlockFrom := base; pruneBlockFrom < pruneHeight-1; pruneBlockFrom += rootmulti.PRUNE_BATCH_SIZE {
		height := pruneBlockFrom
		if height >= pruneHeight-1 {
			height = pruneHeight - 1
		}

		_, err = blockStore.PruneBlocks(height)
		if err != nil {
			//return err
			fmt.Println(err.Error())
		}
	}

	if compact {
		fmt.Println("compacting block store")
		if err := compactDB(blockStoreDB); err != nil {
			fmt.Println(err.Error())
		}
	}

	fmt.Println("pruning state store")
	// prune state store
	// prune one by one instead of range to avoid `panic: pebble: batch too large: >= 4.0 G` issue
	// (see https://github.com/notional-labs/cosmprund/issues/11)
	for pruneStateFrom := base; pruneStateFrom < pruneHeight-1; pruneStateFrom += rootmulti.PRUNE_BATCH_SIZE {
		endHeight := pruneStateFrom + rootmulti.PRUNE_BATCH_SIZE
		if endHeight >= pruneHeight-1 {
			endHeight = pruneHeight - 1
		}
		err = stateStore.PruneStates(pruneStateFrom, endHeight)
		if err != nil {
			//return err
			fmt.Println(err.Error())
		}
	}

	if compact {
		fmt.Println("compacting state store")
		if err := compactDB(stateDB); err != nil {
			fmt.Println(err.Error())
		}
	}

	return nil
}

// Utils

func openDB(dbname string, home string) (db.DB, error) {
	dbType := db.BackendType(backend)
	dbDir := rootify(dataDir, home)

	var db1 db.DB

	if dbType == db.GoLevelDBBackend {
		o := opt.Options{
			DisableSeeksCompaction: true,
		}

		lvlDB, err := db.NewGoLevelDBWithOpts(dbname, dbDir, &o)
		if err != nil {
			return nil, err
		}

		db1 = lvlDB
	} else if dbType == db.PebbleDBBackend {
		opts := &pebble.Options{
			MaxOpenFiles: 100,
			//DisableAutomaticCompactions: true, // freeze when pruning!
		}
		opts.EnsureDefaults()

		ppDB, err := db.NewPebbleDBWithOpts(dbname, dbDir, opts)
		if err != nil {
			return nil, err
		}

		db1 = ppDB
	} else {
		var err error
		db1, err = db.NewDB(dbname, dbType, dbDir)
		if err != nil {
			return nil, err
		}
	}

	return db1, nil
}

func compactDB(vdb db.DB) error {
	dbType := db.BackendType(backend)

	if dbType == db.GoLevelDBBackend {
		vdbLevel := vdb.(*db.GoLevelDB)

		if err := vdbLevel.ForceCompact(nil, nil); err != nil {
			return err
		}
	} else if dbType == db.PebbleDBBackend {
		vdbPebble := vdb.(*db.PebbleDB).DB()

		iter := vdbPebble.NewIter(nil)
		//defer iter.Close()

		var start, end []byte

		if iter.First() {
			start = cp(iter.Key())
		}

		if iter.Last() {
			end = cp(iter.Key())
		}

		// close iter before compacting
		iter.Close()

		err := vdbPebble.Compact(start, end, false)
		if err != nil {
			return err
		}
	}

	return nil
}

func getStoreKeys(db db.DB) (storeKeys []string) {
	latestVer := rootmulti.GetLatestVersion(db)
	latestCommitInfo, err := getCommitInfo(db, latestVer)
	if err != nil {
		panic(err)
	}

	for _, storeInfo := range latestCommitInfo.StoreInfos {
		storeKeys = append(storeKeys, storeInfo.Name)
	}
	return
}

func getCommitInfo(db db.DB, ver int64) (*storetypes.CommitInfo, error) {
	const commitInfoKeyFmt = "s/%d" // s/<version>
	cInfoKey := fmt.Sprintf(commitInfoKeyFmt, ver)

	bz, err := db.Get([]byte(cInfoKey))
	if err != nil {
		return nil, fmt.Errorf("failed to get commit info: %s", err)
	} else if bz == nil {
		return nil, fmt.Errorf("no commit info found")
	}

	cInfo := &storetypes.CommitInfo{}
	if err = cInfo.Unmarshal(bz); err != nil {
		return nil, fmt.Errorf("failed unmarshal commit info: %s", err)
	}

	return cInfo, nil
}

func cp(bz []byte) (ret []byte) {
	ret = make([]byte, len(bz))
	copy(ret, bz)
	return ret
}

func rootify(path, root string) string {
	if filepath.IsAbs(path) {
		return path
	}
	return filepath.Join(root, path)
}

func int64FromBytes(bz []byte) int64 {
	v, _ := binary.Varint(bz)
	return v
}
