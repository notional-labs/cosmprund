package cmd

import (
	"encoding/binary"
	"fmt"
	"github.com/cockroachdb/pebble"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/cosmos/cosmos-sdk/types"
	sdk "github.com/cosmos/cosmos-sdk/types"
	authtypes "github.com/cosmos/cosmos-sdk/x/auth/types"
	banktypes "github.com/cosmos/cosmos-sdk/x/bank/types"
	capabilitytypes "github.com/cosmos/cosmos-sdk/x/capability/types"
	distrtypes "github.com/cosmos/cosmos-sdk/x/distribution/types"
	evidencetypes "github.com/cosmos/cosmos-sdk/x/evidence/types"
	govtypes "github.com/cosmos/cosmos-sdk/x/gov/types"
	minttypes "github.com/cosmos/cosmos-sdk/x/mint/types"
	paramstypes "github.com/cosmos/cosmos-sdk/x/params/types"
	slashingtypes "github.com/cosmos/cosmos-sdk/x/slashing/types"
	stakingtypes "github.com/cosmos/cosmos-sdk/x/staking/types"
	upgradetypes "github.com/cosmos/cosmos-sdk/x/upgrade/types"
	ibctransfertypes "github.com/cosmos/ibc-go/v2/modules/apps/transfer/types"
	ibchost "github.com/cosmos/ibc-go/v2/modules/core/24-host"
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
	fmt.Println("pruning tx_index")
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
	for ; itr.Valid(); itr.Next() {
		key := itr.Key()
		value := itr.Value()

		strKey := string(key)

		if strings.HasPrefix(strKey, "tx.height") { // index by height
			strs := strings.Split(strKey, "/")
			intHeight, _ := strconv.ParseInt(strs[2], 10, 64)

			if intHeight < pruneHeight {
				db.Delete(value)
				db.Delete(key)
			}
		} else {
			if len(value) == 32 { // maybe index tx by events
				strs := strings.Split(strKey, "/")
				if len(strs) == 4 { // index tx by events
					intHeight, _ := strconv.ParseInt(strs[2], 10, 64)
					if intHeight < pruneHeight {
						db.Delete(key)
					}
				}
			}
		}
	}
}

func pruneBlockIndex(db db.DB, pruneHeight int64) {
	itr, itrErr := db.Iterator(nil, nil)
	if itrErr != nil {
		panic(itrErr)
	}

	defer itr.Close()

	for ; itr.Valid(); itr.Next() {
		key := itr.Key()
		value := itr.Value()

		strKey := string(key)

		if strings.HasPrefix(strKey, "block.height") /* index block primary key*/ || strings.HasPrefix(strKey, "block_events") /* BeginBlock & EndBlock */ {
			intHeight := int64FromBytes(value)
			//fmt.Printf("intHeight: %d\n", intHeight)

			if intHeight < pruneHeight {
				db.Delete(key)
			}
		}
	}
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

	// only mount keys from core sdk
	// todo allow for other keys to be mounted
	keys := types.NewKVStoreKeys(
		authtypes.StoreKey, banktypes.StoreKey, stakingtypes.StoreKey,
		minttypes.StoreKey, distrtypes.StoreKey, slashingtypes.StoreKey,
		govtypes.StoreKey, paramstypes.StoreKey, ibchost.StoreKey, upgradetypes.StoreKey,
		evidencetypes.StoreKey, ibctransfertypes.StoreKey, capabilitytypes.StoreKey,
	)

	if app == "osmosis" {
		osmoKeys := types.NewKVStoreKeys(
			"icahost",        //icahosttypes.StoreKey,
			"gamm",           // gammtypes.StoreKey,
			"lockup",         //lockuptypes.StoreKey,
			"incentives",     // incentivestypes.StoreKey,
			"epochs",         // epochstypes.StoreKey,
			"poolincentives", //poolincentivestypes.StoreKey,
			"authz",          //authzkeeper.StoreKey,
			"txfees",         // txfeestypes.StoreKey,
			"superfluid",     // superfluidtypes.StoreKey,
			"bech32ibc",      // bech32ibctypes.StoreKey,
			"wasm",           // wasm.StoreKey,
			"tokenfactory",   //tokenfactorytypes.StoreKey,
		)
		for key, value := range osmoKeys {
			keys[key] = value
		}
	} else if app == "cosmoshub" {
		cosmoshubKeys := types.NewKVStoreKeys(
			"liquidity",
			"feegrant",
			"authz",
			"icahost", // icahosttypes.StoreKey
		)
		for key, value := range cosmoshubKeys {
			keys[key] = value
		}
	} else if app == "terra" { // terra classic
		terraKeys := types.NewKVStoreKeys(
			"oracle",   // oracletypes.StoreKey,
			"market",   // markettypes.StoreKey,
			"treasury", //treasurytypes.StoreKey,
			"wasm",     // wasmtypes.StoreKey,
			"authz",    //authzkeeper.StoreKey,
			"feegrant", // feegrant.StoreKey
		)
		for key, value := range terraKeys {
			keys[key] = value
		}
	} else if app == "kava" {
		kavaKeys := types.NewKVStoreKeys(
			"feemarket", //feemarkettypes.StoreKey,
			"authz",     //authzkeeper.StoreKey,
			"kavadist",  //kavadisttypes.StoreKey,
			"auction",   //auctiontypes.StoreKey,
			"issuance",  //issuancetypes.StoreKey,
			"bep3",      //bep3types.StoreKey,
			//"pricefeed", //pricefeedtypes.StoreKey,
			//"swap",      //swaptypes.StoreKey,
			"cdp",       //cdptypes.StoreKey,
			"hard",      //hardtypes.StoreKey,
			"committee", //committeetypes.StoreKey,
			"incentive", //incentivetypes.StoreKey,
			"evmutil",   //evmutiltypes.StoreKey,
			"savings",   //savingstypes.StoreKey,
			"bridge",    //bridgetypes.StoreKey,
		)
		for key, value := range kavaKeys {
			keys[key] = value
		}

		delete(keys, "mint") // minttypes.StoreKey
	} else if app == "evmos" {
		evmosKeys := types.NewKVStoreKeys(
			"feegrant",   // feegrant.StoreKey,
			"authz",      // authzkeeper.StoreKey,
			"evm",        // evmtypes.StoreKey,
			"feemarket",  // feemarkettypes.StoreKey,
			"inflation",  // inflationtypes.StoreKey,
			"erc20",      // erc20types.StoreKey,
			"incentives", // incentivestypes.StoreKey,
			"epochs",     // epochstypes.StoreKey,
			"claims",     // claimstypes.StoreKey,
			"vesting",    // vestingtypes.StoreKey,
		)
		for key, value := range evmosKeys {
			keys[key] = value
		}
	} else if app == "gravitybridge" {
		gravitybridgeKeys := types.NewKVStoreKeys(
			"authz",     // authzkeeper.StoreKey,
			"gravity",   //  gravitytypes.StoreKey,
			"bech32ibc", // bech32ibctypes.StoreKey,
		)
		for key, value := range gravitybridgeKeys {
			keys[key] = value
		}
	} else if app == "sifchain" {
		sifchainKeys := types.NewKVStoreKeys(
			"feegrant",      // feegrant.StoreKey,
			"dispensation",  // disptypes.StoreKey,
			"ethbridge",     // ethbridgetypes.StoreKey,
			"clp",           // clptypes.StoreKey,
			"oracle",        // oracletypes.StoreKey,
			"tokenregistry", // tokenregistrytypes.StoreKey,
			"admin",         // admintypes.StoreKey,
		)
		for key, value := range sifchainKeys {
			keys[key] = value
		}
	} else if app == "starname" {
		starnameKeys := types.NewKVStoreKeys(
			"wasm",          // wasm.StoreKey,
			"configuration", // configuration.StoreKey,
			"starname",      // starname.DomainStoreKey,
		)
		for key, value := range starnameKeys {
			keys[key] = value
		}
	} else if app == "regen" {
		regenKeys := types.NewKVStoreKeys(
			"feegrant", // feegrant.StoreKey,
			"authz",    // uthzkeeper.StoreKey,
		)
		for key, value := range regenKeys {
			keys[key] = value
		}
	} else if app == "akash" {
		akashKeys := types.NewKVStoreKeys(
			"authz",      // authzkeeper.StoreKey,
			"escrow",     // escrow.StoreKey,
			"deployment", // deployment.StoreKey,
			"market",     // market.StoreKey,
			"provider",   // provider.StoreKey,
			"audit",      // audit.StoreKey,
			"cert",       // cert.StoreKey,
			"inflation",  // inflation.StoreKey,
		)
		for key, value := range akashKeys {
			keys[key] = value
		}
	} else if app == "sentinel" {
		sentinelKeys := types.NewKVStoreKeys(
			"authz",        // authzkeeper.StoreKey,
			"distribution", // distributiontypes.StoreKey,
			"feegrant",     // feegrant.StoreKey,
			"custommint",   // customminttypes.StoreKey,
			"swap",         // swaptypes.StoreKey,
			"vpn",          // vpntypes.StoreKey,
		)
		for key, value := range sentinelKeys {
			keys[key] = value
		}
	} else if app == "emoney" {
		emoneyKeys := types.NewKVStoreKeys(
			"liquidityprovider", // lptypes.StoreKey,
			"issuer",            // issuer.StoreKey,
			"authority",         // authority.StoreKey,
			"market",            // market.StoreKey,
			//"market_indices",    // market.StoreKeyIdx,
			"buyback",   // buyback.StoreKey,
			"inflation", // inflation.StoreKey,
		)

		for key, value := range emoneyKeys {
			keys[key] = value
		}
	} else if app == "ixo" {
		ixoKeys := types.NewKVStoreKeys(
			"did",      // didtypes.StoreKey,
			"bonds",    // bondstypes.StoreKey,
			"payments", // paymentstypes.StoreKey,
			"project",  // projecttypes.StoreKey,
		)

		for key, value := range ixoKeys {
			keys[key] = value
		}
	} else if app == "juno" {
		junoKeys := types.NewKVStoreKeys(
			"authz",    // authzkeeper.StoreKey,
			"feegrant", // feegrant.StoreKey,
			"icahost",  // icahosttypes.StoreKey,
			"wasm",     // wasm.StoreKey,
		)

		for key, value := range junoKeys {
			keys[key] = value
		}
	} else if app == "likecoin" {
		likecoinKeys := types.NewKVStoreKeys(
			"feegrant", // feegrant.StoreKey,
			"authz",    // authzkeeper.StoreKey,
			"iscn",     // iscntypes.StoreKey,
		)

		for key, value := range likecoinKeys {
			keys[key] = value
		}
	} else if app == "kichain" {
		kichainKeys := types.NewKVStoreKeys(
			"feegrant", // feegrant.StoreKey,
			"authz",    // authzkeeper.StoreKey,
			"wasm",     // wasm.StoreKey,
		)

		for key, value := range kichainKeys {
			keys[key] = value
		}
	} else if app == "cyber" {
		cyberKeys := types.NewKVStoreKeys(
			"liquidity", // liquiditytypes.StoreKey,
			"feegrant",  // feegrant.StoreKey,
			"authz",     // authzkeeper.StoreKey,
			"bandwidth", // bandwidthtypes.StoreKey,
			"graph",     // graphtypes.StoreKey,
			"rank",      // ranktypes.StoreKey,
			"grid",      // gridtypes.StoreKey,
			"dmn",       // dmntypes.StoreKey,
			"wasm",      // wasm.StoreKey,
		)

		for key, value := range cyberKeys {
			keys[key] = value
		}
	} else if app == "cheqd" {
		cheqdKeys := types.NewKVStoreKeys(
			"feegrant", // feegrant.StoreKey,
			"authz",    // authzkeeper.StoreKey,
			"cheqd",    // cheqdtypes.StoreKey,
		)

		for key, value := range cheqdKeys {
			keys[key] = value
		}
	} else if app == "stargaze" {
		stargazeKeys := types.NewKVStoreKeys(
			"feegrant", // feegrant.StoreKey,
			"claim",    // claimmoduletypes.StoreKey,
			"alloc",    // allocmoduletypes.StoreKey,
			"authz",    // authzkeeper.StoreKey,
			"wasm",     // wasm.StoreKey,
		)

		for key, value := range stargazeKeys {
			keys[key] = value
		}
	} else if app == "bandchain" {
		bandchainKeys := types.NewKVStoreKeys(
			"feegrant", // feegrant.StoreKey,
			"authz",    // authzkeeper.StoreKey,
			"oracle",   // oracletypes.StoreKey,
		)

		for key, value := range bandchainKeys {
			keys[key] = value
		}
	} else if app == "chihuahua" {
		chihuahuaKeys := types.NewKVStoreKeys(
			"feegrant", // feegrant.StoreKey,
			"wasm",     // wasm.StoreKey,
		)

		for key, value := range chihuahuaKeys {
			keys[key] = value
		}
	} else if app == "bitcanna" {
		bitcannaKeys := types.NewKVStoreKeys(
			"feegrant", // feegrant.StoreKey,
			"bcna",     // bcnamoduletypes.StoreKey,
		)

		for key, value := range bitcannaKeys {
			keys[key] = value
		}
	} else if app == "konstellation" {
		konstellationKeys := types.NewKVStoreKeys(
			"oracle", // racletypes.StoreKey,
			"wasm",   // wasm.StoreKey,
		)

		for key, value := range konstellationKeys {
			keys[key] = value
		}
	} else if app == "omniflixhub" {
		omniflixhubKeys := types.NewKVStoreKeys(
			"feegrant",    // feegrant.StoreKey,
			"authz",       // authzkeeper.StoreKey,
			"alloc",       // alloctypes.StoreKey,
			"onft",        // onfttypes.StoreKey,
			"marketplace", // marketplacetypes.StoreKey,
		)

		for key, value := range omniflixhubKeys {
			keys[key] = value
		}
	} else if app == "vidulum" {
		vidulumKeys := types.NewKVStoreKeys(
			"feegrant", // feegrant.StoreKey,
			"vidulum",  // vidulummoduletypes.StoreKey,
		)

		for key, value := range vidulumKeys {
			keys[key] = value
		}
	} else if app == "provenance" {
		provenanceKeys := types.NewKVStoreKeys(
			"feegrant",  // feegrant.StoreKey,
			"authz",     // authzkeeper.StoreKey,
			"metadata",  // metadatatypes.StoreKey,
			"marker",    // markertypes.StoreKey,
			"attribute", // attributetypes.StoreKey,
			"name",      // nametypes.StoreKey,
			"msgfees",   // msgfeestypes.StoreKey,
			"wasm",      // wasm.StoreKey,
		)

		for key, value := range provenanceKeys {
			keys[key] = value
		}
	} else if app == "dig" {
		digKeys := types.NewKVStoreKeys(
			"feegrant", // feegrant.StoreKey,
			"authz",    // authzkeeper.StoreKey,
			"icahost",  // icaHostTypes.StoreKey,
			"wasm",     // wasm.StoreKey,
		)

		for key, value := range digKeys {
			keys[key] = value
		}
	} else if app == "comdex" {
		comdexKeys := types.NewKVStoreKeys(
			"wasm", // wasm.StoreKey,
		)

		for key, value := range comdexKeys {
			keys[key] = value
		}
	} else if app == "cerberus" {
		cerberusKeys := types.NewKVStoreKeys(
			"feegrant", // feegrant.StoreKey,
			"authz",    // authzkeeper.StoreKey,
		)

		for key, value := range cerberusKeys {
			keys[key] = value
		}
	} else if app == "bitsong" {
		bitsongKeys := types.NewKVStoreKeys(
			"feegrant",               // feegrant.StoreKey,
			"authz",                  // authzkeeper.StoreKey,
			"packetfowardmiddleware", // routertypes.StoreKey,
			"fantoken",               // fantokentypes.StoreKey,
			"merkledrop",             // merkledroptypes.StoreKey,
		)

		for key, value := range bitsongKeys {
			keys[key] = value
		}
	} else if app == "assetmantle" {
		assetmantleKeys := types.NewKVStoreKeys(
			"feegrant",               // feegrant.StoreKey,
			"authz",                  // authzKeeper.StoreKey,
			"packetfowardmiddleware", // routerTypes.StoreKey,
			"icahost",                // icaHostTypes.StoreKey,
		)

		for key, value := range assetmantleKeys {
			keys[key] = value
		}
	} else if app == "fetchhub" {
		fetchhubKeys := types.NewKVStoreKeys(
			"feegrant", // feegrant.StoreKey,
			"wasm",     // wasm.StoreKey,
			"authz",    // authzkeeper.StoreKey,
		)

		for key, value := range fetchhubKeys {
			keys[key] = value
		}
	} else if app == "persistent" {
		persistentKeys := types.NewKVStoreKeys(
			"halving",  // halving.StoreKey,
			"authz",    // sdkAuthzKeeper.StoreKey,
			"feegrant", // feegrant.StoreKey,
		)

		for key, value := range persistentKeys {
			keys[key] = value
		}
	} else if app == "cryptoorgchain" {
		cryptoorgchainKeys := types.NewKVStoreKeys(
			"feegrant",  // feegrant.StoreKey,
			"authz",     // authzkeeper.StoreKey,
			"chainmain", // chainmaintypes.StoreKey,
			"supply",    // supplytypes.StoreKey,
			"nft",       // nfttypes.StoreKey,
		)

		for key, value := range cryptoorgchainKeys {
			keys[key] = value
		}
	} else if app == "irisnet" {
		irisnetKeys := types.NewKVStoreKeys(
			"guardian", // guardiantypes.StoreKey,
			"token",    // tokentypes.StoreKey,
			"nft",      // nfttypes.StoreKey,
			"htlc",     // htlctypes.StoreKey,
			"record",   // recordtypes.StoreKey,
			"coinswap", // coinswaptypes.StoreKey,
			"service",  // servicetypes.StoreKey,
			"oracle",   // oracletypes.StoreKey,
			"random",   // randomtypes.StoreKey,
			"farm",     // farmtypes.StoreKey,
			"feegrant", // feegrant.StoreKey,
			"tibc",     // tibchost.StoreKey,
			"NFT",      // tibcnfttypes.StoreKey,
			"MT",       // tibcmttypes.StoreKey,
			"mt",       // mttypes.StoreKey,
		)

		for key, value := range irisnetKeys {
			keys[key] = value
		}
	} else if app == "axelar" {
		axelarKeys := types.NewKVStoreKeys(
			"feegrant",   // feegrant.StoreKey,
			"vote",       // voteTypes.StoreKey,
			"evm",        // evmTypes.StoreKey,
			"snapshot",   // snapTypes.StoreKey,
			"tss",        // tssTypes.StoreKey,
			"nexus",      // nexusTypes.StoreKey,
			"axelarnet",  // axelarnetTypes.StoreKey,
			"reward",     // rewardTypes.StoreKey,
			"permission", // permissionTypes.StoreKey,
		)

		for key, value := range axelarKeys {
			keys[key] = value
		}
	} else if app == "umee" {
		umeeKeys := types.NewKVStoreKeys(
			"feegrant", // feegrant.StoreKey,
			"authz",    // authzkeeper.StoreKey,
			"gravity",  // gravitytypes.StoreKey,
		)

		for key, value := range umeeKeys {
			keys[key] = value
		}
	} else if app == "sei" {
		seieys := types.NewKVStoreKeys(
			"feegrant", // feegrant.StoreKey,
			"oracle",   // oracletypes.StoreKey,
			"wasm",     // wasm.StoreKey,
			"dex",      // dexmoduletypes.StoreKey,
			"epoch",    // epochmoduletypes.StoreKey,
		)

		for key, value := range seieys {
			keys[key] = value
		}
	} else if app == "injective" {
		injectiveKeys := types.NewKVStoreKeys(
			"feegrant",  // feegrant.StoreKey,
			"authz",     // authzkeeper.StoreKey,
			"icahost",   // icahosttypes.StoreKey,
			"exchange",  // exchangetypes.StoreKey,
			"oracle",    // oracletypes.StoreKey,
			"insurance", //insurancetypes.StoreKey,
			"peggy",     // peggytypes.StoreKey,
			"auction",   // auctiontypes.StoreKey,
			"chainlink", // ocrtypes.StoreKey,
			"wasm",      // wasm.StoreKey,
			"xwasm",     // wasmxtypes.StoreKey,
		)

		for key, value := range injectiveKeys {
			keys[key] = value
		}
	} else if app == "kujira" {
		kujiraKeys := types.NewKVStoreKeys(
			"authz",         // authz.ModuleName,
			"feegrant",      // feegrant.StoreKey,
			"wasm",          // wasm.StoreKey,
			"denom",         // denomtypes.StoreKey,
			"icahost",       // icahosttypes.StoreKey,
			"icacontroller", // icacontrollertypes.StoreKey,
			"intertx",       // intertxtypes.StoreKey,
			"scheduler",     // schedulertypes.StoreKey,
			"oracle",        // oracletypes.StoreKey,
		)

		for key, value := range kujiraKeys {
			keys[key] = value
		}
	} else if app == "passage" {
		passageKeys := types.NewKVStoreKeys(
			"feegrant", // feegrant.StoreKey,
			"authz",    // authzkeeper.StoreKey,
			"claim",    // claimtypes.StoreKey,
		)

		for key, value := range passageKeys {
			keys[key] = value
		}
	} else if app == "terra2" {
		terra2Keys := types.NewKVStoreKeys(
			"authz",                  // authzkeeper.StoreKey,
			"feegrant",               // feegrant.StoreKey,
			"icahost",                // icahosttypes.StoreKey,
			"packetfowardmiddleware", // routertypes.StoreKey,
			"wasm",                   //wasm.StoreKey,
		)

		for key, value := range terra2Keys {
			keys[key] = value
		}
	} else if app == "mars" {
		marsKeys := types.NewKVStoreKeys(
			"authz",        // authzkeeper.StoreKey,
			"distribution", // distrtypes.StoreKey,
			"feegrant",     // feegrant.StoreKey,
			"wasm",         // wasm.StoreKey,
			"incentives",   //incentivestypes.StoreKey,
		)

		for key, value := range marsKeys {
			keys[key] = value
		}
	} else if app == "stride" {
		strideKeys := types.NewKVStoreKeys(
			"feegrant",        // feegrant.StoreKey,
			"stakeibc",        // stakeibcmoduletypes.StoreKey,
			"epochs",          // epochsmoduletypes.StoreKey,
			"interchainquery", // interchainquerytypes.StoreKey,
			"icacontroller",   // icacontrollertypes.StoreKey,
			"icahost",         // icahosttypes.StoreKey,
			"records",         // recordsmoduletypes.StoreKey,
			"icacallbacks",    // icacallbacksmoduletypes.StoreKey,
			"claim",           //claimtypes.StoreKey,
		)

		for key, value := range strideKeys {
			keys[key] = value
		}
	}

	// TODO: cleanup app state
	appStore := rootmulti.NewStore(appDB)

	if txIdxHeight <= 0 {
		txIdxHeight = appStore.LastCommitID().Version
		fmt.Printf("[pruneAppState] set txIdxHeight=%d\n", txIdxHeight)
	}

	for _, value := range keys {
		appStore.MountStoreWithDB(value, sdk.StoreTypeIAVL, nil)
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
