package evm

import (
	sdk "github.com/cosmos/cosmos-sdk/types"
	sdkerrors "github.com/cosmos/cosmos-sdk/types/errors"
	"github.com/ethereum/go-ethereum/common"
	ethtypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/hook"
	apprpctypes "github.com/okex/exchain/app/rpc/types"
	ethermint "github.com/okex/exchain/app/types"
	"github.com/okex/exchain/x/common/perf"
	"github.com/okex/exchain/x/evm/types"
	evmtypes "github.com/okex/exchain/x/evm/types"
	"github.com/okex/exchain/x/evm/watcher"
	"math/big"

	tmtypes "github.com/tendermint/tendermint/types"
)

// NewHandler returns a handler for Ethermint type messages.
func NewHandler(k *Keeper) sdk.Handler {
	return func(ctx sdk.Context, msg sdk.Msg) (result *sdk.Result, err error) {
		ctx = ctx.WithEventManager(sdk.NewEventManager())

		var handlerFun func() (*sdk.Result, error)
		var name string
		switch msg := msg.(type) {
		case types.MsgEthereumTx:
			name = "handleMsgEthereumTx"
			handlerFun = func() (*sdk.Result, error) {
				return handleMsgEthereumTx(ctx, k, msg)
			}
		case types.MsgEthermint:
			name = "handleMsgEthermint"
			handlerFun = func() (*sdk.Result, error) {
				return handleMsgEthermint(ctx, k, msg)
			}
		default:
			return nil, sdkerrors.Wrapf(sdkerrors.ErrUnknownRequest, "unrecognized %s message type: %T", ModuleName, msg)
		}

		seq := perf.GetPerf().OnDeliverTxEnter(ctx, types.ModuleName, name)
		defer perf.GetPerf().OnDeliverTxExit(ctx, types.ModuleName, name, seq)

		result, err = handlerFun()
		if err != nil {
			err = sdkerrors.New(types.ModuleName, types.CodeSpaceEvmCallFailed, err.Error())
		}

		return result, err
	}
}

// handleMsgEthereumTx handles an Ethereum specific tx
func handleMsgEthereumTx(ctx sdk.Context, k *Keeper, msg types.MsgEthereumTx) (*sdk.Result, error) {
	// parse the chainID from a string to a base-10 integer
	chainIDEpoch, err := ethermint.ParseChainID(ctx.ChainID())
	if err != nil {
		return nil, err
	}

	// Verify signature and retrieve sender address
	sender, err := msg.VerifySig(chainIDEpoch)
	if err != nil {
		return nil, err
	}

	txHash := tmtypes.Tx(ctx.TxBytes()).Hash()
	ethHash := common.BytesToHash(txHash)

	st := types.StateTransition{
		AccountNonce: msg.Data.AccountNonce,
		Price:        msg.Data.Price,
		GasLimit:     msg.Data.GasLimit,
		Recipient:    msg.Data.Recipient,
		Amount:       msg.Data.Amount,
		Payload:      msg.Data.Payload,
		Csdb:         types.CreateEmptyCommitStateDB(k.GenerateCSDBParams(), ctx),
		ChainID:      chainIDEpoch,
		TxHash:       &ethHash,
		Sender:       sender,
		Simulate:     ctx.IsCheckTx(),
	}
	// HOOK: prepare tx
	blockHash := st.Csdb.BlockHash()
	blockNumber := uint64(ctx.BlockHeight())
	txIndex := st.Csdb.TxIndex()
	transaction, _ := apprpctypes.NewEthTransaction(&msg, ethHash, blockHash, blockNumber, uint64(txIndex))
	hook.GlobalHook.PrepareTx(sender, transaction)

	// since the txCount is used by the stateDB, and a simulated tx is run only on the node it's submitted to,
	// then this will cause the txCount/stateDB of the node that ran the simulated tx to be different than the
	// other nodes, causing a consensus error
	if !st.Simulate {
		k.Watcher.SaveEthereumTx(msg, common.BytesToHash(txHash), uint64(k.TxCount))
		// Prepare db for logs
		st.Csdb.Prepare(ethHash, k.Bhash, k.TxCount)
		st.Csdb.SetLogSize(k.LogSize)
		k.TxCount++
	}

	config, found := k.GetChainConfig(ctx)
	if !found {
		return nil, types.ErrChainConfigNotFound
	}

	executionResult, resultData, err := st.TransitionDb(ctx, config)
	// HOOK: prepare tx
	var cumulativeGasUsed uint64
	var logs []*ethtypes.Log
	if executionResult != nil {
		cumulativeGasUsed = executionResult.GasInfo.GasConsumed
		logs = executionResult.Logs
	}
	var status uint64 = 1
	if err != nil || executionResult == nil {
		status = 0
	}
	data, _ := evmtypes.DecodeResultData(msg.Data.Payload)
	hook.GlobalHook.HandleReceipt(ethHash.String(), &ethtypes.Receipt{
		CumulativeGasUsed: cumulativeGasUsed,
		Status:            status,
		Bloom:             data.Bloom,
		ContractAddress:   data.ContractAddress,
		Logs:              logs,
		TxHash:            ethHash,
		GasUsed:           ctx.GasMeter().GasConsumed(),
		BlockHash:         blockHash,
		BlockNumber:       big.NewInt(int64(blockNumber)),
		TransactionIndex:  uint(txIndex),
	})
	if err != nil {
		if !st.Simulate {
			k.Watcher.SaveTransactionReceipt(watcher.TransactionFailed, msg, common.BytesToHash(txHash), uint64(k.TxCount-1), &types.ResultData{}, ctx.GasMeter().GasConsumed())
		}
		return nil, err
	}

	if !st.Simulate {
		// update block bloom filter
		k.Bloom.Or(k.Bloom, executionResult.Bloom)
		k.LogSize = st.Csdb.GetLogSize()
		k.Watcher.SaveTransactionReceipt(watcher.TransactionSuccess, msg, common.BytesToHash(txHash), uint64(k.TxCount-1), resultData, ctx.GasMeter().GasConsumed())
		if msg.Data.Recipient == nil {
			k.Watcher.SaveContractCode(resultData.ContractAddress, msg.Data.Payload)
		}
	}

	// log successful execution
	k.Logger(ctx).Info(executionResult.Result.Log)

	ctx.EventManager().EmitEvents(sdk.Events{
		sdk.NewEvent(
			types.EventTypeEthereumTx,
			sdk.NewAttribute(sdk.AttributeKeyAmount, msg.Data.Amount.String()),
		),
		sdk.NewEvent(
			sdk.EventTypeMessage,
			sdk.NewAttribute(sdk.AttributeKeyModule, types.AttributeValueCategory),
			sdk.NewAttribute(sdk.AttributeKeySender, sender.String()),
		),
	})

	if msg.Data.Recipient != nil {
		ctx.EventManager().EmitEvent(
			sdk.NewEvent(
				types.EventTypeEthereumTx,
				sdk.NewAttribute(types.AttributeKeyRecipient, msg.Data.Recipient.String()),
			),
		)
	}

	// set the events to the result
	executionResult.Result.Events = ctx.EventManager().Events()
	return executionResult.Result, nil
}

// handleMsgEthermint handles an sdk.StdTx for an Ethereum state transition
func handleMsgEthermint(ctx sdk.Context, k *Keeper, msg types.MsgEthermint) (*sdk.Result, error) {

	if !ctx.IsCheckTx() && !ctx.IsReCheckTx() {
		return nil, sdkerrors.Wrap(ethermint.ErrInvalidMsgType, "Ethermint type message is not allowed.")
	}

	// parse the chainID from a string to a base-10 integer
	chainIDEpoch, err := ethermint.ParseChainID(ctx.ChainID())
	if err != nil {
		return nil, err
	}

	txHash := tmtypes.Tx(ctx.TxBytes()).Hash()
	ethHash := common.BytesToHash(txHash)

	st := types.StateTransition{
		AccountNonce: msg.AccountNonce,
		Price:        msg.Price.BigInt(),
		GasLimit:     msg.GasLimit,
		Amount:       msg.Amount.BigInt(),
		Payload:      msg.Payload,
		Csdb:         types.CreateEmptyCommitStateDB(k.GenerateCSDBParams(), ctx),
		ChainID:      chainIDEpoch,
		TxHash:       &ethHash,
		Sender:       common.BytesToAddress(msg.From.Bytes()),
		Simulate:     ctx.IsCheckTx(),
	}
	// HOOK: prepare tx
	blockHash := st.Csdb.BlockHash()
	blockNumber := uint64(ctx.BlockHeight())
	txIndex := st.Csdb.TxIndex()
	transaction, _ := apprpctypes.NewEthTransactionFromEthermint(&msg, ethHash, blockHash, blockNumber, uint64(txIndex))
	hook.GlobalHook.PrepareTx(common.BytesToAddress(msg.From.Bytes()), transaction)

	if msg.Recipient != nil {
		to := common.BytesToAddress(msg.Recipient.Bytes())
		st.Recipient = &to
	}

	if !st.Simulate {
		// Prepare db for logs
		st.Csdb.Prepare(ethHash, k.Bhash, k.TxCount)
		st.Csdb.SetLogSize(k.LogSize)
		k.TxCount++
	}

	config, found := k.GetChainConfig(ctx)
	if !found {
		return nil, types.ErrChainConfigNotFound
	}

	executionResult, _, err := st.TransitionDb(ctx, config)
	var cumulativeGasUsed uint64
	var logs []*ethtypes.Log
	if executionResult != nil {
		cumulativeGasUsed = executionResult.GasInfo.GasConsumed
		logs = executionResult.Logs
	}
	var status uint64 = 1
	if err != nil || executionResult == nil {
		status = 0
	}
	data, _ := evmtypes.DecodeResultData(msg.Payload)
	hook.GlobalHook.HandleReceipt(ethHash.String(), &ethtypes.Receipt{
		CumulativeGasUsed: cumulativeGasUsed,
		Status:            status,
		Bloom:             data.Bloom,
		ContractAddress:   data.ContractAddress,
		Logs:              logs,
		TxHash:            ethHash,
		GasUsed:           ctx.GasMeter().GasConsumed(),
		BlockHash:         blockHash,
		BlockNumber:       big.NewInt(int64(blockNumber)),
		TransactionIndex:  uint(txIndex),
	})
	if err != nil {
		return nil, err
	}

	// update block bloom filter
	if !st.Simulate {
		k.Bloom.Or(k.Bloom, executionResult.Bloom)
		k.LogSize = st.Csdb.GetLogSize()
	}

	// log successful execution
	k.Logger(ctx).Info(executionResult.Result.Log)

	ctx.EventManager().EmitEvents(sdk.Events{
		sdk.NewEvent(
			types.EventTypeEthermint,
			sdk.NewAttribute(sdk.AttributeKeyAmount, msg.Amount.String()),
		),
		sdk.NewEvent(
			sdk.EventTypeMessage,
			sdk.NewAttribute(sdk.AttributeKeyModule, types.AttributeValueCategory),
			sdk.NewAttribute(sdk.AttributeKeySender, msg.From.String()),
		),
	})

	if msg.Recipient != nil {
		ctx.EventManager().EmitEvent(
			sdk.NewEvent(
				types.EventTypeEthermint,
				sdk.NewAttribute(types.AttributeKeyRecipient, msg.Recipient.String()),
			),
		)
	}

	// set the events to the result
	executionResult.Result.Events = ctx.EventManager().Events()
	return executionResult.Result, nil
}
