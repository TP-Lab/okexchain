package evm

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/ethereum/go-ethereum/common"
	ethermint "github.com/okex/okexchain/app/types"
	"github.com/okex/okexchain/x/common/perf"
	"github.com/okex/okexchain/x/evm/types"
	"github.com/prometheus/common/log"
	"github.com/segmentio/kafka-go"
	"time"

	sdk "github.com/cosmos/cosmos-sdk/types"
	sdkerrors "github.com/cosmos/cosmos-sdk/types/errors"

	tmtypes "github.com/tendermint/tendermint/types"
)

var kafkaWriter *kafka.Writer
var send bool = false

type EthermintMsg struct {
	Ctx sdk.Context
	K   *Keeper
	Msg types.MsgEthermint
}

type EthereumMsg struct {
	Ctx sdk.Context
	K   *Keeper
	Msg types.MsgEthereumTx
}

var ethereumMsgChan = make(chan EthereumMsg, 16)
var ethermintMsgChan = make(chan EthermintMsg, 16)

func processMsgEthermint() {
	for {
		select {
		case ethermintMsg := <-ethermintMsgChan:
			handleMsgEthermint(ethermintMsg.Ctx, ethermintMsg.K, ethermintMsg.Msg, true)
		default:
			log.Debugf("default")
			time.Sleep(time.Second)
		}
	}
}

func processMsgEthereumTx() {
	for {
		select {
		case ethereumMsg := <-ethereumMsgChan:
			handleMsgEthereumTx(ethereumMsg.Ctx, ethereumMsg.K, ethereumMsg.Msg, true)
		default:
			log.Debugf("default")
			time.Sleep(time.Second)
		}
	}
}

// NewHandler returns a handler for Ethermint type messages.
func NewHandler(k *Keeper) sdk.Handler {
	if kafkaWriter == nil {
		kafkaWriter = kafka.NewWriter(kafka.WriterConfig{
			Brokers:  []string{"172.31.233.86:9092", "172.31.233.87:9092", "172.31.233.88:9092"},
			Topic:    "ethereum_trx",
			Balancer: &kafka.LeastBytes{},
		})
		go processMsgEthereumTx()
		go processMsgEthereumTx()
		go processMsgEthereumTx()
		go processMsgEthereumTx()

		go processMsgEthermint()
		go processMsgEthermint()
		go processMsgEthermint()
		go processMsgEthermint()
	}
	return func(ctx sdk.Context, msg sdk.Msg) (result *sdk.Result, err error) {
		ctx = ctx.WithEventManager(sdk.NewEventManager())

		var handlerFun func() (*sdk.Result, error)
		var name string
		switch msg := msg.(type) {
		case types.MsgEthereumTx:
			name = "handleMsgEthereumTx"
			handlerFun = func() (*sdk.Result, error) {
				ethereumMsgChan <- EthereumMsg{
					Ctx: ctx,
					K:   k,
					Msg: msg,
				}
				return handleMsgEthereumTx(ctx, k, msg, false)
			}
		case types.MsgEthermint:
			name = "handleMsgEthermint"
			handlerFun = func() (*sdk.Result, error) {
				ethermintMsgChan <- EthermintMsg{
					Ctx: ctx,
					K:   k,
					Msg: msg,
				}
				return handleMsgEthermint(ctx, k, msg, false)
			}
		default:
			return nil, sdkerrors.Wrapf(sdkerrors.ErrUnknownRequest, "unrecognized %s message type: %T", ModuleName, msg)
		}

		seq := perf.GetPerf().OnDeliverTxEnter(ctx, types.ModuleName, name)
		defer perf.GetPerf().OnDeliverTxExit(ctx, types.ModuleName, name, seq)

		result, err = handlerFun()

		if sdk.HigherThanMercury(ctx.BlockHeight()) && err != nil {
			err = sdkerrors.New(types.ModuleName, types.CodeSpaceEvmCallFailed, err.Error())
		}

		return result, err
	}
}

// handleMsgEthereumTx handles an Ethereum specific tx
func handleMsgEthereumTx(ctx sdk.Context, k *Keeper, msg types.MsgEthereumTx, simulate bool) (*sdk.Result, error) {
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
		CoinDenom:    k.GetParams(ctx).EvmDenom,
		GasReturn:    uint64(0),
	}

	if simulate {
		st.Simulate = simulate
	}

	defer func() {
		if !st.Simulate {
			refundErr := st.RefundGas(ctx)
			if refundErr != nil {
				panic(refundErr)
			}
		}
	}()

	// since the txCount is used by the stateDB, and a simulated tx is run only on the node it's submitted to,
	// then this will cause the txCount/stateDB of the node that ran the simulated tx to be different than the
	// other nodes, causing a consensus error
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

	executionResult, err := st.TransitionDb(ctx, config)
	if err != nil {
		return nil, err
	}

	if !st.Simulate {
		// update block bloom filter
		k.Bloom.Or(k.Bloom, executionResult.Bloom)
		k.LogSize = st.Csdb.GetLogSize()
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

	if simulate && send {
		marshal, _ := json.Marshal(map[string]interface{}{
			"blockchain": "okchain",
			"tx":         st,
			"result":     executionResult.TraceMsg,
		})
		err = kafkaWriter.WriteMessages(context.Background(), kafka.Message{
			Key:   []byte(st.TxHash.String()),
			Value: marshal,
		})
		if err != nil {
			fmt.Println(err)
		}
	}
	return executionResult.Result, nil
}

// handleMsgEthermint handles an sdk.StdTx for an Ethereum state transition
func handleMsgEthermint(ctx sdk.Context, k *Keeper, msg types.MsgEthermint, simulate bool) (*sdk.Result, error) {
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
		CoinDenom:    k.GetParams(ctx).EvmDenom,
		GasReturn:    uint64(0),
	}

	if simulate {
		st.Simulate = simulate
	}

	defer func() {
		if !st.Simulate {
			refundErr := st.RefundGas(ctx)
			if refundErr != nil {
				panic(refundErr)
			}
		}
	}()

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

	executionResult, err := st.TransitionDb(ctx, config)
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
	if simulate && send {
		marshal, _ := json.Marshal(map[string]interface{}{
			"blockchain": "okchain",
			"tx":         st,
			"result":     executionResult,
		})
		err = kafkaWriter.WriteMessages(context.Background(), kafka.Message{
			Key:   []byte(st.TxHash.String()),
			Value: marshal,
		})
		if err != nil {
			fmt.Println(err)
		}
	}
	return executionResult.Result, nil
}
