package owljsonrpc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"

	"github.com/brendoncarroll/stdctx/logctx"
	"github.com/owlmessenger/owl/pkg/owl"
	"github.com/sourcegraph/jsonrpc2"
)

// ServeRWC serves requests over an io.ReadWriteCloser rwc, using that Owl API o.
func ServeRWC(ctx context.Context, rwc io.ReadWriteCloser, o owl.API) error {
	objStream := jsonrpc2.NewBufferedStream(rwc, jsonrpc2.PlainObjectCodec{})
	h := handler{owl: o}
	c := jsonrpc2.NewConn(ctx, objStream, jsonrpc2.AsyncHandler(h)) // TODO: async handler
	defer c.Close()
	<-c.DisconnectNotify()
	return nil
}

type handler struct {
	owl owl.API
}

func (h handler) Handle(ctx context.Context, c *jsonrpc2.Conn, r *jsonrpc2.Request) {
	y, err := Call(ctx, h.owl, r.Method, *r.Params)
	if err != nil {
		if err := c.ReplyWithError(ctx, r.ID, &jsonrpc2.Error{
			Code:    jsonrpc2.CodeInternalError,
			Message: err.Error(),
		}); err != nil {
			logctx.Errorf(ctx, "while replying with error: %v", err)
			return
		}
		return
	}
	if err := c.Reply(ctx, r.ID, y); err != nil {
		logctx.Errorf(ctx, "while replying: %v", err)
		return
	}
}

// Call looks for a method on target, and then unmarshals reqData into something that looks like it's inputa
//
// Valid method signatures:
// - (a *A) func(ctx context.Context) error
// - (a *A) func(ctx context.Context, req *X) error
// - (a *A) func(ctx context.Context) (*Y, error)
// - (a *A) func(ctx context.Context, req *X) (*Y, error)
//
// - The method can have 1 or 2 output values, and the last one must be an error
// - The method can have 2 or 3 input values--including the receiver.
// - The first input is the receiver, the second is a context.
func Call(ctx context.Context, target interface{}, method string, reqData json.RawMessage) (interface{}, error) {
	ty := reflect.TypeOf(target)
	m, found := ty.MethodByName(method)
	if !found {
		return nil, fmt.Errorf("no method: %q", method)
	}
	// Inputs
	if numIn := m.Type.NumIn(); numIn > 3 {
		return nil, fmt.Errorf("method %q has too many arguments", method)
	} else if numIn == 3 && m.Type.In(2).Kind() != reflect.Pointer {
		return nil, errors.New("input 2 must be pointer ")
	}
	var req reflect.Value
	if m.Type.NumIn() > 2 {
		req = reflect.New(m.Type.In(2).Elem())
		if err := json.Unmarshal(reqData, req.Interface()); err != nil {
			return nil, err
		}
	}

	// Outputs
	if numOut := m.Type.NumOut(); numOut > 2 || numOut == 0 {
		return nil, errors.New("must have 1 or 2 outputs")
	}

	// Prepare for call
	args := []reflect.Value{reflect.ValueOf(target), reflect.ValueOf(ctx)}
	if m.Type.NumIn() > 2 {
		args = append(args, req)
	}
	outs := m.Func.Call(args)
	if len(outs) == 1 {
		return struct{}{}, errorFromAny(outs[0].Interface())
	}
	return outs[0].Interface(), errorFromAny(outs[1].Interface())
}

func errorFromAny(x any) error {
	if x == nil {
		return nil
	}
	return x.(error)
}
