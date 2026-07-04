package bridge

/*
#cgo LDFLAGS: -L../../runtime/target/release -lflowrulz_core -ldl
#include <stdlib.h>
#include <stdint.h>

typedef int (*caller_cb_t)(uint64_t, uint16_t, const unsigned char*, size_t, unsigned char*, size_t*);

int flowrulz_execute(
    uint64_t ctx_id,
    const unsigned char* plan_ptr, size_t plan_len,
    const unsigned char* body_ptr, size_t body_len,
    caller_cb_t caller_cb,
    unsigned char* out_ptr, size_t out_cap, size_t* out_len,
    unsigned char* err_ptr, size_t err_cap, size_t* err_len,
    const unsigned char* msg_id_ptr, size_t msg_id_len,
    const unsigned char* corr_id_ptr, size_t corr_id_len,
    const unsigned char* trace_id_ptr, size_t trace_id_len,
    uint32_t partition, int64_t offset
);

int flowrulz_init_context(
    const unsigned char* body_ptr, size_t body_len,
    unsigned char* out_ptr, size_t out_cap, size_t* out_len,
    unsigned char* err_ptr, size_t err_cap, size_t* err_len
);

int flowrulz_execute_step(
    uint64_t ctx_id,
    const unsigned char* plan_ptr, size_t plan_len,
    const unsigned char* ctx_bytes_ptr, size_t ctx_bytes_len,
    const unsigned char* resp_ptr, size_t resp_len,
    caller_cb_t caller_cb,
    unsigned char* out_ptr, size_t out_cap, size_t* out_len,
    unsigned char* err_ptr, size_t err_cap, size_t* err_len,
    uint16_t* pending_svc_id,
    unsigned char* pending_body_ptr, size_t pending_body_cap, size_t* pending_body_len,
    uint64_t* pending_timeout_ms,
    unsigned char* ctx_out_ptr, size_t ctx_out_cap, size_t* ctx_out_len
);

caller_cb_t getCallerBridgePtr(void);
*/
import "C"

import (
	"encoding/binary"
	"fmt"
	"unsafe"
)

func InitContext(body []byte) ([]byte, error) {
	outBuf := *outputBufPool.Get().(*[]byte)
	defer outputBufPool.Put(&outBuf)
	var outLen C.size_t
	errBuf := make([]byte, 4096)
	var errLen C.size_t

	var bodyPtr *C.uchar
	if len(body) > 0 {
		bodyPtr = (*C.uchar)(unsafe.Pointer(&body[0]))
	}

	rc := C.flowrulz_init_context(
		bodyPtr, C.size_t(len(body)),
		(*C.uchar)(unsafe.Pointer(&outBuf[0])), C.size_t(cap(outBuf)), &outLen,
		(*C.uchar)(unsafe.Pointer(&errBuf[0])), C.size_t(cap(errBuf)), &errLen,
	)
	if rc != 0 {
		return nil, fmt.Errorf("init context failed: %s", string(errBuf[:errLen]))
	}
	out := make([]byte, outLen)
	copy(out, outBuf[:outLen])
	return out, nil
}

func Execute(plan []byte, body []byte, caller ServiceCaller, ctx *ExecContext) ([]byte, error) {
	if len(plan) == 0 {
		return nil, fmt.Errorf("execute: empty plan")
	}

	ctxID := nextExecID.Add(1)
	if caller != nil {
		callerMap.Store(ctxID, caller)
		defer callerMap.Delete(ctxID)
	}

	var msgIdPtr *C.uchar
	var msgIdLen C.size_t
	var corrIdPtr *C.uchar
	var corrIdLen C.size_t
	var traceIdPtr *C.uchar
	var traceIdLen C.size_t
	var partition C.uint32_t
	var offset C.int64_t

	if ctx != nil {
		if len(ctx.MessageID) > 0 {
			b := []byte(ctx.MessageID)
			msgIdPtr = (*C.uchar)(unsafe.Pointer(&b[0]))
			msgIdLen = C.size_t(len(b))
		}
		if len(ctx.CorrelationID) > 0 {
			b := []byte(ctx.CorrelationID)
			corrIdPtr = (*C.uchar)(unsafe.Pointer(&b[0]))
			corrIdLen = C.size_t(len(b))
		}
		if len(ctx.TraceID) > 0 {
			b := []byte(ctx.TraceID)
			traceIdPtr = (*C.uchar)(unsafe.Pointer(&b[0]))
			traceIdLen = C.size_t(len(b))
		}
		partition = C.uint32_t(ctx.Partition)
		offset = C.int64_t(ctx.Offset)
	}

	outBuf := *outputBufPool.Get().(*[]byte)
	defer outputBufPool.Put(&outBuf)
	var outLen C.size_t
	errBuf := make([]byte, 4096)
	var errLen C.size_t

	var bodyPtr *C.uchar
	if len(body) > 0 {
		bodyPtr = (*C.uchar)(unsafe.Pointer(&body[0]))
	}
	var planPtr *C.uchar
	if len(plan) > 0 {
		planPtr = (*C.uchar)(unsafe.Pointer(&plan[0]))
	}

	cb := C.getCallerBridgePtr()
	rc := C.flowrulz_execute(
		C.uint64_t(ctxID),
		planPtr, C.size_t(len(plan)),
		bodyPtr, C.size_t(len(body)),
		cb,
		(*C.uchar)(unsafe.Pointer(&outBuf[0])), C.size_t(cap(outBuf)), &outLen,
		(*C.uchar)(unsafe.Pointer(&errBuf[0])), C.size_t(cap(errBuf)), &errLen,
		msgIdPtr, msgIdLen,
		corrIdPtr, corrIdLen,
		traceIdPtr, traceIdLen,
		partition, offset,
	)
	if rc != 0 {
		return nil, fmt.Errorf("execute failed: %s", string(errBuf[:errLen]))
	}
	out := make([]byte, outLen)
	copy(out, outBuf[:outLen])
	return out, nil
}

type StepResult int

const (
	StepDone     StepResult = 0
	StepPending  StepResult = 1
	StepContinue StepResult = 2
	StepDelay    StepResult = 3
)

type StepOutput struct {
	Result      StepResult
	Output      []byte
	Error       string
	PendingSvc  uint16
	PendingBody []byte
	TimeoutMs   uint64
	CtxBytes    []byte
}

func ExecuteStep(plan, ctxBytes, respBytes []byte, caller ServiceCaller) (*StepOutput, error) {
	ctxID := nextExecID.Add(1)
	if caller != nil {
		callerMap.Store(ctxID, caller)
		defer callerMap.Delete(ctxID)
	}

	outBuf := *outputBufPool.Get().(*[]byte)
	defer outputBufPool.Put(&outBuf)
	var outLen C.size_t
	errBuf := *outputBufPool.Get().(*[]byte)
	defer outputBufPool.Put(&errBuf)
	var errLen C.size_t
	var pendingSvcID C.uint16_t
	pendingBodyBuf := *outputBufPool.Get().(*[]byte)
	defer outputBufPool.Put(&pendingBodyBuf)
	var pendingBodyLen C.size_t
	var pendingTimeoutMs C.uint64_t
	ctxOutBuf := *outputBufPool.Get().(*[]byte)
	defer outputBufPool.Put(&ctxOutBuf)
	var ctxOutLen C.size_t

	respP, respLen := respBytesPtr(respBytes)
	rc := C.flowrulz_execute_step(
		C.uint64_t(ctxID),
		(*C.uchar)(unsafe.Pointer(&plan[0])), C.size_t(len(plan)),
		ctxBytesPtr(ctxBytes), C.size_t(len(ctxBytes)),
		respP, respLen,
		C.getCallerBridgePtr(),
		(*C.uchar)(unsafe.Pointer(&outBuf[0])), C.size_t(cap(outBuf)), &outLen,
		(*C.uchar)(unsafe.Pointer(&errBuf[0])), C.size_t(cap(errBuf)), &errLen,
		&pendingSvcID,
		(*C.uchar)(unsafe.Pointer(&pendingBodyBuf[0])), C.size_t(cap(pendingBodyBuf)), &pendingBodyLen,
		&pendingTimeoutMs,
		(*C.uchar)(unsafe.Pointer(&ctxOutBuf[0])), C.size_t(cap(ctxOutBuf)), &ctxOutLen,
	)

	out := &StepOutput{
		Result:      StepResult(rc),
		Output:      copyBytes(outBuf, int(outLen)),
		PendingSvc:  uint16(pendingSvcID),
		PendingBody: copyBytes(pendingBodyBuf, int(pendingBodyLen)),
		TimeoutMs:   uint64(pendingTimeoutMs),
		CtxBytes:    copyBytes(ctxOutBuf, int(ctxOutLen)),
	}

	if rc == -8 || rc == -1 {
		out.Error = string(copyBytes(errBuf, int(errLen)))
	}

	return out, nil
}

func (o *StepOutput) DelayMs() uint64 {
	if len(o.PendingBody) < 8 {
		return 0
	}
	return binary.LittleEndian.Uint64(o.PendingBody[:8])
}
