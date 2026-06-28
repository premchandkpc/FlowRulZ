package bridge

/*
#cgo LDFLAGS: -L../../../rust/target/release -lflowrulz_core -ldl
#include <stdlib.h>
#include <stdint.h>

typedef int (*caller_cb_t)(uint64_t, uint16_t, const unsigned char*, size_t, unsigned char*, size_t*);

int flowrulz_compile(
    const unsigned char* dsl_ptr, size_t dsl_len,
    const unsigned char* rule_id_ptr, size_t rule_id_len,
    unsigned char* out_ptr, size_t out_cap, size_t* out_len,
    unsigned char* err_ptr, size_t err_cap, size_t* err_len
);

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

unsigned char* flowrulz_msg_alloc(size_t size);
void flowrulz_msg_release(unsigned char* ptr);
uint16_t flowrulz_intern(const unsigned char* s_ptr, size_t s_len);
void flowrulz_intern_lookup(uint16_t id, unsigned char* out_ptr, size_t* out_len);

size_t flowrulz_get_spans(unsigned char* out_ptr, size_t out_cap);
uint32_t flowrulz_plan_complexity(const unsigned char* plan_ptr, size_t plan_len);

caller_cb_t getCallerBridgePtr(void);
*/
import "C"

import (
	"fmt"
	"sync"
	"sync/atomic"
	"unsafe"
)

type ServiceCaller func(svcID uint16, body []byte) ([]byte, error)

type ExecContext struct {
	MessageID     string
	CorrelationID string
	TraceID       string
	Partition     uint32
	Offset        int64
}

var (
	callerMap sync.Map
	nextExecID atomic.Uint64
)

//export goServiceCaller
func goServiceCaller(ctxID C.uint64_t, svcID C.uint16_t, bodyPtr *C.uchar, bodyLen C.size_t, respPtr *C.uchar, respLen *C.size_t) C.int {
	v, ok := callerMap.Load(uint64(ctxID))
	if !ok {
		return -1
	}
	cb, ok := v.(ServiceCaller)
	if !ok || cb == nil {
		return -1
	}

	body := C.GoBytes(unsafe.Pointer(bodyPtr), C.int(bodyLen))
	resp, err := cb(uint16(svcID), body)
	if err != nil {
		return -1
	}

	if len(resp) > 65536 {
		resp = resp[:65536]
	}
	copy((*[1 << 30]byte)(unsafe.Pointer(respPtr))[:len(resp):len(resp)], resp)
	*respLen = C.size_t(len(resp))
	return 0
}

func Compile(dsl string, ruleID string) ([]byte, error) {
	if len(dsl) == 0 {
		return nil, fmt.Errorf("compile: empty dsl")
	}
	dslBytes := []byte(dsl)
	ridBytes := []byte(ruleID)

	outBuf := make([]byte, 256*1024)
	var outLen C.size_t
	errBuf := make([]byte, 4096)
	var errLen C.size_t

	rc := C.flowrulz_compile(
		(*C.uchar)(unsafe.Pointer(&dslBytes[0])), C.size_t(len(dslBytes)),
		(*C.uchar)(unsafe.Pointer(&ridBytes[0])), C.size_t(len(ridBytes)),
		(*C.uchar)(unsafe.Pointer(&outBuf[0])), C.size_t(cap(outBuf)), &outLen,
		(*C.uchar)(unsafe.Pointer(&errBuf[0])), C.size_t(cap(errBuf)), &errLen,
	)
	if rc != 0 {
		return nil, fmt.Errorf("compile failed: %s", string(errBuf[:errLen]))
	}
	return outBuf[:outLen], nil
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

	outBuf := make([]byte, 256*1024)
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
	return outBuf[:outLen], nil
}

func MsgAlloc(size int) unsafe.Pointer {
	return unsafe.Pointer(C.flowrulz_msg_alloc(C.size_t(size)))
}

func MsgRelease(ptr unsafe.Pointer) {
	C.flowrulz_msg_release((*C.uchar)(ptr))
}

func Intern(s string) uint16 {
	if len(s) == 0 {
		return 0
	}
	b := []byte(s)
	return uint16(C.flowrulz_intern((*C.uchar)(unsafe.Pointer(&b[0])), C.size_t(len(b))))
}

func InternLookup(id uint16) string {
	buf := make([]byte, 256)
	var outLen C.size_t
	C.flowrulz_intern_lookup(C.uint16_t(id), (*C.uchar)(unsafe.Pointer(&buf[0])), &outLen)
	return string(buf[:outLen])
}

func GetSpans() []byte {
	buf := make([]byte, 4096)
	n := C.flowrulz_get_spans((*C.uchar)(unsafe.Pointer(&buf[0])), C.size_t(cap(buf)))
	return buf[:n]
}

func PlanComplexity(plan []byte) uint32 {
	if len(plan) == 0 {
		return 0
	}
	return uint32(C.flowrulz_plan_complexity(
		(*C.uchar)(unsafe.Pointer(&plan[0])), C.size_t(len(plan)),
	))
}
