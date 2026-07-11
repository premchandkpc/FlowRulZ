package bridge

/*
#cgo LDFLAGS: -L../../runtime/target/release -lflowrulz_core -ldl
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

size_t flowrulz_span_size(void);
size_t flowrulz_get_spans(unsigned char* out_ptr, size_t out_cap);

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

int flowrulz_plan_services(const unsigned char* plan_ptr, size_t plan_len, unsigned char* out_ptr, size_t out_cap, size_t* out_len);
uint32_t flowrulz_plan_complexity(const unsigned char* plan_ptr, size_t plan_len);

int flowrulz_register_plugin(const unsigned char* name_ptr, size_t name_len, const unsigned char* wasm_ptr, size_t wasm_len);

caller_cb_t getCallerBridgePtr(void);
*/
import "C"

import (
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"unsafe"
)

var outputBufPool = sync.Pool{
	New: func() any {
		b := make([]byte, 256*1024)
		return &b
	},
}

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
	emptyRespSentinel [1]byte
)

//export goServiceCaller
func goServiceCaller(ctxID C.uint64_t, svcID C.uint16_t, bodyPtr *C.uchar, bodyLen C.size_t, respPtr *C.uchar, respLen *C.size_t) C.int {
	defer func() {
		if r := recover(); r != nil {
			slog.Error("goServiceCaller panic", "recover", r)
		}
	}()

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
		return -1
	}
	copy((*[1 << 30]byte)(unsafe.Pointer(respPtr))[:len(resp):len(resp)], resp)
	*respLen = C.size_t(len(resp))
	return 0
}

func SpanSize() int {
	return int(C.flowrulz_span_size())
}

func GetSpans() []byte {
	buf := make([]byte, 4096)
	n := C.flowrulz_get_spans((*C.uchar)(unsafe.Pointer(&buf[0])), C.size_t(cap(buf)))
	if int(n) > cap(buf) {
		n = C.size_t(cap(buf))
	}
	return buf[:n]
}

func ParseServiceMethod(s string) (service, method string) {
	if idx := strings.IndexByte(s, '.'); idx >= 0 {
		return s[:idx], s[idx+1:]
	}
	return s, ""
}

func ParseCompensation(s string) (service, method, compensator, compMethod string) {
	colonIdx := strings.IndexByte(s, ':')
	if colonIdx < 0 {
		svc, m := ParseServiceMethod(s)
		return svc, m, "", ""
	}

	beforeColon := s[:colonIdx]
	afterColon := s[colonIdx+1:]

	svc, method := ParseServiceMethod(beforeColon)

	if dot := strings.IndexByte(afterColon, '.'); dot >= 0 {
		return svc, method, afterColon[:dot], afterColon[dot+1:]
	}

	return svc, method, svc, afterColon
}

func ctxBytesPtr(b []byte) *C.uchar {
	if len(b) == 0 {
		return nil
	}
	return (*C.uchar)(unsafe.Pointer(&b[0]))
}

func respBytesPtr(b []byte) (*C.uchar, C.size_t) {
	if b == nil {
		return nil, 0
	}
	if len(b) == 0 {
		return (*C.uchar)(unsafe.Pointer(&emptyRespSentinel[0])), 0
	}
	return (*C.uchar)(unsafe.Pointer(&b[0])), C.size_t(len(b))
}

func copyBytes(buf []byte, n int) []byte {
	if n == 0 {
		return []byte{}
	}
	out := make([]byte, n)
	copy(out, buf[:n])
	return out
}
