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

uint16_t flowrulz_intern(const unsigned char* s_ptr, size_t s_len);
void flowrulz_intern_lookup(uint16_t id, unsigned char* out_ptr, size_t* out_len);

int flowrulz_register_plugin(const unsigned char* name_ptr, size_t name_len, const unsigned char* wasm_ptr, size_t wasm_len);
*/
import "C"

import (
	"fmt"
	"unsafe"
)

func Compile(dsl string, ruleID string) ([]byte, error) {
	if len(dsl) == 0 {
		return nil, fmt.Errorf("compile: empty dsl")
	}
	dslBytes := []byte(dsl)
	ridBytes := []byte(ruleID)

	outBuf := *outputBufPool.Get().(*[]byte)
	defer outputBufPool.Put(&outBuf)
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
	out := make([]byte, outLen)
	copy(out, outBuf[:outLen])
	return out, nil
}

func RegisterPlugin(name string, wasmBytes []byte) error {
	if len(name) == 0 {
		return fmt.Errorf("register plugin: empty name")
	}
	if len(wasmBytes) == 0 {
		return fmt.Errorf("register plugin '%s': empty wasm bytes", name)
	}
	nameBytes := []byte(name)
	rc := C.flowrulz_register_plugin(
		(*C.uchar)(unsafe.Pointer(&nameBytes[0])), C.size_t(len(nameBytes)),
		(*C.uchar)(unsafe.Pointer(&wasmBytes[0])), C.size_t(len(wasmBytes)),
	)
	if rc != 0 {
		return fmt.Errorf("register plugin '%s': ffi error %d", name, rc)
	}
	return nil
}
