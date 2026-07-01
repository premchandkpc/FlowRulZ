package bridge

/*
#cgo LDFLAGS: -L../../rust/target/release -lflowrulz_core -ldl
#include <stdlib.h>
#include <stdint.h>

int flowrulz_plan_services(const unsigned char* plan_ptr, size_t plan_len, unsigned char* out_ptr, size_t out_cap, size_t* out_len);
uint32_t flowrulz_plan_complexity(const unsigned char* plan_ptr, size_t plan_len);
*/
import "C"

import (
	"encoding/json"
	"fmt"
	"unsafe"
)

type ServiceEntry struct {
	ID   uint16 `json:"id"`
	Name string `json:"name"`
}

func PlanServices(plan []byte) ([]ServiceEntry, error) {
	if len(plan) == 0 {
		return nil, fmt.Errorf("plan services: empty plan")
	}
	outBuf := make([]byte, 4096)
	var outLen C.size_t
	rc := C.flowrulz_plan_services(
		(*C.uchar)(unsafe.Pointer(&plan[0])), C.size_t(len(plan)),
		(*C.uchar)(unsafe.Pointer(&outBuf[0])), C.size_t(cap(outBuf)), &outLen,
	)
	if rc != 0 {
		return nil, fmt.Errorf("plan services: ffi error %d", rc)
	}
	var entries []ServiceEntry
	if err := json.Unmarshal(outBuf[:outLen], &entries); err != nil {
		return nil, fmt.Errorf("plan services: unmarshal: %w", err)
	}
	return entries, nil
}

func PlanComplexity(plan []byte) uint32 {
	if len(plan) == 0 {
		return 0
	}
	return uint32(C.flowrulz_plan_complexity(
		(*C.uchar)(unsafe.Pointer(&plan[0])), C.size_t(len(plan)),
	))
}
