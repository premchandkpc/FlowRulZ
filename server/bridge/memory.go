package bridge

/*
#cgo LDFLAGS: -L../../rust/target/release -lflowrulz_core -ldl
#include <stdlib.h>
#include <stdint.h>

unsigned char* flowrulz_msg_alloc(size_t size);
void flowrulz_msg_release(unsigned char* ptr);
*/
import "C"

import (
	"unsafe"
)

func MsgAlloc(size int) unsafe.Pointer {
	return unsafe.Pointer(C.flowrulz_msg_alloc(C.size_t(size)))
}

func MsgRelease(ptr unsafe.Pointer) {
	C.flowrulz_msg_release((*C.uchar)(ptr))
}
