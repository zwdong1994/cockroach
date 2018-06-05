package result

// #cgo CPPFLAGS: -I../../../c-deps/libroach/include
// #cgo LDFLAGS: -lstdc++
// #cgo LDFLAGS: -lrocksdb
// #cgo LDFLAGS: -lroach
// #cgo LDFLAGS: -lprotobuf
// #cgo LDFLAGS: -lsnappy
//
// #include <stdlib.h>
// #include <select_from_libroach.h>
import "C"

func return_col_num() (int, int) {
	var res_info C.DBres

	C.get_result_num(&res_info)
	if res_info.column_num > 0 && res_info.result_num >0 {
		return res_info.column_num, res_info.result_num
	} else {
		return 0, 0
	}
}
