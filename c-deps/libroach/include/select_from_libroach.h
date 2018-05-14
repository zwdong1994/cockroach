//
// Created by victor on 4/30/18.
//

#ifndef ROACHLIB_SELECT_FROM_LIBROACH_H
#define ROACHLIB_SELECT_FROM_LIBROACH_H

#include <rocksIO_op.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct result_num_info{
    int result_num;
    int column_num;
    int total_col_num;
}DBres;

typedef struct {
    char* data;
    int len;
    int col_id;
} DBString;

void commit_stmts(char *command);
void get_result_num(DBres* res);
void push_result(DBString *result, int result_num, int column_num);

#ifdef __cplusplus
}
#endif

bool SplitKey_(rocksdb::Slice buf, rocksdb::Slice *key, rocksdb::Slice *timestamp);

int Compare_(const rocksdb::Slice &a, const rocksdb::Slice &b);


#endif //ROACHLIB_SELECT_FROM_LIBROACH_H
