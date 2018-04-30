//
// Created by victor on 4/25/18.
//

#ifndef ROACHLIB_INSERT_FROM_CRDB_H
#define ROACHLIB_INSERT_FROM_CRDB_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>


#ifdef __cplusplus
extern "C" {
#endif

void insert_kv(char *T_name, char *col_name, char *primary, char *primary_name, char *value);
char* encode_(char *T_name, char *col_name,char *primary);

#ifdef __cplusplus
}
#endif


#endif //ROACHLIB_INSERT_FROM_CRDB_H
