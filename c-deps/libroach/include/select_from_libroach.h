//
// Created by victor on 4/30/18.
//

#ifndef ROACHLIB_SELECT_FROM_LIBROACH_H
#define ROACHLIB_SELECT_FROM_LIBROACH_H



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
void push_result(DBString *result);

#ifdef __cplusplus
}
#endif




#endif //ROACHLIB_SELECT_FROM_LIBROACH_H
