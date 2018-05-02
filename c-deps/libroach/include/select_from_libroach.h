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
}DBres;

void commit_stmts(char *command);
void get_result_num(DBres** res);
void push_result(char *result, int result_num, int column_num);

#ifdef __cplusplus
}
#endif





#endif //ROACHLIB_SELECT_FROM_LIBROACH_H
