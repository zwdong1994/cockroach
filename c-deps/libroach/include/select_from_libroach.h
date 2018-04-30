//
// Created by victor on 4/30/18.
//

#ifndef ROACHLIB_SELECT_FROM_LIBROACH_H
#define ROACHLIB_SELECT_FROM_LIBROACH_H

#ifdef __cplusplus
extern "C" {
#endif

void commit_stmts(char *command);
void push_result(char *result, int col_id);

#ifdef __cplusplus
}
#endif

#endif //ROACHLIB_SELECT_FROM_LIBROACH_H
