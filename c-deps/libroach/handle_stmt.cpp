//
// Created by victor on 4/30/18.
//

#include "handle_stmt.h"
#include <iostream>


/*
 *  stmts: this is command string.
 *  range_q: the query range.
 *  table: table name.
 *  col_num: column number.
 *
 *  After this function, we could get these information: column number(col_num), table name(char *table),
 *  the query range(range_q), and column information(return ).
 *
 *  And this function will only return SELECT FROM TABLE's column, if the command is not this type, this function
 *  will return NULL.
 */

qci *handle_stmt(char *stmt, rg &range_q, char *table, int &col_num) {
   return NULL;
}
