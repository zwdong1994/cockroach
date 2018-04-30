//
// Created by victor on 4/30/18.
//

#ifndef ROACHLIB_HANDLE_STMT_H
#define ROACHLIB_HANDLE_STMT_H

#include <query_info.h>


qci* handle_stmt(char *stmt, rg &range_q, char *table_name, int &col_num); //Return the queried column information.

#endif //ROACHLIB_HANDLE_STMT_H
