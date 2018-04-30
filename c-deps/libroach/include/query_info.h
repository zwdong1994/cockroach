//
// Created by victor on 4/30/18.
//

#ifndef ROACHLIB_QUERY_INFO_H
#define ROACHLIB_QUERY_INFO_H

#include <iostream>

typedef struct range{ //For example, the query condition is: lower_limit < variable_name < upper_limit
    std::string upper_limit;  // * means unlimited.
    std::string lower_limit;
    std::string variable_name;
}rg;

typedef struct query_column_info{
    std::string column_name; // Column name.
}qci;


#endif //ROACHLIB_QUERY_INFO_H
