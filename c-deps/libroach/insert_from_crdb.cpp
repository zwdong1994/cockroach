//
// Created by victor on 4/25/18.
//

#include <iostream>
#include <stdlib.h>
#include <string.h>
#include "insert_from_crdb.h"


void get_kv(char *T_name, char *col_name, char *primary, char *primary_name, char *value) {
    if(strcmp(T_name, "episodes") == 0)
    std::cout << "T_name: " << T_name << " col_name: " << col_name << " primary: " << primary
              << " primary_name: " << primary_name << " value: " << value << std::endl;
}