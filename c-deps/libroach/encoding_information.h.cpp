//
// Created by victor on 4/27/18.
//

#include "encoding_infomation.h"
#include <iostream>
#include <stdio.h>
#include <stdlib.h>

encoding_info::encoding_info() {
    present_table_id = 1;
    present_column_id = 0;
}

encoding_info::~encoding_info() {

}

encoding_info* encoding_info::encoding_info_instance = NULL;

encoding_info* encoding_info::Get_encoding_info(){
    static pthread_mutex_t mu = PTHREAD_MUTEX_INITIALIZER;
    if(encoding_info_instance == NULL){
        pthread_mutex_lock(&mu);
        if(encoding_info_instance ==NULL)
            encoding_info_instance = new encoding_info();
        pthread_mutex_unlock(&mu);
    }
    return encoding_info_instance;
}

int encoding_info::insert_colomn_id(char *encode_column) {
    it = table_to_col.find(encode_column);
    if(it == table_to_col.end()) { //the column doesn't exist in the table.
        table_to_col[encode_column] = present_column_id++; //insert column id to table_to_col.
        return present_column_id - 1;
    } else {
        return table_to_col[encode_column];
    }
}

int encoding_info::insert_table(char *table_name) {
    it = table_to_id.find(table_name);
    if(it == table_to_id.end()) { // the table doesn't exist in the table.
        table_to_id[table_name] = present_table_id++;
        return present_table_id - 1;
    }
    else {
        return table_to_id[table_name];
    }
}

int encoding_info::table_to_primaryname(char *table_name, char* primary_name) {
    it_str_to_str = table_to_primary.find(table_name);
    if(it_str_to_str == table_to_primary.end()) {
        table_to_primary[table_name] = std::string(primary_name);
        return 0;
    } else {
        return 1;
    }
}



