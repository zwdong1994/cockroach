//
// Created by victor on 4/27/18.
//

#include "encoding_infomation.h"
#include <iostream>
#include <stdio.h>
#include <stdlib.h>

char Tid_col[30];
char encode_str[100]; // This is the encoded key, which is consist of /table_id/column_id/primary_key.

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

int encoding_info::insert_colomn_id(char *encode_column, char *table_name) {
    it = table_to_col.find(encode_column);
    if(it == table_to_col.end()) { //the column doesn't exist in the table.
        table_to_col[encode_column] = table_to_col_num[table_name] ++; //insert column id to table_to_col.
        return table_to_col[encode_column];
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


char* encode_(char *T_name, const char *col_name, const char *primary){
    int table_id;
    int col_id;
    encoding_info *enc_info = encoding_info::Get_encoding_info();
    table_id = enc_info -> insert_table(T_name); //get table id.
    sprintf(Tid_col, "/%d/%s/", table_id, col_name);
    col_id = enc_info -> insert_colomn_id(Tid_col, T_name); //get column id
    sprintf(encode_str, "/%d/%d/%s/", table_id, col_id, primary);
    //std::cout << encode_str << enc_info -> table_to_col_num[T_name] << std::endl;
    return encode_str;
}

char* encode_colid(char *T_name, const char *col_name, const char *primary, int &col){
    int table_id;
    int col_id;
    encoding_info *enc_info = encoding_info::Get_encoding_info();
    table_id = enc_info -> insert_table(T_name); //get table id.
    sprintf(Tid_col, "/%d/%s/", table_id, col_name);
    col_id = enc_info -> insert_colomn_id(Tid_col, T_name); //get column id
    col = col_id;
    sprintf(encode_str, "/%d/%d/%s/", table_id, col_id, primary);
    //std::cout << encode_str << enc_info -> table_to_col_num[T_name] << std::endl;
    return encode_str;
}
