//
// Created by victor on 4/25/18.
//

#include <iostream>
#include <stdlib.h>
#include <string.h>
#include "insert_from_crdb.h"
#include "encoding_infomation.h"

char Tid_col[30];
char encode_str[100]; // This is the encoded key, which is consist of /table_id/column_id/primary_key.


void get_kv(char *T_name, char *col_name, char *value, char *primary_name, char *primary) {
    encode_table_colname(T_name, col_name, primary);


}

char* encode_table_colname(char *T_name, char *col_name, char *primary){
    int table_id;
    int col_id;
    encoding_info *enc_info = encoding_info::Get_encoding_info();
    table_id = enc_info -> insert_table(T_name); //get table id.
    sprintf(Tid_col, "/%d/%s/", table_id, col_name);
    col_id = enc_info -> insert_colomn_id(Tid_col); //get column id
    sprintf(encode_str, "/%d/%d/%s/", table_id, col_id, primary);
    std::cout << encode_str << std::endl;
    return encode_str;
}