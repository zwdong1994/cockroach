//
// Created by victor on 4/25/18.
//

#include <iostream>
#include <stdlib.h>
#include <string.h>
#include <insert_from_crdb.h>
#include "encoding_infomation.h"
#include "rocksIO_op.h"





void insert_kv(char *T_name, char *col_name, char *value, char *primary_name, char *primary) {
    char *encode_s;

    std::string str;

    if(strlen(primary) > MAX_PRIMARY_LENGTH)
        return;
    encoding_info *enc_info = encoding_info::Get_encoding_info();
    rocksIO* rocks_op = rocksIO::Get_rocksIO();
    //std::cout << primary_name << std::endl;

    enc_info -> table_to_primaryname(T_name, primary_name);
    encode_s = encode_(T_name, col_name, primary);
    //std::cout << encode_s << std::endl;
    rocks_op -> kv_write(encode_s, value);
    free(T_name);
    free(col_name);
    free(value);
    free(primary);
    free(primary_name);
    //rocks_op -> kv_read(encode_s, str);
    //std::cout << str << std::endl;

}

