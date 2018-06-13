//
// Created by victor on 4/27/18.
//

#include "encoding_infomation.h"
#include <iostream>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>

char Tid_col[30];
char encode_str[100]; // This is the encoded key, which is consist of /table_id/column_id/primary_key.
char mid_p[MAX_PRIMARY_LENGTH + 1];

encoding_info::encoding_info() {
    present_table_id = 1;
    present_column_id = 0;
    ini_pool();
}

encoding_info::~encoding_info() {
    destory_pool();
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
        //std::cout << "Table name: " << table_name << "  Primary name: " << table_to_primary[table_name] << std::endl;
        return 0;
    } else {
        return 1;
    }
}

int encoding_info::get_primaryname(char *table_name, std::string &primary_name) {
    it_str_to_str = table_to_primary.find(table_name);
    if(it_str_to_str == table_to_primary.end()) {
        return 0;
        //table_to_primary[table_name] = std::string(primary_name);
        //std::cout << "Table name: " << table_name << "  Primary name: " << table_to_primary[table_name] << std::endl;

    } else {
        primary_name = table_to_primary[table_name];
        return 1;
    }
}

int encoding_info::get_column_num(const char *table_name) {
    return table_to_col_num[table_name];
}

void encoding_info::ini_pool() {
    unsigned long long i = 0;
    row_res *new_alloc = NULL;
    allocate_pool_head = NULL;
    delete_pool_head = NULL;

    for(; i < 20000000; ++i){
        new_alloc = new row_res;
        new_alloc -> next = allocate_pool_head;
        new_alloc -> flag = 0;
        allocate_pool_head = new_alloc;
    }
}

void encoding_info::destory_pool() {
    row_res *ap = allocate_pool_head, *hp = allocate_pool_head -> next;
    row_res *dp = delete_pool_head, *dhp = delete_pool_head -> next;


    while(ap != NULL){
        delete ap;
        ap = hp;
        if(hp != NULL)
            hp = hp ->next;
    }

    while(dp != NULL){
        delete dp;
        dp = dhp;
        if(dhp != NULL)
            dhp = dhp -> next;
    }

}

row_res *encoding_info::malloc_rbs() {
    row_res *alloc_node = NULL;
    if(allocate_pool_head != NULL) {
        alloc_node = allocate_pool_head;
        allocate_pool_head = allocate_pool_head -> next;
        alloc_node -> flag = 1;
        alloc_node->next = NULL;
    }
    else{
        allocate_pool_head = delete_pool_head;
        delete_pool_head = NULL;
        alloc_node = allocate_pool_head;
        allocate_pool_head = allocate_pool_head -> next;
        alloc_node -> next = NULL;
        alloc_node -> flag = 1;
    }
    return alloc_node;
}

int encoding_info::free_rbs(row_res *f_rbs) {
    f_rbs -> next = delete_pool_head;
    f_rbs -> flag = 0;
    delete_pool_head = f_rbs;
    return 0;
}

int encoding_info::get_column_id(char *table_name, const char *column_name) {
    int table_id;

    table_id = get_table_id(table_name); //get table id.
    sprintf(Tid_col, "/%d/%s/", table_id, column_name);
    it = table_to_col.find(Tid_col);
    if(it == table_to_col.end()) { //the column doesn't exist in the table.
        return -1;
    } else {
        return table_to_col[Tid_col];
    }
}

int encoding_info::get_table_id(char *table_name) {
    it = table_to_id.find(table_name);
    if(it == table_to_id.end()) { // the table doesn't exist in the table.
        return -1;
    }
    else {
        return table_to_id[table_name];
    }

}


char* encode_(char *T_name, const char *col_name, const char *primary){
    int table_id;
    int col_id;
    ini_p(mid_p);
    mid_p[MAX_PRIMARY_LENGTH] = 0;
    strcpy(mid_p + MAX_PRIMARY_LENGTH - strlen(primary), primary);
    encoding_info *enc_info = encoding_info::Get_encoding_info();
    table_id = enc_info -> insert_table(T_name); //get table id.
    sprintf(Tid_col, "/%d/%s/", table_id, col_name);
    col_id = enc_info -> insert_colomn_id(Tid_col, T_name); //get column id
    sprintf(encode_str, "/%d/%d/%s", table_id, col_id, mid_p);
    //std::cout << encode_str << enc_info -> table_to_col_num[T_name] << std::endl;
    return encode_str;
}

char* encode_colid(char *T_name, const char *col_name, const char *primary, int &col){
    int table_id;
    int col_id;
    ini_p(mid_p);
    mid_p[MAX_PRIMARY_LENGTH] = 0;
    strcpy(mid_p + MAX_PRIMARY_LENGTH - strlen(primary), primary);
    encoding_info *enc_info = encoding_info::Get_encoding_info();
    table_id = enc_info -> insert_table(T_name); //get table id.
    sprintf(Tid_col, "/%d/%s/", table_id, col_name);
    col_id = enc_info -> insert_colomn_id(Tid_col, T_name); //get column id
    col = col_id;
    sprintf(encode_str, "/%d/%d/%s", table_id, col_id, mid_p);
    //std::cout << encode_str << enc_info -> table_to_col_num[T_name] << std::endl;
    return encode_str;
}

void ini_p(char *s) {
    for(int i = 0; i < MAX_PRIMARY_LENGTH; ++i) {
        s[i] = '0';
    }
}