//
// Created by victor on 4/30/18.
//

#include <select_from_libroach.h>
#include <encoding_infomation.h>
#include <handle_stmt.h>
#include <cstring>
#include <string.h>
#include <stdlib.h>

#include <iostream>
#include <map>

typedef struct result_info{
    int result_num;
    int column_num;
}res_info;



class get_res{
public:
    static get_res *Get_get_res();
    static get_res *get_res_instance;

    std::map<int, row_res *> primaryid_to_result;
    std::map<std::string, int> primary_to_id;
    std::map<std::string, int>::iterator p2id_iter;
    int present_id;
    res_info r_info;
    std::string current_table_name;
    void delete_res();
    row_result *p;
    char temp_result[200];

private:

    get_res(get_res const &);
    get_res& operator = (get_res const&);
    get_res();
    ~get_res();
};

void set_current_T_name(char *table_name);


get_res::get_res() {
    p2id_iter = primary_to_id.begin();
    p = NULL;
    present_id = 0;
    r_info.column_num = 0;
    r_info.result_num = 0;
}

get_res::~get_res() {

}

get_res* get_res::get_res_instance = NULL;

get_res* get_res::Get_get_res(){
    static pthread_mutex_t mu = PTHREAD_MUTEX_INITIALIZER;
    if(get_res_instance == NULL){
        pthread_mutex_lock(&mu);
        if(get_res_instance ==NULL)
            get_res_instance = new get_res();
        pthread_mutex_unlock(&mu);
    }
    return get_res_instance;
}

void get_res::delete_res() {
    delete get_res_instance;
    get_res_instance = NULL;
}

void commit_stmts(char *command) { //Get command string from cockroachdb.
    char table_name[30];
    char Tid_colid[30];
    char encode_str_lower[100];
    char encode_str_upper[100];
    int column_num;
    int temp_tid, temp_colid, seek_tid, seek_colid;
    int primary_id;
    char primary[20];
    std::string key, value;
    char key_char[100];
    char select[7] = "select";
    char SELECT[7] = "SELECT";
    int seq = 0;
    rocksdb::Iterator* it;
    rg range_q;
    qci *q_col_name;
    std::string mid_str;
    encoding_info *enc_info = encoding_info::Get_encoding_info();
    rocksIO* rocks_op = rocksIO::Get_rocksIO();
    get_res *g_res = get_res::Get_get_res();


    if(strncmp(command, select, 6) != 0 && strncmp(command, SELECT, 6) != 0)
        return;

    it = rocks_op -> get_iter();
    //std::cout << command << std::endl;
    q_col_name = handle_stmt(command, range_q, table_name, column_num);
    if(q_col_name == NULL)
        return;
    //std::cout << "table name: " << table_name << std::endl;



    //std::cout << "range_q.variable_name: " << range_q.variable_name << std::endl;
    //std::cout << "range_q.low: " << range_q.lower_limit << std::endl;
    //std::cout << "range_q.up: " << range_q.upper_limit << std::endl;

    if(enc_info -> get_primaryname(table_name, mid_str) == 0)// mid str represent the primary column name.
        return;
    set_current_T_name(table_name);
    //std::cout << "mid_str: " << mid_str << std::endl;

    //std::cout << mid_str.compare(range_q.variable_name) << std::endl;
    if(mid_str.compare(range_q.variable_name) == 0) { // if the query variable is primary key.
        for(int i = 0; i < column_num; i++){
            int q_col_id;
            //std::cout << range_q.lower_limit << std::endl;
            if(range_q.lower_limit != "*")
                strcpy(encode_str_lower, encode_colid(table_name, q_col_name[i].column_name.data(), range_q.lower_limit.data(), q_col_id));
            else
                encode_str_lower[0] = 0;
            if(range_q.upper_limit != "*")
                strcpy(encode_str_upper, encode_colid(table_name, q_col_name[i].column_name.data(), range_q.upper_limit.data(), q_col_id));
            else
                encode_str_upper[0] = 0;
            //std::cout << q_col_name[i].column_name << std::endl;
            if(encode_str_lower[0] != 0)
                sscanf(encode_str_lower, "/%d/%d/%s/", &temp_tid, &temp_colid, primary);
            if(encode_str_upper[0] != 0)
                sscanf(encode_str_upper, "/%d/%d/%s/", &temp_tid, &temp_colid, primary);

            //std::cout << "temp_colid: " << temp_colid << std::endl;

            sprintf(Tid_colid, "/%d/%d/", temp_tid, temp_colid);
            if(encode_str_lower[0] != 0 && encode_str_upper[0] == 0){  // x > lower
                //std::cout << ">>>>>>>>>>" << std::endl;
                for (it->Seek(encode_str_lower); it->Valid() ; it->Next()) {
                    key = it->key().ToString();


                    //std::cout << key << std::endl;

                    strcpy(key_char, key.data());
                    sscanf(key_char, "/%d/%d/%s/", &seek_tid, &seek_colid, primary); // get the primary key.
                    if(seek_tid != temp_tid && seek_colid != temp_colid)
                        continue;
                    primary[strlen(primary) - 1] = 0;
                    //std::cout << "primary: " << atoi(primary) << std:: endl;
                    //std::cout << "lower: " << atoi(range_q.lower_limit.data()) << std::endl;
                    if(seek_colid != q_col_id)
                        continue;
                    if(atoi(primary) <= atoi(range_q.lower_limit.data()))
                        continue;
                    //std::cout << "key: " << key << std::endl;
                    //std::cout << "value: " << value << std::endl;
                    g_res -> p2id_iter = g_res -> primary_to_id.find(primary);
                    if(g_res -> p2id_iter == g_res -> primary_to_id.end())
                        g_res -> primary_to_id[primary] = g_res -> present_id ++;
                    primary_id = g_res -> primary_to_id[primary];
                    row_res *r_r = enc_info -> malloc_rbs();
                    row_res *temp_res = NULL;
                    r_r -> column_n = seq;
                    r_r -> result = it->value().ToString();
                    temp_res = g_res -> primaryid_to_result[primary_id];
                    if(temp_res != NULL){
                        g_res -> primaryid_to_result[primary_id] = r_r;
                        r_r -> next = temp_res;
                    } else {
                        g_res -> primaryid_to_result[primary_id] = r_r;
                        r_r -> next = NULL;
                    }
                }
            } else if(encode_str_lower[0] == 0 && encode_str_upper[0] != 0) { // x < upper
                //std::strrev(encode_str_upper);
                for (it->Seek(Tid_colid); it->Valid() && Compare_(it->key(), encode_str_upper) < 0; it->Next()) {
                    key = it->key().ToString();


                    strcpy(key_char, key.data());
                    sscanf(key_char, "/%d/%d/%s/", &seek_tid, &seek_colid, primary); // get the primary key.

                    if(seek_tid != temp_tid && seek_colid != temp_colid)
                        continue;

                    primary[strlen(primary) - 1] = 0;
                    std::cout << "primary: " << atoi(primary) << std:: endl;
                    std::cout << "upper: " << atoi(range_q.upper_limit.data()) << std::endl;
                    if(seek_colid != q_col_id)
                        continue;
                    //if(atoi(primary) >= atoi(range_q.upper_limit.data()))
                      //  continue;
                    //std::cout << "key: " << key << std::endl;
                    //std::cout << "value: " << value << std::endl;
                    g_res -> p2id_iter = g_res -> primary_to_id.find(primary);
                    if(g_res -> p2id_iter == g_res -> primary_to_id.end())
                        g_res -> primary_to_id[primary] = g_res -> present_id ++;
                    primary_id = g_res -> primary_to_id[primary];
                    row_res *r_r = enc_info -> malloc_rbs();
                    row_res *temp_res = NULL;
                    r_r -> column_n = seq;
                    r_r -> result = it->value().ToString();
                    temp_res = g_res -> primaryid_to_result[primary_id];
                    if(temp_res != NULL){
                        g_res -> primaryid_to_result[primary_id] = r_r;
                        r_r -> next = temp_res;
                    } else {
                        g_res -> primaryid_to_result[primary_id] = r_r;
                        r_r -> next = NULL;
                    }
                }
            } else { // x = *****. And lower = upper = *****
                if(strcmp(encode_str_lower, encode_str_upper) == 0){
                    sscanf(encode_str_lower, "/%d/%d/%s/", &temp_tid, &temp_colid, primary); // get the primary key.
                    primary[strlen(primary) - 1] = 0;
                    key = encode_str_lower;
                    rocks_op -> kv_read(encode_str_lower, value);
                    //std::cout << "key: " << key << std::endl;
                    //std::cout << "value: " << value << std::endl;
                    g_res -> p2id_iter = g_res -> primary_to_id.find(primary);
                    if(g_res -> p2id_iter == g_res -> primary_to_id.end())
                        g_res -> primary_to_id[primary] = g_res -> present_id ++;
                    primary_id = g_res -> primary_to_id[primary];
                    row_res *r_r = enc_info -> malloc_rbs();
                    row_res *temp_res = NULL;
                    r_r -> column_n = seq;
                    r_r -> result = value;
                    temp_res = g_res -> primaryid_to_result[primary_id];
                    if(temp_res != NULL){
                        g_res -> primaryid_to_result[primary_id] = r_r;
                        r_r -> next = temp_res;
                    } else {
                        g_res -> primaryid_to_result[primary_id] = r_r;
                        r_r -> next = NULL;
                    }

                }

            }
            seq ++;
        }
    } else {
        std::cout << "1234" << std::endl;
    }

    g_res -> p2id_iter = g_res -> primary_to_id.begin();
    g_res -> p = g_res -> primaryid_to_result[g_res -> p2id_iter -> second];
    g_res -> r_info . result_num = g_res -> present_id;
    g_res -> r_info . column_num = column_num;
}


void push_result(DBString *result, int result_num, int column_num) { //Push the result to the cockroach.

    //row_result *p = NULL;
    get_res *g_res = get_res::Get_get_res();
    //std::cout << "s111111111111111111" << std::endl;

    while(g_res -> p2id_iter != g_res -> primary_to_id.end()) {
        //std::cout << "primary name: " << g_res -> p2id_iter -> first;
        while(g_res -> p != NULL){
            //std::cout << " column id: " << g_res -> p ->column_n << "  result: " << g_res -> p -> result << std::endl;
            result -> col_id = g_res -> p -> column_n;
            result->len = strlen(g_res -> p -> result.data());
            result->data = g_res -> temp_result;
            strcpy(result -> data, g_res -> p -> result.data());
            g_res -> p = g_res -> p -> next;
            return;
        }
        //std::cout << "asdfadfasdf" <<std::endl;
        g_res -> p = NULL;
        g_res -> p2id_iter ++;
        g_res -> p = g_res -> primaryid_to_result[g_res -> p2id_iter -> second];
        if(g_res -> p2id_iter == g_res -> primary_to_id.end()){
            //std::cout << "sadfadsfasdfasdf" << std::endl;
            g_res -> delete_res();
            return;
        }
    }

}

void get_result_num(DBres *res) { //Get the result number and give it back to the cockroach.
    get_res *g_res = get_res::Get_get_res();
    encoding_info *enc_info = encoding_info::Get_encoding_info();
    res -> total_col_num = enc_info -> get_column_num(g_res -> current_table_name.data());
    res -> column_num = g_res -> r_info.column_num;
    res -> result_num = g_res -> r_info.result_num;
}

void set_current_T_name(char *table_name) {
    get_res *g_res = get_res::Get_get_res();
    g_res -> current_table_name = table_name;
}


int Compare_(const rocksdb::Slice &a, const rocksdb::Slice &b){
    rocksdb::Slice key_a, key_b;
    rocksdb::Slice ts_a, ts_b;
    if (!SplitKey_(a, &key_a, &ts_a) ||
        !SplitKey_(b, &key_b, &ts_b)) {
        // This should never happen unless there is some sort of corruption of
        // the keys.
        return a.compare(b);
    }

    const int c = key_a.compare(key_b);
    if (c != 0) {
        return c;
    }
    if (ts_a.empty()) {
        if (ts_b.empty()) {
            return 0;
        }
        return -1;
    } else if (ts_b.empty()) {
        return +1;
    }
    return ts_b.compare(ts_a);
}


bool SplitKey_(rocksdb::Slice buf, rocksdb::Slice *key, rocksdb::Slice *timestamp) {
    if (buf.empty()) {
        return false;
    }
    const char ts_size = buf[buf.size() - 1];
    if (ts_size >= buf.size()) {
        return false;
    }
    *key = rocksdb::Slice(buf.data(), buf.size() - ts_size - 1);
    *timestamp = rocksdb::Slice(key->data() + key->size(), ts_size);
    return true;
}