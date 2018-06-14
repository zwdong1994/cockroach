//
// Created by victor on 4/30/18.
//

#include <select_from_libroach.h>
#include <encoding_infomation.h>
#include <handle_stmt.h>
#include <stdlib.h>
#include <rocksIO_op.h>
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

    std::map<unsigned long long, row_res *> primaryid_to_result;
    std::map<std::string, unsigned long long> primary_to_id;
    std::map<std::string, unsigned long long>::iterator p2id_iter;
    unsigned long long present_id;
    res_info r_info;
    std::string current_table_name;
    void delete_res();
    row_result *p;
    char temp_result[200];
    DBString *head;
    bool output;
    unsigned long long total_trans;

private:

    get_res(get_res const &);
    get_res& operator = (get_res const&);
    get_res();
    ~get_res();
};

void set_current_T_name(char *table_name);
void set_limit(char *upper, char *lower, char *T_name, const char *col_name, rg &range, int &col);
void return_res(char *primary, unsigned long long &primary_id, int &seq, rocksdb::Iterator* it);
void return_equal_res(char *primary, unsigned long long &primary_id, int &seq, std::string value);
void get_single_res(const char *key, char *table_name, qci *q_col_name, int col_num, int T_id);


get_res::get_res() {
    total_trans = 0;
    output = false;
    head = NULL;
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

    int column_num;
    int temp_tid, temp_colid, seek_tid, seek_colid;
    unsigned long long primary_id;
    char primary[MAX_PRIMARY_LENGTH + 1];
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

    if(enc_info -> get_primaryname(table_name, mid_str) == 0)// mid_str represent the primary column name.
        return;
    set_current_T_name(table_name);
    //std::cout << "mid_str: " << mid_str << std::endl;

    //std::cout << mid_str.compare(range_q.variable_name) << std::endl;
    if(mid_str.compare(range_q.variable_name) == 0) { // if the query variable is primary key.
        char encode_str_lower[100];
        char encode_str_upper[100];
        for(int i = 0; i < column_num; i++){
            int q_col_id;
            //std::cout << range_q.lower_limit << std::endl;
            //std::cout << q_col_name[i].column_name << std::endl;
            set_limit(encode_str_upper, encode_str_lower, table_name, q_col_name[i].column_name.data(), range_q, q_col_id);
            //std::cout << encode_str_lower << "          " << encode_str_upper <<std::endl;
            //std::cout << q_col_name[i].column_name << std::endl;
            if(encode_str_lower[0] != 0)
                sscanf(encode_str_lower, "/%d/%d/%s", &temp_tid, &temp_colid, primary);
            if(encode_str_upper[0] != 0)
                sscanf(encode_str_upper, "/%d/%d/%s", &temp_tid, &temp_colid, primary);

            //std::cout << "temp_colid: " << temp_colid << std::endl;

            sprintf(Tid_colid, "/%d/%d/", temp_tid, temp_colid);
            //std::cout << Tid_colid << std::endl;
            if(encode_str_lower[0] != 0 && encode_str_upper[0] == 0){  // x > lower
                //std::cout << ">>>>>>>>>>" << std::endl;
                for (it->Seek(encode_str_lower); it->Valid() && (key = it->key().ToString()).compare(encode_str_lower) >= 0 ; it->Next()) {
                    //std::cout << key << std::endl;

                    strcpy(key_char, it->key().data());
                    sscanf(key_char, "/%d/%d/%s", &seek_tid, &seek_colid, primary); // get the primary key.
                    if(seek_tid != temp_tid || seek_colid != temp_colid)
                        break;
                    primary[MAX_PRIMARY_LENGTH] = 0;
                    //std::cout << "primary: " << atoi(primary) << std:: endl;
                    //std::cout << "lower: " << atoi(range_q.lower_limit.data()) << std::endl;
                    if(seek_colid != q_col_id)
                        break;

                    //std::cout << "key: " << key << std::endl;
                    //std::cout << "value: " << value << std::endl;
                    return_res(primary, primary_id, seq, it);

                }
            } else if(encode_str_lower[0] == 0 && encode_str_upper[0] != 0) { // x < upper
                for (it->Seek(Tid_colid); it->Valid() && (key = it->key().ToString()).compare(encode_str_upper) < 0 ; it->Next()) {
                    //std::cout << key << std::endl;
                    //std::cout << encode_str_upper << std::endl;

                    strcpy(key_char, key.data());
                    sscanf(key_char, "/%d/%d/%s", &seek_tid, &seek_colid, primary); // get the primary key.
                    if(seek_tid != temp_tid || seek_colid != temp_colid)
                        continue;
                    primary[MAX_PRIMARY_LENGTH] = 0;
                    //std::cout << "primary: " << atoi(primary) << std:: endl;
                    //std::cout << "lower: " << atoi(range_q.lower_limit.data()) << std::endl;
                    if(seek_colid != q_col_id)
                        continue;

                    //std::cout << "key: " << key << std::endl;
                    //std::cout << "value: " << value << std::endl;
                    return_res(primary, primary_id, seq, it);
                }
            } else { // x = *****. And lower = upper = *****
                if(strcmp(encode_str_lower, encode_str_upper) == 0){
                    sscanf(encode_str_lower, "/%d/%d/%s/", &temp_tid, &temp_colid, primary); // get the primary key.
                    primary[strlen(primary) - 1] = 0;
                    key = encode_str_lower;
                    rocks_op -> kv_read(encode_str_lower, value);
                    if(value.size() <= 0)
                        continue;
                    g_res -> total_trans += value.size();
                    //std::cout << "key: " << key << std::endl;
                    //std::cout << "value: " << value << std::endl;
                    return_equal_res(primary, primary_id, seq, value);

                }

            }
            seq ++;
        }
    } else {
        int var_id;
        int T_id;


        T_id = enc_info -> get_table_id(table_name);
        if(T_id == -1)
            return;
        var_id = enc_info -> get_column_id(table_name, range_q.variable_name.data());
        if(var_id == -1)
            return;

        sprintf(Tid_colid, "/%d/%d/", T_id, var_id);
        //std::cout << Tid_colid << std::endl;
        for (it->Seek(Tid_colid); it->Valid(); it->Next()) {
            sscanf(it -> key().data(), "/%d/%d/%s", &seek_tid, &seek_colid, primary); // get the primary key.
            //std::cout << it -> key().data() << "    "<< seek_colid << std::endl;
            if(seek_colid != var_id)
                break;

            value = it -> value().ToString();


            g_res -> total_trans += value.size();
            if(range_q.lower_limit != "*" && range_q.upper_limit == "*") { // x > lower
                if((value.size()) > range_q.lower_limit.size()){ //length(x) > length(lower)
                    get_single_res(it -> key().data(), table_name, q_col_name, column_num, T_id);
                } else if(value.size() < range_q.lower_limit.size()) { //length(x) < length(lower)
                    continue;
                } else { //length(x) = length(lower)
                    if (value.compare(range_q.lower_limit) > 0) {
                        get_single_res(it->key().data(), table_name, q_col_name, column_num, T_id);
                    }
                }
            }
            else if(range_q.lower_limit == "*" && range_q.upper_limit != "*") { // x < upper
                if(value.size() > range_q.upper_limit.size()){ //length(x) > length(lower)
                    continue;

                } else if(value.size() < range_q.upper_limit.size()) { //length(x) < length(lower)
                    get_single_res(it -> key().data(), table_name, q_col_name, column_num, T_id);
                } else { //length(x) = length(lower)
                    if (value.compare(range_q.upper_limit) < 0) {
                        get_single_res(it -> key().data(), table_name, q_col_name, column_num, T_id);
                    }
                }

            }
            else if(range_q.lower_limit == range_q.upper_limit ) { // x = *****

                if(value.compare(range_q.lower_limit) == 0) {
                    //num++;
                    get_single_res(it -> key().data(), table_name, q_col_name, column_num, T_id);
                }
            }
            else
                return;

        }

    }

    g_res -> p2id_iter = g_res -> primary_to_id.begin();
    g_res -> p = g_res -> primaryid_to_result[g_res -> p2id_iter -> second];
    g_res -> r_info . result_num = g_res -> present_id;
    g_res -> r_info . column_num = column_num;
    g_res -> output = true;
    std::cout << "The transfer size from rocksdb is: " << g_res -> total_trans << "B" << std::endl;
    g_res -> total_trans = 0;
    //std::cout << "asdfadsasdf" << std::endl;
}


void push_result(DBString **result) { //Push the result to the cockroach.

    //row_result *p = NULL;
    get_res *g_res = get_res::Get_get_res();
    //std::cout << "s111111111111111111" << std::endl;
    DBString *p = g_res -> head;
    row_result *hp = NULL;
    encoding_info *enc_info = encoding_info::Get_encoding_info();


    while(g_res -> p2id_iter != g_res -> primary_to_id.end()) {
        //std::cout << "primary name: " << g_res -> p2id_iter -> first;
        /*
        while(g_res -> p != NULL){
            //std::cout << " column id: " << g_res -> p ->column_n << "  result: " << g_res -> p -> result << std::endl;
            result -> col_id = g_res -> p -> column_n;
            result->len = strlen(g_res -> p -> result.data());
            result->data = g_res -> temp_result;
            strcpy(result -> data, g_res -> p -> result.data());
            g_res -> p = g_res -> p -> next;
            return;
        }
         */
        while(g_res -> p != NULL){
            //std::cout << " column id: " << g_res -> p ->column_n << "  result: " << g_res -> p -> result << std::endl;
            p -> col_id = g_res -> p -> column_n;

            p->len = strlen(g_res -> p -> result.data());

            p->data = (char *)malloc(p -> len + 1);
            strcpy(p -> data, g_res -> p -> result.data());
            hp = g_res -> p;
            g_res -> p = g_res -> p -> next;
            p = p -> next;
            enc_info -> free_rbs(hp);
            //return;
        }
        *result = g_res -> head;
        //p = *result;
        /*while(p != NULL){
            std::cout << p ->data << "         " << p -> col_id << std::endl;
            p = p->next;
        }*/
        //std::cout << result -> data << std::endl;
        //std::cout << "asdfadfasdf" <<std::endl;
        g_res -> p = NULL;
        g_res -> p2id_iter ++;
        g_res -> p = g_res -> primaryid_to_result[g_res -> p2id_iter -> second];
        if(g_res -> p2id_iter == g_res -> primary_to_id.end()){
            //std::cout << "sadfadsfasdfasdf" << std::endl;
            g_res -> delete_res();
            return;
        }
        return;
    }


}

void get_result_num(DBres *res) { //Get the result number and give it back to the cockroach.
    get_res *g_res = get_res::Get_get_res();
    if(g_res -> output == true) {
        encoding_info *enc_info = encoding_info::Get_encoding_info();
        DBString *p = NULL, *hp = NULL;
        if (g_res->r_info.column_num == 0 && g_res->r_info.result_num == 0)
            return;

        if (g_res->head != NULL) {
            p = g_res->head;
            while (p != NULL) {
                hp = p->next;
                delete p;
                p = hp;
            }
            g_res->head = NULL;
        }
        for (int i = 0; i < g_res->r_info.column_num; i++) {
            p = new DBString;

            if (g_res->head != NULL) {
                p->next = g_res->head;
                g_res->head = p;
            } else {
                p->next = NULL;
                g_res->head = p;
            }
        }
        res->total_col_num = enc_info->get_column_num(g_res->current_table_name.data());
        res->column_num = g_res->r_info.column_num;
        res->result_num = g_res->r_info.result_num;
        g_res -> output = false;
    } else {
        res->total_col_num = 0;
        res->column_num = 0;
        res->result_num = 0;
    }
}

void set_current_T_name(char *table_name) {
    get_res *g_res = get_res::Get_get_res();
    g_res -> current_table_name = table_name;
}

void set_limit(char *upper, char *lower, char *T_name, const char *col_name, rg &range, int &col) {
    if(range.lower_limit != "*")
        strcpy(lower, encode_colid(T_name, col_name, range.lower_limit.data(), col));
    else
        lower[0] = 0;
    if(range.upper_limit != "*")
        strcpy(upper, encode_colid(T_name, col_name, range.upper_limit.data(), col));
    else
        upper[0] = 0;
}

void return_res(char *primary, unsigned long long &primary_id, int &seq, rocksdb::Iterator* it) {
    encoding_info *enc_info = encoding_info::Get_encoding_info();
    get_res *g_res = get_res::Get_get_res();
    g_res -> p2id_iter = g_res -> primary_to_id.find(primary);
    if(g_res -> p2id_iter == g_res -> primary_to_id.end())
        g_res -> primary_to_id[primary] = g_res -> present_id ++;
    primary_id = g_res -> primary_to_id[primary];
    row_res *r_r = enc_info -> malloc_rbs();
    row_res *temp_res = NULL;
    r_r -> column_n = seq;
    r_r -> result = it->value().ToString();
    g_res -> total_trans += r_r -> result.size();
    temp_res = g_res -> primaryid_to_result[primary_id];
    if(temp_res != NULL){
        g_res -> primaryid_to_result[primary_id] = r_r;
        r_r -> next = temp_res;
    } else {
        g_res -> primaryid_to_result[primary_id] = r_r;
        r_r -> next = NULL;
    }
}

void return_equal_res(char *primary, unsigned long long &primary_id, int &seq, std::string value) {
    encoding_info *enc_info = encoding_info::Get_encoding_info();
    get_res *g_res = get_res::Get_get_res();
    g_res -> p2id_iter = g_res -> primary_to_id.find(primary);
    if(g_res -> p2id_iter == g_res -> primary_to_id.end())
        g_res -> primary_to_id[primary] = g_res -> present_id ++;
    primary_id = g_res -> primary_to_id[primary];
    row_res *r_r = enc_info -> malloc_rbs();
    row_res *temp_res = NULL;
    r_r -> column_n = seq;
    r_r -> result = value;
    g_res -> total_trans += r_r -> result.size();
    temp_res = g_res -> primaryid_to_result[primary_id];
    if(temp_res != NULL){
        g_res -> primaryid_to_result[primary_id] = r_r;
        r_r -> next = temp_res;
    } else {
        g_res -> primaryid_to_result[primary_id] = r_r;
        r_r -> next = NULL;
    }
}

void get_single_res(const char *key, char *table_name, qci *q_col_name, int col_num, int T_id) {
    char encode_str[100];
    int temp_tid, temp_colid;
    char primary[MAX_PRIMARY_LENGTH + 1];
    int seq = 0;
    unsigned long long primary_id;
    //get_res *g_res = get_res::Get_get_res();
    std::string value;
    encoding_info *enc_info = encoding_info::Get_encoding_info();
    rocksIO* rocks_op = rocksIO::Get_rocksIO();
    sscanf(key, "/%d/%d/%s", &temp_tid, &temp_colid, primary);
    for(int i = 0; i < col_num; i++) {
        sprintf(encode_str, "/%d/%d/%s", T_id, enc_info -> get_column_id(table_name, q_col_name[i].column_name.data()), primary);
        rocks_op -> kv_read(encode_str, value);
        //g_res -> total_trans += value.size();
        //std::cout << "key: " << encode_str << std::endl;
        //std::cout << "value: " << value << std::endl;
        return_equal_res(primary, primary_id, seq, value);
        seq ++;
    }
}