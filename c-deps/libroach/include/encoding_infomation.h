//
// Created by victor on 4/27/18.
//

#ifndef ROACHLIB_ENCODING_INFOMATION_H
#define ROACHLIB_ENCODING_INFOMATION_H

#include <map>
#include <iostream>

#define MAX_PRIMARY_LENGTH 20

typedef struct row_result{
    std::string result;
    int column_n;
    struct row_result *next;
    int flag;
}row_res;

class encoding_info {
public:
    static encoding_info *Get_encoding_info();
    static encoding_info *encoding_info_instance;
    std::map<std::string, int> table_to_col; // map from /Table_id/column to column_id.
    std::map<std::string, int> table_to_id; // map from table name to table id.
    std::map<std::string, int> table_to_col_num; //map from table name to column number.
    std::map<std::string, std::string> table_to_primary; //record the primary column name.
    int insert_colomn_id(char *encode_column, char *table_name);
    int insert_table(char *table_name);
    int table_to_primaryname(char *table_name, char *primary_name);

    int get_table_id(char *table_name);
    int get_primaryname(char *table_name, std::string &primary_name);
    int get_column_num(const char *table_name);
    int get_column_id(char *table_name, const char *column_name);

    row_res *allocate_pool_head;
    row_res *delete_pool_head;

    void ini_pool();
    void destory_pool();
    row_res* malloc_rbs();
    int free_rbs(row_res *f_rbs);

private:

    std::map<std::string, int>::iterator it;
    std::map<std::string, std::string>::iterator it_str_to_str;

    int present_table_id;
    int present_column_id;

    encoding_info();
    encoding_info(encoding_info const &);
    encoding_info& operator = (encoding_info const&);
    ~encoding_info();
};

char* encode_(char *T_name, const char *col_name, const char *primary);
char* encode_colid(char *T_name, const char *col_name, const char *primary, int &col);
void ini_p(char *s);


#endif //ROACHLIB_ENCODING_INFOMATION_H
