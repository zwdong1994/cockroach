//
// Created by victor on 4/27/18.
//

#ifndef ROACHLIB_ENCODING_INFOMATION_H
#define ROACHLIB_ENCODING_INFOMATION_H

#include <map>
#include <iostream>

class encoding_info {
public:
    static encoding_info *Get_encoding_info();
    static encoding_info *encoding_info_instance;
    std::map<std::string, int> table_to_col; // map from /Table_id/column to column_id.
    std::map<std::string, int> table_to_id; // map from table name to table id.
    std::map<std::string, int> table_to_primary;
    int insert_colomn_id(char *encode_column);
    int insert_table(char *table_name);

private:

    std::map<std::string, int>::iterator it;

    int present_table_id;
    int present_column_id;

    encoding_info();
    encoding_info(encoding_info const &);
    encoding_info& operator = (encoding_info const&);
    ~encoding_info();
};

#endif //ROACHLIB_ENCODING_INFOMATION_H
