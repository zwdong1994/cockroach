//
// Created by victor on 4/30/18.
// modified by Forenewhan on 5/2/2018
//

#include "handle_stmt.h"
#include <string.h>

/*
 *  stmts: this is command string.
 *  range_q: the query range.
 *  table: table name.
 *  col_num: column number.
 *
 *  After this function, we could get these information: column number(col_num), table name(char *table),
 *  the query range(range_q), and column information(return ).
 *
 *  And this function will only return SELECT FROM TABLE's column, if the command is not this type, this function
 *  will return NULL.
 *
 *  By using regex, we could neglect all the space input.
 *  NOTICE: query range must be writen as var op num, e.g. ab < 9 or bc > 10 or ef = 11
 *
 *  examle:
			char *command = "select ab,cd,ef,gh ,  hg,  id from tbl where ab < 9";
			char table_name[30];
			int column_num;
			rg range_q;
			qci *q_col_name;
			q_col_name = handle_stmt(command,range_q, table_name, column_num);
			std::cout << "table name: " << table_name << std::endl;
			std::cout << "column number: " << column_num << std::endl;
			for(int i=0;i<column_num;i++)
				std::cout << "column name: " << q_col_name[i].column_name << std::endl;
			std::cout << "lower limit: " << range_q.lower_limit << std::endl;
			std::cout << "upper limit: " << range_q.upper_limit << std::endl;
			std::cout << "variable name: " << range_q.variable_name << std::endl;
 *
 */

qci *handle_stmt(char *stmt, rg &range_q, char *table, int &col_num) {

    int i, j, k, id = 0, num_of_col = 1;
    char T_name[30] = {0}, col_ref[200] = {0}, range_ref[30] = {0};// id=1, col_ref; id=3, T_name; id=5, range_ref
    char mid_string[30];
    int len_stmt, len_col_ref, len_range_ref;
    qci *q_col_name = NULL;

    len_stmt = strlen(stmt);

    for(i = 0, j = 0; i < len_stmt; i++){
        if(stmt[i] == ' ' && id != 5)
            continue;
        else{
            if(stmt[i - 1] == ' ' && id < 5) {
                ++id;
                j = 0;
            }
            if(id == 0)
                continue;
            else if(id == 1){
                col_ref[j++] = stmt[i];
                if(stmt[i] == ',')
                    num_of_col++;
            }
            else if(id == 3)
                T_name[j++] = stmt[i];
            else if(id == 5)
                range_ref[j++] = stmt[i];
            else
                continue;
        }
    }
    //std::cout << "T_name: " << T_name << std::endl;
    //std::cout << "col_ref: " << col_ref << " col_num: " << num_of_col << std::endl;
    //std::cout << "range_ref: " << range_ref << std::endl;

    len_col_ref = strlen(col_ref);
    len_range_ref = strlen(range_ref);

    col_num = num_of_col;
    strcpy(table, T_name);
    if(num_of_col <= 0 || strlen(range_ref) == 0)
        return NULL;
    q_col_name = new qci[num_of_col];

    for(i = 0, j = 0; i < len_col_ref; i++){
        if(col_ref[i] == ',') {
            //std::cout << q_col_name[j].column_name << std::endl;
            j++;
        }
        else{
            q_col_name[j].column_name += col_ref[i];
        }
    }
    //std::cout << q_col_name[j].column_name << std::endl;

    for(i = 0, j = 0, k = 0; i < len_range_ref; i++){
        if(range_ref[i] == '=' && j == 0) {
            j = 1;
            continue;
        }
        if(range_ref[i] == '>' && j == 0){
            j = 2;
            continue;
        }
        if(range_ref[i] == '<' && j == 0){
            j = 3;
            continue;
        }
        if(j == 0)
            range_q.variable_name += range_ref[i];

        if(j != 0 ) {
            mid_string[k++] = range_ref[i];
            mid_string[k] = 0;
        }

    }
    mid_string[k - 1] = 0;
    if(j == 1){
        range_q.lower_limit = mid_string;
        range_q.upper_limit = mid_string;
    }
    if(j == 2){
        range_q.lower_limit = mid_string;
        range_q.upper_limit = '*';
    }
    if(j == 3){
        range_q.lower_limit = '*';
        range_q.upper_limit = mid_string;
    }


    return q_col_name;
    /*
    std::string cmd(stmt);

    // first pattern, select content from "from [content] where" structure
    std::regex rmah(R"((from|FROM)\s+(.*)\s+(where|WHERE))");
    std::smatch mah;
    // mah[2] stores the table name required
    if(std::regex_search(cmd, mah, rmah)){
        strcpy(table, mah.str(2).c_str());
    }
    else{
        //std::cout << "table name input error" << std::endl;
        return NULL;
    }

    // second pattern, get range information
    rmah = R"((where|WHERE)\s+(.+)\s+([>|<\=])\s+(.+)+(;))";
    if(std::regex_search(cmd, mah, rmah)){
        if(mah[3] == ">"){
            range_q.lower_limit = mah[4];
            range_q.upper_limit = "*";
            range_q.variable_name = mah[2];
        }
        else if(mah[3] == "<"){
            range_q.lower_limit = "*";
            range_q.upper_limit = mah[4];
            range_q.variable_name = mah[2];
        }
        else if(mah[3] == "="){
            range_q.lower_limit = mah[4];
            range_q.upper_limit = mah[4];
            range_q.variable_name = mah[2];
        }
    }
    else{
        std::cout << "range input error" << std::endl;
        return NULL;
    }

    // final pattern, get column information
    rmah = R"((select|SELECT)\s+(.*)\s+(from|FROM))";
    if(std::regex_search(cmd, mah, rmah)){
        // mah[2] stores all column we selected
        cmd = mah[2];

        // pattern to seperate column information by comma
        rmah = R"((\w+\(.+\))|\w+)";
        int count = 0;
        // use temperory sp to store columns
        std::string *sp = new std::string[10];

        // do regex for a second time, extract every column out
        std::regex_iterator<std::string::iterator> rit ( cmd.begin(), cmd.end(), rmah );
        std::regex_iterator<std::string::iterator> rnd;
        while (rit!=rnd) {
            sp[count] = rit->str();
            ++rit;
            ++count;
        }

        // write count into col_num
        col_num = count;
        // transfer sp to qci structure qci
        qci *q_col_name = new qci[count];
        for(int i=0;i<count;i++)
            q_col_name[i].column_name = sp[i];

        delete[] sp; // free sp
        return q_col_name;
    }
    else{
        std::cout << "column input error" << std::endl;
        return NULL;
    }
*/
}