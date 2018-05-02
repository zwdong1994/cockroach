//
// Created by victor on 4/30/18.
// modified by Forenewhan on 5/2/2018
//

#include "handle_stmt.h"
#include <iostream>
#include <regex>

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
	std::string cmd(stmt);

	// first pattern, select content from "from [content] where" structure
	std::regex rmah(R"((from|FROM)\s+(.*)\s+(where|WHERE))");
    std::smatch mah;
    // mah[2] stores the table name required
    if(std::regex_search(cmd, mah, rmah)){
		 strcpy(table, mah.str(2).c_str());
    }
	else{
		std::cout << "table name input error" << std::endl;
		return NULL;
	}

	// second pattern, get range information
	rmah = R"((where|WHERE)\s+(.*)+([>|<\=])\s+(.*))";
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

}
