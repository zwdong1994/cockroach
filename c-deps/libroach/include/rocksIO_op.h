//
// Created by victor on 4/29/18.
//

#ifndef ROACHLIB_ROCKSIO_OP_H
#define ROACHLIB_ROCKSIO_OP_H

#include <rocksdb/db.h>
#include <rocksdb/iterator.h>
#include <cassert>
#include <rocksdb/write_batch.h>
#include <iostream>


class rocksIO{
public:
    static rocksIO *Get_rocksIO();
    static rocksIO *rocksIO_instance;

    int kv_write(char *key, char *value);
    int kv_read(char *key, std::string &value);
    rocksdb::Iterator* get_iter();
    rocksdb::Iterator* it;

private:

    rocksdb::DB* db;
    rocksdb::Options options;

    bool option_set;

    rocksIO();
    rocksIO(rocksIO const &);
    rocksIO& operator = (rocksIO const&);
    ~rocksIO();
};

#endif //ROACHLIB_ROCKSIO_OP_H
