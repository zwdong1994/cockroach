//
// Created by victor on 4/29/18.
//

#include "rocksIO_op.h"



rocksIO::rocksIO() {
    options.create_if_missing = true;
    //options.error_if_exists = true;
    rocksdb::Status status = rocksdb::DB::Open(options, "test_db", &db);
    assert(status.ok());
}

rocksIO::~rocksIO() {
    delete db;
}

rocksIO* rocksIO::rocksIO_instance = NULL;

rocksIO* rocksIO::Get_rocksIO() {
    static pthread_mutex_t mu = PTHREAD_MUTEX_INITIALIZER;
    if(rocksIO_instance == NULL){
        pthread_mutex_lock(&mu);
        if(rocksIO_instance ==NULL)
            rocksIO_instance = new rocksIO();
        pthread_mutex_unlock(&mu);
    }
    return rocksIO_instance;
}

int rocksIO::kv_write(char *key, char *value) {
    if(key != NULL && value != NULL){
        rocksdb::Status s;
        rocksdb::WriteBatch batch;
        batch.Put(key, value);
        s = db -> Write(rocksdb::WriteOptions(), &batch);
        if(s.ok())
            return 1;
        else
            return 0;

    } else {
        return 0;
    }
}

int rocksIO::kv_read(char *key, std::string &value) {
    rocksdb::Status s;
    if(key != NULL){
        s = db -> Get(rocksdb::ReadOptions(), std::string(key), &value);
        return 1;
    }
    return 0;
}

