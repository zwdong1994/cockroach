//
// Created by victor on 4/29/18.
//

#include "rocksIO_op.h"



rocksIO::rocksIO() {
    tag = 0;
    option_set = false;
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
    if(option_set == true) {
        if (key != NULL && value != NULL) {
            rocksdb::Status s;
            /*rocksdb::WriteBatch batch;
            batch.Put(key, value);
            s = db->Write(rocksdb::WriteOptions(), &batch);*/
            s = db -> Put(rocksdb::WriteOptions(), key, value);
            if (s.ok())
                return 1;
            else
                return 0;

        } else {
            return 0;
        }
    } else {
        std::cout << "error kv_write!" << std::endl;
        exit(-1);
    }
}

int rocksIO::kv_read(const char *key, std::string &value) {
    if(option_set == true) {
        rocksdb::Status s;
        if (key != NULL) {
            s = db->Get(rocksdb::ReadOptions(), std::string(key), &value);
            return 1;
        }
        return 0;
    } else {
        std::cout << "error kv_read!" << std::endl;
        exit(-1);
    }
}

rocksdb::Iterator *rocksIO::get_iter() {
    if(option_set == true) {
        rocksdb::ReadOptions opts;
        bool prefix = 0;
        opts.prefix_same_as_start = prefix;
        opts.total_order_seek = !prefix;
        it = db->NewIterator(opts);
        return it;
    } else {
        exit(-1);
    }
}

void rocksIO::set_opt() {
    option_set = true;
}

void rocksIO::set_option(rocksdb::Options opt) {
    if(option_set == false && tag++) {
        set_opt();
        options = opt;
        options.max_open_files = 20000;
        rocksdb::Status status = rocksdb::DB::Open(options, "test_db", &db);

        assert(status.ok());
    }
}

