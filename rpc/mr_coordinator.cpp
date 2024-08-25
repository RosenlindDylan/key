#include <iostream>
#include <vector>
#include <string>
#include <mutex>
#include <grpcpp/grpcpp.h>
#include "./mapreduce.grpc.pb.h"

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;
using mapreduce::MapReduce;
using mapreduce::MapRequest;
using mapreduce::MapResponse;

// we're hitting race conditions on filenames
std::mutex process_value_mutex;

class MRCoordinator final : public MapReduce::Service {
public:
    Status mapCall(ServerContext* context, const MapRequest* req, MapResponse* res) override {
        // process_value_mutex.lock();
        std::cout << "Process value " << process_value << std::endl;
        if (process_value < filenames.size()) {
            std::cout << "Processing here" << std::endl;
            std::string worker = "Requesting worker was : " + req->worker_id();
            
            std::string filename = filenames[process_value];
            res->set_filename(filename);
            res->set_process_id(process_value);
            process_value++;
            // process_value_mutex.unlock();
            return Status::OK;
        } else {
            std::cout << "Done processing files" << std::endl;
            res->set_filename("");
            res->set_process_id(-1);
            // process_value_mutex.unlock();
            return Status::OK; // this is in regards to the rpc working
        }
        
    }

private:
    int process_value = 0;
    std::vector<std::string> filenames = {"pg-being_ernest.txt", "pg-dorian_gray.txt", 
            "pg-frankenstein.txt", "pg-grimm.txt", "pg-huckleberry_finn.txt", 
            "pg-metamorphosis.txt", "pg-sherlock_holmes.txt", "pg-tom_sawyer.txt"};
};

void runServer() {
    std::string server_address("0.0.0.0:50051");
    MRCoordinator service;

    ServerBuilder builder;
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);
    std::unique_ptr<grpc::Server> server(builder.BuildAndStart());
    if (!server) {
        throw std::runtime_error("Failed to create server");
    } else {
        std::cout << "Server listening on " << server_address << std::endl;
    }
    server->Wait();
}

int main(int argc, char** argv) {
    
    int num_mappers = *argv[1];
    // int num_reducers = *argv[2];
    
    
    runServer();
    

    // intermediate step

    // shuffle and sort
    // first combine to one file
    int num_files = 8;


    /*

    


    for (int i = 0; i < filenames.size(); i++) {
        std::string* fname = &filenames[i]; // maybe not pointer? deference in rpc call?
        // respond to workers with the filename
    }

    // shuffle and sort phase

    // make bucket folders
    std::vector<std::string> bucket_fnames;
    for (int i = 0; i < (26 / num_reducers); i++) {
        bucket_fnames.push_back("bucket_" + i);
    }

    // read in a file, parse it and assign items to their bucket
    // do this by taking the first letter of the key, converting to a = 0 ascii, then modulo by num_reducers
    for (int i = 0; i < filenames.size(); i++) {
        std::string fname = "mr-" + i;
        
    }

    */

    // do map

    // send request to coordinator for reduce function buckets

    return 0;
}
