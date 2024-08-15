#include <vector>
#include <iostream>
#include <map>
#include <fstream>
#include <sstream>
#include <string>
#include <grpcpp/grpcpp.h>

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;


void runServer() {
    std::string server_address("0.0.0.0:50051");

    ServerBuilder builder;
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);
    std::unique_ptr<Server> server(builder.BuildAndStart());
    if (!server) {
        throw std::runtime_error("Failed to create server");
    } else {
        std::cout << "Server listening on " << server_address << std::endl;
    }
}


int main() {

    // initialize server
    runServer();

    std::vector<std::string> filenames = {"pg-being_ernest.txt", "pg-dorian_gray.txt", 
            "pg-frankenstein.txt", "pg-grimm.txt", "pg-huckleberry_finn.txt", 
            "pg-metamorphosis.txt", "pg-sherlock_holmes.txt", "pg-tom_sawyer.txt"};


    


    // do map

    // send request to coordinator for reduce function buckets


    return 0;
}