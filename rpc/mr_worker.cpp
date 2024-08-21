#include <vector>
#include <iostream>
#include <map>
#include <fstream>
#include <sstream>
#include <string>
#include <grpcpp/grpcpp.h>
#include "./mapreduce.grpc.pb.h"

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;
using mapreduce::MapReduce;
using mapreduce::MapRequest;
using mapreduce::MapResponse;

void mapf(const std::string & ifname, std::ifstream & input, std::ofstream & output);

class MapClient {
public:
    MapClient(std::shared_ptr<Channel> channel) : stub_(MapReduce::NewStub(channel)) {}

    void mapCall(const std::string worker_id) {
        MapRequest request;
        request.set_worker_id(worker_id);

        MapResponse response;
        ClientContext context;

        Status status = stub_->mapCall(&context, request, &response);

        if (status.ok()) {
            std::cout << "Filename : " << response.filename() << std::endl;
            std::cout << "Process ID : " << response.process_id() << std::endl;
        } else {
            std::cerr << "RPC failed: " << status.error_message() << std::endl;
        }

        // call map on the filename 
        std::string ifname;
        ifname = response.filename();
        std::ifstream input(ifname);
        
        std::string ofname;
        ofname = "mr-" + worker_id + "-" + std::to_string(response.process_id()) + ifname; // only temporarily appending filename to check for race conditions on getting files (gdb says multithreaded server?)
        std::ofstream output;
        output.open(ofname);

        mapf(ifname, input, output);
    }

private:
    std::unique_ptr<MapReduce::Stub> stub_;
};

// map function
void mapf(const std::string & ifname, std::ifstream & input, std::ofstream & output) {
    if (input.is_open()) {
        std::map<std::string, int> res;
        std::string line;
        std::string token;
        while (getline(input, line)) {
            std::istringstream data(line);
            while (data >> token) {
                res[token]++;
            }
        }

        // write to output file
        if (output.is_open()) {
            for (auto& pair : res) {
                output << pair.first << " " << pair.second << std::endl;
            }
            output.close();
        } else {
            std::cerr << "Failed to open output file" << std::endl;
        }
    } else {
        std::cerr << "Failed to open input file " << std::endl;
    }
}


/*
    params:
        [1] - worker_id

*/
int main(int argc, char** argv) {
    /* int worker_id = *argv[1];
    */

    MapClient client(grpc::CreateChannel("0.0.0.0:50051",
        grpc::InsecureChannelCredentials()));
    client.mapCall("1");

    return 0;
}