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
    MapClient(std::shared_ptr<Channel> channel, bool debug_flag) : stub_(MapReduce::NewStub(channel)), previous_success(1), retry_counter(3), debug(debug_flag) {}

    bool mapCall(const int worker_id) {
        MapRequest request;
        MapResponse response;

        for (int i = 0; i < retry_counter; i++) { // checking for rpc errors
            request.set_worker_id(worker_id);
            request.set_previous_success(previous_success);
            ClientContext context;
            Status status = stub_->mapCall(&context, request, &response);

            if (status.ok()) {
                std::cout << "Filename : " << response.filename() << ", Process ID : " << response.process_id() << ", Worker ID: " << worker_id << std::endl;
                previous_success = 1;
                break; // succeeded, break out of loop
            } else {
                std::cerr << "RPC failed: " << status.error_message() << std::endl;
                previous_success = 0; // 0 is previous fail
            }

            if (i == retry_counter - 1) {
                return false; // all attempts failed
            }
        }
                

        if (response.process_id() > -1) { // checking for done with files
            // call map on the filename 
            std::string ifname;
            ifname = response.filename();
            std::ifstream input(ifname);
            
            std::string ofname;
            ofname = "mr-" + std::to_string(worker_id) + "-" + std::to_string(response.process_id()) + ifname; // only temporarily appending filename to check for race conditions on getting files (gdb says multithreaded server?)
            std::ofstream output;
            output.open(ofname);

            mapf(ifname, input, output); // return a bool with this to trigger change in previous_success

            // test to see what happens when a map process error gets thrown
            if (debug) { // might not be hitting TODO
                if (worker_id == 1) {
                    if (response.process_id() > 2) {
                        // throw an error, don't actually have to have one just telling the coordinator that there was one during the last map process to see how it handles that
                        std::cout << "Hitting the error block" << std::endl;
                        previous_success = -1;
                        debug = false;
                    }
                } else {
                    debug = false; // only do this once
                }
            }

            return true;
        } else {
            return false;
        }
            
    }

private:
    std::unique_ptr<MapReduce::Stub> stub_;
    int previous_success; // 1 is previous success
    int retry_counter; // number of times to retry, set in constructor to 3
    bool debug; // debug flag for development - see worker main function
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
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <worker_id>" << std::endl;
        return 1;
    }
    int worker_id = std::stoi(argv[1]);

    /*
        toggle this for debug
        currently have worker 1 going down at some point to simulate an error in one worker     
    */
    bool debug = true; 

    MapClient client(grpc::CreateChannel("0.0.0.0:50051",
        grpc::InsecureChannelCredentials()), debug);
    
    std::cout << "Worker " << worker_id << " successfully created" << std::endl;
    
    bool flag = true;
    while (flag) { 
        flag = client.mapCall(worker_id);
        sleep(5);
    }

    return 0;
}