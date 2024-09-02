#include <iostream>
#include <vector>
#include <string>
#include <cctype>
#include <fstream>
#include <cstdlib>
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

std::mutex process_value_mutex;
std::mutex process_tracker_mutex;
std::mutex outfiles_mutex;

void checkTermination(const int num_workers, const std::vector<int>& tracker);
void runServer();
int findLastProcess(const int& worker_id, const std::vector<int>& tracker);

class MRCoordinator final : public MapReduce::Service {
public:
    MRCoordinator(int num_mappers, int num_reducers) : process_value(0), process_tracker(8, -1), num_reducers(num_reducers),
        num_mappers(num_mappers)    
    { // change size to filenames.size()
        for (int i = 0; i < num_reducers; i++) {
            intermediate_data.push_back(std::map<std::string, std::vector<int>>());
        }
    }

    Status mapCall(ServerContext* context, const MapRequest* req, MapResponse* res) override {
        if (req->previous_success() == 1) { // if this worker's previous map succeeded
            process_value_mutex.lock();
            int local_process_value = process_value;            
            process_value++;
            process_value_mutex.unlock();
            std::cout << "Process value " << local_process_value << " running on worker " << req->worker_id() << std::endl;
                        
            if (local_process_value < filenames.size()) {
                std::string worker = "Requesting worker was : " + req->worker_id();
                process_tracker_mutex.lock();
                process_tracker[local_process_value] = req->worker_id();
                process_tracker_mutex.unlock();
                std::string filename = filenames[local_process_value];
                res->set_filename(filename);
                res->set_process_id(local_process_value);
                shuffleSort(req->worker_id);
                return Status::OK;
            } else {
                std::cout << "Done processing files" << std::endl;
                res->set_filename("");
                res->set_process_id(-1);
                
                // check if all workers done
                process_tracker_mutex.lock();
                std::vector<int> local_process_tracker = process_tracker;                
                process_tracker_mutex.unlock();
                checkTermination(num_mappers, local_process_tracker);
                // do sort and shuffle

                return Status::OK; // this is in regards to the rpc working
            }
        } else { // this worker's previous map failed, retry - could make this a seperate function TODO

            std::cout << "Worker " << req->worker_id() << " failed." << std::endl;
            
            // find the last process this worker ran
            process_tracker_mutex.lock();
            std::vector<int> local_process_tracker = process_tracker;
            process_tracker_mutex.unlock();
            int failed_process_id = findLastProcess(req->worker_id(), local_process_tracker);
            if (failed_process_id == -1) {
                std::cerr << "Something went wrong trying to recover a failed map process" << std::endl;
                return Status::CANCELLED;
            }
            std::cout << "Worker " << req->worker_id() << " has failed on process " << failed_process_id << ", attempting to recover..." << std::endl;
            // retry worker on same process id - it'll try a number of times set by the worker, currently 3
            // write something to fail log or something in this branch

            res->set_filename(filenames[failed_process_id]);
            res->set_process_id(failed_process_id);

            return Status::OK;
        }
        
    }

    // currently it'll be not really taking advantage of the server multithreading
    // just whenever a worker calls for a new filename process their last
    void shuffleSort(const int worker_id) { // test this out TODO
        outfiles_mutex.lock();
        std::string infile = filenames[findLastProcess(worker_id, process_tracker)]; // is last proces a valid assumption? think @ this
        std::ifstream input(infile);
        // parse the infile
        std::string line;
        while (getline(input, line)) { // refactor later TODO
            // i guess to lower for now ?? TODO decide @ ascii conversion zero based indexing
            std::string key = line.substr(0, line.find(' '));
            for (auto& e : key) {
                e = std::tolower(e);
            }
            int value = std::stoi(line.substr(line.find(' ') + 1));
            int bucket = (key[0] - 'a') / num_reducers;
            intermediate_data[bucket][key].push_back(value);
        }
        std::cout << "completed shuffling for " << worker_id << std::endl;
        outfiles_mutex.unlock();
    }

private:
    std::vector<int> process_tracker; // each index corresponds to an index of filenames (a file), the element at that index is the worker assigned to it
    int process_value = 0;
    std::vector<std::string> filenames = {"pg-being_ernest.txt", "pg-dorian_gray.txt", 
            "pg-frankenstein.txt", "pg-grimm.txt", "pg-huckleberry_finn.txt", 
            "pg-metamorphosis.txt", "pg-sherlock_holmes.txt", "pg-tom_sawyer.txt"};
    int num_reducers; // this is equal to the number of reducers
    int num_mappers;
    std::vector<std::map<std::string, std::vector<int>>> intermediate_data;
};

void runServer(int num_mappers, int num_reducers) {
    std::string server_address("0.0.0.0:50051");
    MRCoordinator service(num_mappers, num_reducers);

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

void checkTermination(const int num_workers, const std::vector<int>& tracker) { // non - class member method bc it will be utility for reduce as well
    // check last num_workers indices of tracker for -1
    std::cout << " checking termination" << std::endl;
    for (const int& e : tracker) {
        if (e == -1) {
            std::cout << "not terminating because of value " << e << std::endl;
            return; // its still running
        }
    }
    int ret_code = std::system("./kill_processes.sh"); // all workers are done, kill the mr-.* processes
    if (ret_code != 0) {
        std::cerr << "Failed to execute the script." << std::endl;
    } else {
        std::cout << "Script executed successfully." << std::endl;
    }
}


// this is inefficient for some cases TODO
int findLastProcess(const int& worker_id, const std::vector<int>& tracker) { // non - class member method bc it will be utility for reduce as well
    // find the index of last occurrence of worker_id in tracker
    for (int i = tracker.size() - 1; i >= 0; i--) {
        if (tracker[i] == worker_id) {
            return i;
        }
    }
    std::cerr << "Something went wrong trying to redo a bad worker return" << std::endl; // this shouldnt get called based on the structure of the program but just in case
    return -1;
}

int main(int argc, char** argv) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <num_mappers>" << std::endl;
        return 1;
    }
    
    int num_mappers = std::stoi(argv[1]);
    int num_reducers = std::stoi(argv[2]);
    
    runServer(num_mappers, num_reducers);    
    

    // intermediate step

    // shuffle and sort
    // first combine to one file
    int num_files = 8;


    /*    

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

    // send request to coordinator for reduce function buckets

    return 0;
}
