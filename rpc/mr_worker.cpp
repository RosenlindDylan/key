#include <vector>
#include <iostream>
#include <map>
#include <fstream>
#include <sstream>
#include <string>

// map function
std::map<std::string, int> mapf(const std::string & fname, std::ifstream & input) {
    std::map<std::string, int> res;
    std::string line;
    std::string token;
    while (getline(input, line)) {
        std::istringstream data(line);
        while (data >> token) {
            res[token]++;
        }
    }

    return res;
}



int main() {

    // send request to coordinator for map functions


    // do map

    // send request to coordinator for reduce function buckets


    return 0;
}