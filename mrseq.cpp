#include <iostream>
#include <vector>
#include <fstream>
#include <sstream>
#include <string>
#include <map>

// map function
std::map<std::string, int> mapf(const std::string & fname, std::ifstream & input);

// reduce function
std::map<std::string, int> reduce(std::vector<std::map<std::string, int>> & imres);


/*
    sequential map reduce for word count (hard coded map and reduce functions)
    command line args:
        space delineated .txt filenames
    output:
        output.txt - 
*/
int main(int argc, char** argv) {
    if (argc < 2) {
        std::cout << "Usage: mrseq inputfiles..."; // naive case here just make input files textfiles of books
        return 1;
    }

    // program output
    std::ofstream output("output.txt");
    if (!output) {
        std::cerr << "Error opening output file" << std::endl;
        return 1;
    }
    output << "Generated output from files: " << std::endl;

    std::vector<std::map<std::string, int>> intermediate_results;

    // open file and run map
    for (int i = 1; i < argc; ++i) {
        output << argv[i] << std::endl;
        std::string fname = argv[i];
        std::ifstream input("./src/" + fname);

        if (!input.is_open()) {
            std::cerr << "Failed to open file " << fname << std::endl;
            continue;
        }

        intermediate_results.emplace_back(mapf(fname, input));
        input.close();
    }
    output << std::endl << std::endl;

    // run reduce on intermediate_results;
    std::map<std::string, int> end_results = reduce(intermediate_results);
    

    for (const auto & pair : end_results) {
        output << pair.first << "  :  " << pair.second << std::endl;
    }

    return 0;
}


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

// reduce function
std::map<std::string, int> reduce(std::vector<std::map<std::string, int>> & imres) {
    std::map<std::string, int> res;
    for (auto i = imres.begin(); i != imres.end(); i++) {
        for (const auto &pair : *i) {
            res[pair.first] += pair.second;
        }
    }
    return res;
}