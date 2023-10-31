#include <iostream>
#include <memory>
#include <string>
#include <vector>
#include <string>
#include <cstdlib>
#include <cstdint>
#include <utility>
#include <cassert>
#include <sysexits.h>
#include <chrono>
#include <random>
#include <limits>

#include <grpc/grpc.h>
#include <grpc++/channel.h>
#include <grpc++/client_context.h>
#include <grpc++/create_channel.h>
#include <grpc++/security/credentials.h>
#include <grpc++/grpc++.h>
#include <grpcpp/grpcpp.h>
#include <thread>
#include <chrono>

#include <csignal>
#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/json_parser.hpp>
#include <boost/algorithm/string.hpp>
#include "file_exchange.grpc.pb.h"

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;

using fileexchange::OffsetData;
using fileexchange::success_failure;

class FileExchangeClient
{
public:
    FileExchangeClient(std::shared_ptr<Channel> channel)
        : m_stub(fileexchange::FileExchange::NewStub(channel))
    {
    }

    bool Put(const std::vector<unsigned long long> &offsets, const std::vector<std::string> &values)
    {
        OffsetData request;

        for (int offset : offsets)
        {
            request.add_offsets(offset);
        }

        for (std::string value : values)
        {
            request.add_values(value);
        }

        success_failure response;
        grpc::ClientContext context;

        grpc::Status status;
        int max_retry_attempts = 3;
        int retry_count = 0;

        while (retry_count < max_retry_attempts)
        {
            status = m_stub->Put(&context, request, &response);
            int backoff_duration_ms = 10;
            if (status.ok())
            {
                break;
            }
            else
            {
                retry_count++;
                std::this_thread::sleep_for(std::chrono::milliseconds(backoff_duration_ms));
            }
        }

        if (status.ok())
        {
            // std::cout << "Data inserted successfully at offset " << response.id() << std::endl;
            // std::cout << response.id() << std::endl;
            return true;
        }
        else
        {
            std::cerr << "RPC failed: " << status.error_message() << std::endl;
            return false;
        }
    }

    unsigned long long generateRandomNumber(unsigned long long max)
    {
        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_int_distribution<unsigned long long> distribution(0, max);
        return distribution(gen);
    }

private:
    std::unique_ptr<fileexchange::FileExchange::Stub> m_stub;
};

void usage [[noreturn]] (const char *prog_name)
{
    std::cerr << "USAGE: " << prog_name << " [put|get] num_id [filename]" << std::endl;
    std::exit(EX_USAGE);
}

int main(int argc, char **argv)
{
    boost::property_tree::ptree config;
    try
    {
        boost::property_tree::read_json("client_config.json", config);
    }
    catch (const std::exception &e)
    {
        std::cerr << "Error reading config file: " << e.what() << std::endl;
        return EXIT_FAILURE;
    }

    std::string serverAddress = config.get<std::string>("server_address");
    long long total_execution_time = 0;
    long long max_put_time = 0;
    long long count = 0;
    FileExchangeClient client(grpc::CreateChannel(serverAddress, grpc::InsecureChannelCredentials()));

    std::ifstream inputFile("/users/Ramya/workloads/client_1.txt");


    if (!inputFile.is_open()) {
        std::cerr << "Failed to open the input file." << std::endl;
        return 1;
    }

    std::string inputLine;

    while (std::getline(inputFile, inputLine)) {
        // std::cout << inputLine << std::endl;

        // Split the input line by semicolons to separate commands
        std::istringstream iss(inputLine);
        std::string command;

        while (std::getline(iss, command, ';')) {
            // Trim leading and trailing whitespace
            command = boost::algorithm::trim_copy(command);

            // Extract operation, offset, and value
            std::string operation;
            std:: string offset, value;

            if (std::istringstream(command) >> offset >> value) {
                // if (operation == "W") {
                    count++;
                    std::vector<unsigned long long> offsets = {std::stoull(offset)};
                    std::vector<std::string> values  = {value};
                    auto start_time = std::chrono::high_resolution_clock::now();

                    client.Put(offsets, values);
                    //  std::cout << "Done" << count << std::endl;
                    auto end_time = std::chrono::high_resolution_clock::now();
                    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time);

                    total_execution_time += duration.count();
                    // std::cout << "Execution time for Put: " << duration.count() << " microseconds" << std::endl;

                    if (duration.count() > max_put_time) {
                        max_put_time = duration.count();
                    }
                // } 
                // else {
                //     std::cerr << "Invalid operation: " << operation << std::endl;
                // }
            } else {
                std::cerr << "Invalid command format: " << command << std::endl;
            }
        }
    }

    inputFile.close();

    // After processing all commands from the file, print the total execution time and max_put_time.
    std::cout << "Total execution time for all operations: " << total_execution_time << " microseconds" << std::endl;
    std::cout << "Maximum execution time for Put: " << max_put_time << " microseconds" << std::endl;
    std::cout << "Total Operations: " << count << std::endl;

    // while (true)
    // {
    //     std::string inputLine;
    //     std::getline(std::cin, inputLine);
    //     std::cout << inputLine << std::endl;
    //     // bool succeeded;
    //     // Split the input line by semicolons to separate commands
    //     std::istringstream iss(inputLine);
    //     std::string command;

    //     while (std::getline(iss, command, ';'))
    //     {
    //         // Trim leading and trailing whitespace
    //         command = boost::algorithm::trim_copy(command);

    //         // Extract operation, offset, and value
    //         std::string operation;
    //         int offset, value;

    //         if (std::istringstream(command) >> operation >> offset >> value)
    //         {
    //             if (operation == "W")
    //             {
    //                 // Prepare and send the offset and value to the server
    //                 std::vector<int> offsets = {offset};
    //                 std::vector<int> values = {value};
    //                 auto start_time = std::chrono::high_resolution_clock::now();
    //                 client.Put(offsets, values);
    //                 auto end_time = std::chrono::high_resolution_clock::now();
    //                 auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time);

    //                 total_execution_time += duration.count();
    //                 std::cout << "Execution time for Put: " << duration.count() << " microseconds" << std::endl;

    //                 if (duration.count() > max_put_time)
    //                 {
    //                     max_put_time = duration.count();
    //                 }
    //             }
    //             else
    //             {
    //                 std::cerr << "Invalid operation: " << operation << std::endl;
    //             }
    //         }
    //         else
    //         {
    //             std::cerr << "Invalid command format: " << command << std::endl;
    //         }
    //     }
    // }

    return EXIT_SUCCESS;
}
