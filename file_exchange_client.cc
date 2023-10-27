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

#include "utils.h"
#include "sequential_file_writer.h"
#include "file_reader_into_stream.h"

using grpc::Channel;
using grpc::ClientContext;
using grpc::ClientReader;
using grpc::ClientReaderWriter;
using grpc::ClientWriter;
using grpc::Status;

using fileexchange::FileId;
using fileexchange::FileContent;
using fileexchange::FileExchange;
using fileexchange::OffsetData;
using fileexchange::success_failure;


class FileExchangeClient {
public:
    FileExchangeClient(std::shared_ptr<Channel> channel)
        : m_stub(FileExchange::NewStub(channel))
    {
        
    }

    bool PutFile(std::int32_t id, const std::string& filename)
    {
        FileId returnedId;
        ClientContext context;

        std::unique_ptr<ClientWriter<FileContent>> writer(m_stub->PutFile(&context, &returnedId));
        try {
            FileReaderIntoStream< ClientWriter<FileContent> > reader(filename, id, *writer);

            // TODO: Make the chunk size configurable
            const size_t chunk_size = 1UL << 20;    // Hardcoded to 1MB, which seems to be recommended from experience.
            reader.Read(chunk_size);
        }
        catch (const std::exception& ex) {
            std::cerr << "Failed to send the file " << filename << ": " << ex.what() << std::endl;
            // FIXME: Indicate to the server that something went wrong and that the trasfer should be aborted.
        }
    
        writer->WritesDone();
        Status status = writer->Finish();
        if (!status.ok()) {
            std::cerr << "File Exchange rpc failed: " << status.error_message() << std::endl;
            return false;
        }
        else {
            std::cout << "Finished sending file with id " << returnedId.id() << std::endl;
        }

        return true;
    }

bool Put(std::int32_t offset, const std::string& data) {
std::cout<<"in puts";
    OffsetData request;
    request.set_offset(offset);
    request.set_data(data);

    success_failure response;
    grpc::ClientContext context;

    grpc::Status status;
int max_retry_attempts = 3;
int retry_count = 0;

while (retry_count < max_retry_attempts) {
    // Make your RPC call here.
    // Replace YourRpcMethod with your actual RPC method.
    status =  m_stub->Put(&context, request, &response);
int backoff_duration_ms = 10;  
    if (status.ok()) {
        break;
    } else {
        retry_count++;
        std::this_thread::sleep_for(std::chrono::milliseconds(backoff_duration_ms));
    }
}
    if (status.ok()) {
        std::cout << "Data inserted successfully at offset " << response.id() << std::endl;
        std::cout << response.id() << std::endl;
        return true;
    } else {
        std::cerr << "RPC failed: " << status.error_message() << std::endl;
        return false;
    }
    
}
unsigned long long generateRandomNumber(unsigned long long max) {
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<unsigned long long> distribution(0, max);
    return distribution(gen);
}


    bool GetFileContent(std::int32_t id)
    {
        FileId requestedId;
        FileContent contentPart;
        ClientContext context;
        SequentialFileWriter writer;
        std::string filename;

        requestedId.set_id(id);
        std::unique_ptr<ClientReader<FileContent> > reader(m_stub->GetFileContent(&context, requestedId));
        try {
            while (reader->Read(&contentPart)) {
                assert(contentPart.id() == id);
                filename = contentPart.name();
                writer.OpenIfNecessary(contentPart.name());
                auto* const data = contentPart.mutable_content();
                writer.Write(*data);
            };
            const auto status = reader->Finish();
            if (! status.ok()) {
                std::cerr << "Failed to get the file ";
                if (! filename.empty()) {
                    std::cerr << filename << ' ';
                }
                std::cerr << "with id " << id << ": " << status.error_message() << std::endl;
                return false;
            }
            std::cout << "Finished receiving the file "  << filename << " id: " << id << std::endl;
        }
        catch (const std::system_error& ex) {
            std::cerr << "Failed to receive " << filename << ": " << ex.what();
            return false;
        }

        return true;
    }
private:
    std::unique_ptr<fileexchange::FileExchange::Stub> m_stub;
};

void usage [[ noreturn ]] (const char* prog_name)
{
    std::cerr << "USAGE: " << prog_name << " [put|get] num_id [filename]" << std::endl;
    std::exit(EX_USAGE);
}

int main(int argc, char** argv)
{
    if (argc < 3) {
        usage(argv[0]);
    }

    
    boost::property_tree::ptree config;
    try {
        boost::property_tree::read_json("client_config.json", config);
    } catch (const std::exception& e) {
        std::cerr << "Error reading config file: " << e.what() << std::endl;
        return EXIT_FAILURE;
    }

    std::string serverAddress = config.get<std::string>("server_address");
    const std::string verb = argv[1];
    std::int32_t id = -1;
    try {
        id = std::atoi(argv[2]);
    }
    catch (std::invalid_argument) {
        std::cerr << "Invalid Id " << argv[2] << std::endl;
        usage(argv[0]);
    }
    bool succeeded = false;

    // grpc::ChannelArguments channel_args;
    // channel_args.SetInt(GRPC_ARG_MAX_RECONNECT_BACKOFF_MS, max_backoff_ms);
    // channel_args.SetInt(GRPC_ARG_INITIAL_RECONNECT_BACKOFF_MS, initial_backoff_ms);
    // channel_args.SetInt(GRPC_ARG_MAX_RECONNECT_BACKOFF_MS, max_backoff_ms);

// std::shared_ptr<grpc::Channel> channel = grpc::CreateCustomChannel(server_address, grpc::InsecureChannelCredentials(), channel_args);


    FileExchangeClient client(grpc::CreateChannel(serverAddress, grpc::InsecureChannelCredentials()));

    if ("put" == verb) {
        if (4 != argc) {
            usage(argv[0]);
        }
        const std::string filename = argv[3];
        succeeded = client.PutFile(id, filename);
    }

if ("puts" == verb) {
        if (4 != argc) {
            usage(argv[0]);
        }
   
        // unsigned long long rangeMax = 5 * 1024 * 1024; // 5GB in KB
        // unsigned long long randomValue = generateRandomNumber(rangeMax)
        const std::string value = argv[3];
        auto start_time = std::chrono::high_resolution_clock::now();
        succeeded = client.Put(id, value);
        auto end_time = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time);

        std::cout << "Execution time: " << duration.count() << " microseconds" << std::endl;
    }

    else if ("get" == verb) {
        if (3 != argc) {
            usage(argv[0]);
        }
        succeeded = client.GetFileContent(id);
    }
    else {
        std::cerr << "Unknown verb " << verb << std::endl;
        usage(argv[0]);
    }

    return succeeded ? EX_OK : EX_IOERR;
}
