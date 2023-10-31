#include <iostream>
#include <sstream>
#include <memory>
#include <string>
#include <map>
#include <cstdint>
#include <stdexcept>

#include <grpc/grpc.h>
#include <grpc++/server.h>
#include <grpc++/server_builder.h>
#include <grpc++/server_context.h>
#include <grpc++/security/server_credentials.h>
#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/json_parser.hpp>
#include "file_exchange.grpc.pb.h"

#include <mutex>
#include <fcntl.h>
#include <unistd.h>
#include <cstdio>
#include <thread>

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;
using grpc::StatusCode;

using fileexchange::OffsetData;
using fileexchange::success_failure;

class FileExchangeImpl final : public fileexchange::FileExchange::Service
{
private:
    typedef google::protobuf::int32 FileIdKey;
    std::mutex mutex;
    int fileDescriptor;
    int journalFileDescriptor;
    bool journalOnAll;
    bool enableJournal;
    int groupCommit;
    const off_t fileSize = 5ULL * 1024 * 1024 * 1024; // 10GB
    const size_t blockSize = 4 * 1024;                 // 4KB

public:
    FileExchangeImpl(bool journalOnAll, bool enableJournal, int groupCommit):offsetMutexes(2000000) 
    {
        fileDescriptor = open("BlockStore", O_RDWR | O_CREAT, 0666);
        if (fileDescriptor == -1)
        {
            perror("open");
            exit(1);
        }

        if (lseek(fileDescriptor, fileSize - 1, SEEK_SET) == -1)
        {
            perror("lseek");
            exit(1);
        }

        if (write(fileDescriptor, "", 1) != 1)
        {
            perror("write");
            exit(1);
        }

        this->journalOnAll = journalOnAll;
        this->enableJournal = enableJournal;
        this->groupCommit = groupCommit;
        if (enableJournal)
        {
            journalFileDescriptor = open("journal_log", O_RDWR | O_CREAT | O_SYNC, S_IRUSR | S_IWUSR);
            if (journalFileDescriptor == -1)
            {
                std::cerr << "Failed to open 'journal_log'" << std::endl;

                close(journalFileDescriptor);
                exit(1);
            }

            std::thread journalFlushThread([this]()
                                           {
                try {
                    FlushJournalPeriodically();
                } catch (const std::exception& ex) {
            
                    std::cerr << "Exception in thread: " << ex.what() << std::endl;
                } });
            journalFlushThread.detach();
        }
    }

Status Put(
    ServerContext* context,
    const OffsetData* request,
    success_failure* response) override {

    const google::protobuf::RepeatedField<uint64_t>& offsets = request->offsets();
    const google::protobuf::RepeatedPtrField<std::string>& values = request->values();


    // Lock all offsets first
    std::vector<std::unique_lock<std::mutex>> locks;
    for (size_t i = 0; i < offsets.size(); ++i) {
        int offset = offsets.Get(i);
    

        if (offset >= 0) {
            locks.emplace_back(offsetMutexes[offset]);
        }
    }

    for (size_t i = 0; i < offsets.size(); ++i) {
        int offset = offsets.Get(i);
        std::string value = values.Get(i);

        if (offset >= 0) {
            std::string journalString = std::to_string(offset) + " " + value;
            // std::cout << journalString << std::endl;

            // Write to journal
            ssize_t JournalbytesWritten = write(journalFileDescriptor, journalString.c_str(), journalString.size());

            // Flush journal for every write, else thread will block
            if (journalOnAll) {
                if (journalFileDescriptor != -1) {
                    fsync(journalFileDescriptor);
                }
            }

            // Make changes
            off_t position = lseek(fileDescriptor, offset * blockSize, SEEK_SET);
            if (position == -1) {
                return grpc::Status(grpc::INTERNAL, "Failed to set file position");
            }
            ssize_t bytesWritten = write(fileDescriptor, value.c_str(), value.size());
            if (bytesWritten == -1) {
                return grpc::Status(grpc::INTERNAL, "Failed to write data to the file");
            }
            fsync(fileDescriptor);
        } else {
            return grpc::Status(grpc::INVALID_ARGUMENT, "Invalid offset");
        }
    }

    response->set_id(1);
    return grpc::Status::OK;
}




    // Status Put(
    //     grpc::ServerContext *context,
    //     const OffsetData *request,
    //     success_failure *response) override
    // {
       
    //     const std::string &data = request->data();

    //      // const int offset = request->data();
    //     //   std::cout<<"putss req";
    //     if (offset < 0)
    //     {
    //         return grpc::Status(grpc::INVALID_ARGUMENT, "Invalid offset");
    //     }

    //     std::lock_guard<std::mutex> lock(mutex);
    //     std::string journalString = std::to_string(offset) + " " + data;

    //     // write to journal
    //     ssize_t JournalbytesWritten = write(journalFileDescriptor, journalString.c_str(), journalString.size());
    //     // flush  journal for every write, else thread will tc
    //     if (journalOnAll)
    //     {
    //         if (journalFileDescriptor != -1)
    //         {
    //             fsync(journalFileDescriptor);
    //         }
    //     }

    //     // make changes
    //     off_t position = lseek(fileDescriptor, offset * 1024, SEEK_SET);
    //     if (position == -1)
    //     {
    //         return grpc::Status(grpc::INTERNAL, "Failed to set file position");
    //     }
    //     ssize_t bytesWritten = write(fileDescriptor, data.c_str(), data.size());
    //     if (bytesWritten == -1)
    //     {
    //         return grpc::Status(grpc::INTERNAL, "Failed to write data to the file");
    //     }
    //     // FILE* dataFile = fdopen(fileDescriptor, "w");
    //     //     if (dataFile != nullptr) {
    //     //         fflush(dataFile);
    //     //     }
    //     fsync(fileDescriptor);
    //     response->set_id(offset);

    //     return grpc::Status::OK;
    // }

private:
    std::vector<std::mutex> offsetMutexes;


    // void writeLSNToFile(unsigned long long lsn) {
    //     std::ofstream lsnFile("lsn.txt");
    //     if (lsnFile.is_open()) {
    //         lsnFile << lsn;
    //   fflush(lsnFile);
    //         lsnFile.close();
    //     } else {
    //         std::cerr << "Failed to open the LSN file for writing." << std::endl;
    //     }
    // }

    void FlushJournalPeriodically()
    {
        while (true)
        {
            // Sleep for a specified interval
            std::this_thread::sleep_for(std::chrono::seconds(2));

            // Flush the journal
            if (journalFileDescriptor != -1)
            {
                FILE *journalFile = fdopen(journalFileDescriptor, "w");
                if (journalFile != nullptr)
                {
                    fflush(journalFile);
                }
            }
            //  long tailOffset = ftell(journalFile);
            // writeLSNToFile(tailOffset)
        }
    }
};

void RunServer()
{
    // TODO: Allow the port to be customised
    std::string server_address("0.0.0.0:50051");

    boost::property_tree::ptree config;
    try
    {
        boost::property_tree::read_json("server_config.json", config);
    }
    catch (const std::exception &e)
    {
        std::cerr << "Error reading config file: " << e.what() << std::endl;
        return exit(1);
    }

    std::string serverAddress = config.get<std::string>("server_address");
    bool journalOnAll = config.get<bool>("journalOnAll");
    bool enableJournal = config.get<bool>("enableJournal");
    int32_t groupCommit = config.get<int>("groupCommit");
    //   bool journalOnAll = true;
    // bool enableJournal = true;
    // int32_t groupCommit = false;
    // FileExchangeImpl service;
    FileExchangeImpl service(journalOnAll, enableJournal, groupCommit);
    ;

    ServerBuilder builder;
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);
    std::unique_ptr<Server> server(builder.BuildAndStart());
    std::cout << "Server listening on " << server_address << ". Press Ctrl-C to end." << std::endl;
    server->Wait();
}

int main(int argc, char **argv)
{
    RunServer();
    return 0;
}
