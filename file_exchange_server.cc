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
#include "sequential_file_writer.h"
#include "file_reader_into_stream.h"



#include <mutex>
#include <fcntl.h> 
#include <unistd.h>
#include <cstdio>
#include <thread>

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerReader;
using grpc::ServerReaderWriter;
using grpc::ServerWriter;
using grpc::Status;
using grpc::StatusCode;

using fileexchange::FileId;
using fileexchange::FileContent;
using fileexchange::FileExchange;
using fileexchange::OffsetData;
using fileexchange::success_failure;


class FileExchangeImpl final : public FileExchange::Service {
private:
    typedef google::protobuf::int32 FileIdKey;
     std::mutex mutex;
    int fileDescriptor;
    int journalFileDescriptor;
    bool journalOnAll;
    bool enableJournal;
    int groupCommit;

public:
//    FileExchangeImpl(bool journalOnAll, bool enableJournal, int groupCommit): journalOnAll(journalOnAll), enableJournal(enableJournal), groupCommit(groupCommit) {
//     fileDescriptor = open("block_store", O_RDWR | O_CREAT | O_SYNC, S_IRUSR | S_IWUSR);
//     if (fileDescriptor == -1) {
//         std::cerr << "Failed to open file 'block_store'" << std::endl;
//         exit(1); 
//     }
//     // this->journalOnAll = journalOnAll;
//     // this->enableJournal = enableJournal;
//     // this->groupCommit = groupCommit;
//      if (enableJournal) {
//         journalFileDescriptor = open("journal_log", O_RDWR | O_CREAT | O_SYNC, S_IRUSR | S_IWUSR);
//         if (journalFileDescriptor == -1) {
//             std::cerr << "Failed to open 'journal_log'" << std::endl;
//             // Handle the error, e.g., close the 'block_store' file descriptor and exit.
//             close(fileDescriptor);
//             exit(1);
//         }
//         std::thread journalFlushThread;
//         journalFlushThread = std::thread(&FileExchangeImpl::FlushJournalPeriodically, this);
        
//     }
// }

 FileExchangeImpl(bool journalOnAll, bool enableJournal, int groupCommit) {
        fileDescriptor = open("block_store", O_RDWR | O_CREAT | :wq, S_IRUSR | S_IWUSR);
        if (fileDescriptor == -1) {
            std::cerr << "Failed to open file 'block_store'" << std::endl;
            exit(1);
        }
        this->journalOnAll = journalOnAll;
        this->enableJournal = enableJournal;
        this->groupCommit = groupCommit;
        if (enableJournal) {
            journalFileDescriptor = open("journal_log", O_RDWR | O_CREAT | O_SYNC, S_IRUSR | S_IWUSR);
            if (journalFileDescriptor == -1) {
                std::cerr << "Failed to open 'journal_log'" << std::endl;

                close(journalFileDescriptor);
                exit(1);
            }
    
            std::thread journalFlushThread([this]() {
                try {
                    FlushJournalPeriodically();
                } catch (const std::exception& ex) {
            
                    std::cerr << "Exception in thread: " << ex.what() << std::endl;
                }
            });
            journalFlushThread.detach(); 
        }
    }


    Status PutFile(
      ServerContext* context, ServerReader<FileContent>* reader,
      FileId* summary) override
    {
        FileContent contentPart;
        SequentialFileWriter writer;
        while (reader->Read(&contentPart)) {
            try {
                writer.OpenIfNecessary(contentPart.name());
                auto* const data = contentPart.mutable_content();
                writer.Write(*data);

                summary->set_id(contentPart.id());
                // FIXME: Protect from concurrent access by multiple threads
                m_FileIdToName[contentPart.id()] = contentPart.name();
            }
            catch (const std::system_error& ex) {
                const auto status_code = writer.NoSpaceLeft() ? StatusCode::RESOURCE_EXHAUSTED : StatusCode::ABORTED;
                return Status(status_code, ex.what());
            }
        }

        return Status::OK;
    }


Status Put(
        grpc::ServerContext* context,
        const OffsetData* request,
        success_failure* response
    ) override {
        const int offset = request->offset();
        const std::string& data = request->data();
    //   std::cout<<"putss req";
        if (offset < 0) {
            return grpc::Status(grpc::INVALID_ARGUMENT, "Invalid offset");
        }

        std::lock_guard<std::mutex> lock(mutex);
        std::string journalString =  std::to_string(offset) + " " + data;

        //write to journal
        ssize_t JournalbytesWritten = write(journalFileDescriptor, journalString.c_str(), journalString.size());
        //flush  journal for every write, else thread will tc
        if (journalOnAll) {
            FILE* journalFile = fdopen(journalFileDescriptor, "w");
            if (journalFile != nullptr) {
                fflush(journalFile);
            } 
        }
        //make changes
        off_t position = lseek(fileDescriptor, offset*1024, SEEK_SET);
        if (position == -1) {
            return grpc::Status(grpc::INTERNAL, "Failed to set file position");
        }
        ssize_t bytesWritten = write(fileDescriptor, data.c_str(), data.size());
        if (bytesWritten == -1) {
            return grpc::Status(grpc::INTERNAL, "Failed to write data to the file");
        }
        FILE* dataFile = fdopen(fileDescriptor, "w");
            if (dataFile != nullptr) {
                fflush(dataFile);
            }

        response->set_id(offset); 

        return grpc::Status::OK;
    }



    Status GetFileContent(
        ServerContext* context,
        const FileId* request,
        ServerWriter<FileContent>* writer) override
    {
        const auto id = request->id();
        const auto it = m_FileIdToName.find(id);
        if (m_FileIdToName.end() == it) {
            return Status(grpc::StatusCode::NOT_FOUND, "No file with the id " + std::to_string(id));
        }
        const std::string filename = it->second;

        try {
            FileReaderIntoStream< ServerWriter<FileContent> > reader(filename, id, *writer);

            // TODO: Make the chunk size configurable
            const size_t chunk_size = 1UL << 20;    // Hardcoded to 1MB, which seems to be recommended from experience.
            reader.Read(chunk_size);
        }
        catch (const std::exception& ex) {
            std::ostringstream sts;
            sts << "Error sending the file " << filename << ": " << ex.what();
            std::cerr << sts.str() << std::endl;
            return Status(StatusCode::ABORTED, sts.str());
        }

        return Status::OK;
    }

private:
    std::map<FileIdKey, std::string> m_FileIdToName;

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

   void FlushJournalPeriodically() {
        while (true) {
            // Sleep for a specified interval
            std::this_thread::sleep_for(std::chrono::seconds(2));

            // Flush the journal
            if (journalFileDescriptor != -1) {
                FILE* journalFile = fdopen(journalFileDescriptor, "w");
                if (journalFile != nullptr) {
                    fflush(journalFile);
                }
            }
            //  long tailOffset = ftell(journalFile);
            // writeLSNToFile(tailOffset)
        }
    }

};


void RunServer() {
    // TODO: Allow the port to be customised
  std::string server_address("0.0.0.0:50051");
 
 
    boost::property_tree::ptree config;
    try {
        boost::property_tree::read_json("server_config.json", config);
    } catch (const std::exception& e) {
        std::cerr << "Error reading config file: " << e.what() << std::endl;
        return exit(1);
    }

    std::string serverAddress = config.get<std::string>("server_address");
    bool journalOnAll = config.get<bool>("journalOnAll");
    bool enableJournal = config.get<bool>("enableJournal");
    int32_t groupCommit = config.get<int>("groupCommit");
// FileExchangeImpl service;
  FileExchangeImpl service(journalOnAll, enableJournal, groupCommit);;

  ServerBuilder builder;
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  builder.RegisterService(&service);
  std::unique_ptr<Server> server(builder.BuildAndStart());
    std::cout << "Server listening on " << server_address << ". Press Ctrl-C to end." << std::endl;
  server->Wait();
}

int main(int argc, char** argv)
{
    RunServer();
    return 0;
}
