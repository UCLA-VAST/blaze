#include <stdio.h>
#include <sys/time.h>
#include <time.h>
#include <iostream>
#include <stdexcept>
#include <fcntl.h>   
#include <sys/mman.h>
#include <sys/stat.h>

#include "Comm.h"

#define MAX_MSGSIZE 4096
#define OUTPUT_DIR "/tmp"

void logInfo(const char *msg) {
  struct timespec tr;
  clock_gettime(CLOCK_REALTIME, &tr);
  struct tm *l_time = localtime(&tr.tv_sec);
  char t_str[100];
  strftime(t_str, sizeof(t_str), "%Y-%m-%d %H:%M:%S", l_time);

  int tid = 0;
  std::string tid_str = boost::lexical_cast<std::string>(boost::this_thread::get_id());
  sscanf(tid_str.c_str(), "%lx", &tid);
  fprintf(stdout, "[%s,%d:t%x] %s\n", 
          t_str, tr.tv_nsec/1000000, 
          tid, msg);
}

namespace acc_runtime {

// receive one message, bytesize first
void Comm::recv(
    TaskMsg &task_msg, 
    ip::tcp::iostream &socket_stream) 
{
  int msg_size = 0;

  //TODO: why doesn't this work: socket_stream >> msg_size;
  socket_stream.read(reinterpret_cast<char*>(&msg_size), sizeof(int));

  if (msg_size<=0) {
    throw std::runtime_error("Invalid message size");
  }
  char* msg_data = new char[msg_size];
  socket_stream.read(msg_data, msg_size);

  if (!task_msg.ParseFromArray(msg_data, msg_size)) {
    throw std::runtime_error("Failed to parse input message");
  }

  delete msg_data;
}

// send one message, bytesize first
void Comm::send(
    TaskMsg &task_msg, 
    ip::tcp::iostream &socket_stream) 
{
  int msg_size = task_msg.ByteSize();

  //TODO: why doesn't this work: socket_stream << msg_size;
  socket_stream.write(reinterpret_cast<char*>(&msg_size), sizeof(int));

  task_msg.SerializeToOstream(&socket_stream);
}

void Comm::process(socket_ptr sock) {

  // This may not be the best available method
  ip::tcp::iostream socket_stream;
  socket_stream.rdbuf()->assign( ip::tcp::v4(), sock->native());

  logInfo("Comm:process(): Start processing a new connection.");

  TaskMsg task_msg;

  try {
    recv(task_msg, socket_stream);
  } catch (std::runtime_error &e) {
    fprintf(stderr, "Comm:process() error: %s.\n", e.what());
    return;
  }

  if (task_msg.type() == ACCREQUEST) {

    //printf("Comm:listen(): Received an ACCREQUEST message.\n");
    logInfo(std::string("Comm:process(): Received an ACCREQUEST message.").c_str());

    // TODO: calculate scheduling decision
    // here assuming always accept

    // TODO: also consult cache manager to see if data is cached

    TaskMsg accept_msg;
    accept_msg.set_type(ACCGRANT);

		for(int i = 0; i < task_msg.data_size(); ++i) {
	    Data *block_info = accept_msg.add_data();
			block_info->set_partition_id(task_msg.data(0).partition_id());
			block_info->clear_cached(); // FIXME: Now assume we have no cached data
		}

    // start a new thread to process the subsequent messages
    // socket_stream should be copied
    //process(socket_stream);		

    // send msg back to client
    send(accept_msg, socket_stream);

    logInfo(std::string("Comm:process(): Replied with an ACCGRANT message.").c_str());

    // wait for data
    TaskMsg data_msg;

    try {
      recv(data_msg, socket_stream);
    } catch (std::runtime_error &e) {
      fprintf(stderr, "Comm:process() error: %s.\n", e.what());
      return;
    }

   	TaskMsg finish_msg;
   	finish_msg.set_type(ACCFINISH);

		if (data_msg.type() == ACCDATA) {
			for (int d = 0; d < data_msg.data_size(); ++d) {
				int blockId = data_msg.data(d).partition_id();
				int dataSize = data_msg.data(d).size();
				int dataLength = data_msg.data(d).width();
				void *in = NULL;

				char filePath[256];
				strcpy(filePath, data_msg.data(d).path().c_str());
				//logInfo(std::string("Comm::process(): Read data from " + 
				//	data_msg.data(d).path()).c_str());

				if (dataLength != -1) { // Known length means memory mapped file
					in = (void *)malloc(dataSize);
					int fd = open(filePath, O_RDWR, S_IRUSR | S_IWUSR);
					void *memory_file = mmap(0, data_msg.data(d).size(), PROT_READ | PROT_WRITE,
							MAP_SHARED, fd, 0);
					close(fd);
	
					memcpy((void *) in, (const void *) memory_file, dataSize);
					munmap (memory_file, dataSize);
				}
				else { // Read from file directly
					FILE *infilep = fopen(filePath, "r");
					fseek(infilep, data_msg.data(d).offset(), SEEK_SET);
//					char *buf = (char *)malloc(dataSize);
//					fread(buf, dataSize, 1, infilep);

					in = (void *)malloc(sizeof(double) * 1000);

					// FIXME: Should be done at accelerator}
					dataLength = 1000;
					for (int i = 0; i < dataLength; ++i)
						fscanf(infilep, "%lf", ((double *) in + i));

					fclose(infilep);
//					free(buf);
					fprintf(stderr, "Read data from file, first value: %.2f\n", (double) *((double *) in));
				}

// FIXME: Simulate accelerator
				void *out = (void *)malloc(sizeof(double) * dataLength);
				for (int i = 0; i < dataLength; ++i) {
					*((double *) out + i) = (double) *((double *) in + i) + 1.0;
				}
// Accelerator end

				// Write result
				char out_file_name[128];
				sprintf(out_file_name, "%s/spark_acc%d.out", OUTPUT_DIR, blockId);

				int fd = open(out_file_name, O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
				int pageSize = getpagesize();
				int offset = 0;
				int outFileSize = sizeof(double) * dataLength; // FIXME: data type should vary.
				void *memory_file = NULL;

				// Write one page (usually 4k)
				while ((offset + pageSize) < outFileSize) {
					ftruncate(fd, offset + pageSize);
					memory_file = mmap(0, pageSize, PROT_READ | PROT_WRITE, 
							MAP_SHARED, fd, offset);
					memcpy((void *) memory_file, (const void *) out + offset, pageSize);
					munmap (memory_file, pageSize); 
					offset += pageSize;
				}
				ftruncate(fd, outFileSize);
				memory_file = mmap (0, (outFileSize - offset), PROT_READ | PROT_WRITE,        
							MAP_SHARED, fd, offset);
				memcpy((void *) memory_file, (const void *) out + offset, outFileSize - offset);
				munmap (memory_file, outFileSize - offset);                                   
	
				close(fd);

				// Add file path to finish message
				Data *block_info = finish_msg.add_data();
				block_info->set_partition_id(blockId);
				block_info->set_path(out_file_name);
				block_info->set_width(dataLength);

				free(in);
				free(out);
			}
		}
		else
			fprintf(stderr, "Comm:process() error: Unknown message type, discarding message.\n");

    send(finish_msg, socket_stream);
    logInfo(std::string("Comm:process(): Sent an ACCFINISH message.").c_str());
  }
  else {
    fprintf(stderr, "Comm:process() error: Unknown message type, discarding message.\n");
  }
}

void Comm::listen() {

  io_service ios;

  ip::tcp::endpoint endpoint(
      ip::address::from_string(ip_address),
      srv_port);

  ip::tcp::acceptor acceptor(ios, endpoint);

  while(1) {
    
    // create socket for connection
    socket_ptr sock(new ip::tcp::socket(ios));

    // accept incoming connection
    acceptor.accept(*sock);
    //acceptor.accept(*socket_stream.rdbuf());
    
    logInfo(std::string("Comm:listen(): Accepted a new connection.").c_str());
    
    boost::thread t(boost::bind(&Comm::process, this, sock));
  }
}

}

