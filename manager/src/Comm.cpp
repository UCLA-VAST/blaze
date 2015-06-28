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

#define READ_MAPPED_FILE(cType, input, file, size, length) { \
	input = (void *)malloc(sizeof(cType) * size); \
	memcpy((void *) input, (const void *) file, sizeof(cType) * length); }

#define FREE_DATA(input) \
	free(input);

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

void Comm::init()
{
	// The data type size in Java
	Type2Size["int"] = 4;
	Type2Size["float"] = 4;
	Type2Size["long"] = 8;
	Type2Size["double"] = 8;

	return ;
}

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
	#define cType double

  // This may not be the best available method
  ip::tcp::iostream socket_stream;
  socket_stream.rdbuf()->assign( ip::tcp::v4(), sock->native());

  logInfo("Comm:process(): Start processing a new connection.");

  TaskMsg task_msg;

  try {
    recv(task_msg, socket_stream);
  } catch (std::runtime_error &e) {
    printf("Comm:process() error: %s.\n", e.what());
    return;
  }

  if (task_msg.type() == ACCREQUEST) {

    //printf("Comm:listen(): Received an ACCREQUEST message.\n");
    logInfo(std::string("Comm:process(): Received an ACCREQUEST message.").c_str());

    // TODO: calculate scheduling decision
    // here assuming always accept

    // TODO: also consult cache manager to see if data is cached

    // start a new thread to process the subsequent messages
    // socket_stream should be copied
    //process(socket_stream);

    TaskMsg accept_msg;
    accept_msg.set_type(ACCGRANT);
    accept_msg.set_get_data(1);

    // send msg back to client
    send(accept_msg, socket_stream);

    logInfo(std::string("Comm:process(): Replied with an ACCGRANT message.").c_str());

    // wait for data
    TaskMsg data_msg;

    try {
      recv(data_msg, socket_stream);
    } catch (std::runtime_error &e) {
      printf("Comm:process() error: %s.\n", e.what());
      return;
    }

		if (data_msg.type() == ACCDATA) {
	    // task execution
			int fd = open(data_msg.data().path().c_str(), O_RDWR, S_IRUSR | S_IWUSR);
			void *memory_file = mmap(0, data_msg.data().size(), PROT_READ | PROT_WRITE,
					MAP_SHARED, fd, 0);
			close(fd);
	
			int dataSize = data_msg.data().size();
			std::string dataType = data_msg.data().data_type();
			int dataLength = dataSize / Type2Size[dataType];
			void *in;
			printf("Reading %d data...", dataLength);

			if (dataType == "int") {
				READ_MAPPED_FILE(int, in, memory_file, dataSize, dataLength);
			}
			else if (dataType == "float")	{
				READ_MAPPED_FILE(float, in, memory_file, dataSize, dataLength);
			}
			else if (dataType == "long") {
				READ_MAPPED_FILE(long, in, memory_file, dataSize, dataLength);
			}
			else if (dataType == "double") {
				READ_MAPPED_FILE(double, in, memory_file, dataSize, dataLength);
			}

			// comaniac: only for testing, print first 10 values
			for (int i = 0; i < 10; ++i) {
				printf("%.2f\n", (double) *((double *) in + i));
				memory_file += Type2Size["double"];
			}

			FREE_DATA(in);
		}

    TaskMsg finish_msg;
    finish_msg.set_type(ACCFINISH);

    send(finish_msg, socket_stream);
    logInfo(std::string("Comm:process(): Sent an ACCFINISH message.").c_str());
  }
  else {
    printf("Comm:process() error: Unknown message type, discarding message.\n");
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

