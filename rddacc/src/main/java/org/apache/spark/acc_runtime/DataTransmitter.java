package org.apache.spark.acc_runtime;

import java.lang.InterruptedException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.io.*;
import java.util.*;
import java.net.Socket;
import java.net.SocketException;
import java.lang.System;

/**
* The class is in charge of maintaining the connection between Spark and Accelerator manager.
* DataTransmitter is used for building the connection, send and receive the data between Spark
* and Accelerator manager.
**/
public class DataTransmitter {
	static Socket acc_socket = null; // FIXME: static?

	/**
	* Initialize connection.
	* Initialize the connection. This method should be called only once to avoid 
	* duplicated connections.
	*
	* @param hostname 
	*		The hostname of the Accelerator manager.
	* @param port
	*		The port of the Accelerator manager.
	**/
	public static void init(String hostname, int port) {
		try {
      acc_socket = new Socket(hostname, port); 
    } catch (Exception e) {
      e.printStackTrace(System.err);
    }
	}

	/**
	* Send message to Accelerator manager.
	* This method also builds the connection when it is called first time by launching 
	* method init.
	*
	* @param msg
	*		The message to be sent.
	*	@see init(String hostname, int port)
	**/
	public static void send(AccMessage.TaskMsg msg)
    throws IOException {

    int msg_size = msg.getSerializedSize();

		if (acc_socket == null)
			init("127.0.0.1", 1027);

    // send byte size
    DataOutputStream sout = new DataOutputStream(acc_socket.getOutputStream());
    sout.write(ByteBuffer.allocate(4)
        .order(ByteOrder.nativeOrder())
        .putInt(msg_size).array(),0,4);
    sout.flush();

    // send message data
    msg.writeTo(sout);
    sout.flush();
  }

	/**
	* Receive the message from Accelerator manager.
	**/
  public static AccMessage.TaskMsg receive()
    throws IOException {

    DataInputStream sin = new DataInputStream(acc_socket.getInputStream());

    int msg_size = ByteBuffer.allocate(4)
      .putInt(sin.readInt())
      .order(ByteOrder.nativeOrder())
      .getInt(0);

    byte[] msg_data = ByteBuffer.allocate(msg_size).array();

    sin.read(msg_data, 0, msg_size);

    return AccMessage.TaskMsg.parseFrom(msg_data);
  }

	/**
	* Create a task message for requesting.
	**/
	public static AccMessage.TaskMsg createTaskMsg(int idx, AccMessage.MsgType type) {
		AccMessage.TaskMsg msg = AccMessage.TaskMsg.newBuilder()
			.setType(type)
			.setAccId("request" + idx)
			.build();
		
		return msg;
	}

	/**
	* Create a data message.
	**/
	public static AccMessage.TaskMsg createDataMsg(int id, String dataType, int size, String path) {
		AccMessage.Data data = AccMessage.Data.newBuilder()
			.setPartitionId(id)
			.setDataType(dataType)
			.setSize(size)
			.setPath(path)
			.build();
		
		AccMessage.TaskMsg msg = AccMessage.TaskMsg.newBuilder()
			.setType(AccMessage.MsgType.ACCDATA)
			.setData(data)
			.build();

		return msg;
	}

	/**
	* Main method is used for testing only.
	**/
  public static void main (String[] args) {

    try {
      // send an ACCREQUEST
      AccMessage.TaskMsg task_msg = 
        AccMessage.TaskMsg.newBuilder()
        .setType(AccMessage.MsgType.ACCREQUEST)
        .setAccId("example")
        .build();

      send(task_msg);

      System.out.println("sent an ACCREQUEST message.");

      // wait for a reply
      // - byte size first
      AccMessage.TaskMsg result_msg = receive();

      if (result_msg.getType() == AccMessage.MsgType.ACCGRANT) {
        System.out.println("received an ACCGRANT message.");
      }
      else {
        System.out.println("invalid message.");
        System.exit(1);
      }

      // open a memory mapped file 
      

      // build a ACCDATA message
      AccMessage.TaskMsg data_msg = 
        AccMessage.TaskMsg.newBuilder()
        .setType(AccMessage.MsgType.ACCDATA)
        .build();

      // send
      send(data_msg);

      task_msg = receive();
      if (task_msg.getType() == AccMessage.MsgType.ACCFINISH) {
        System.out.println("Task finished.");
      }
      else {
        System.out.println("Task failed.");
        System.exit(1);
      }
    } catch (Exception e) {
      e.printStackTrace(System.err);
      System.exit(1);
    }

    System.exit(0);
  }
}
