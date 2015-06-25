import java.lang.InterruptedException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.io.*;
import java.util.*;
import java.net.Socket;
import java.net.SocketException;
import java.lang.System;

import ucla.vast.acc_runtime.AccMessage;

public final class Task {

  public static void send(AccMessage.TaskMsg msg, Socket s)
    throws IOException {

    int msg_size = msg.getSerializedSize();

    // send byte size
    DataOutputStream sout = new DataOutputStream(s.getOutputStream());
    sout.write(ByteBuffer.allocate(4)
        .order(ByteOrder.nativeOrder())
        .putInt(msg_size).array(),0,4);
    sout.flush();

    // send message data
    msg.writeTo(sout);
    sout.flush();
  }

  public static AccMessage.TaskMsg recv(Socket s)
    throws IOException {

    DataInputStream sin = new DataInputStream(s.getInputStream());

    int msg_size = ByteBuffer.allocate(4)
      .putInt(sin.readInt())
      .order(ByteOrder.nativeOrder())
      .getInt(0);

    byte[] msg_data = ByteBuffer.allocate(msg_size).array();

    sin.read(msg_data, 0, msg_size);

    return AccMessage.TaskMsg.parseFrom(msg_data);
  }

  public static void main (String[] args) {

    if (args.length < 1) {
      System.err.println("Usage: Task <host> <port>");
      System.exit(1);
    }
    String hostname = args[0];
    int port = Integer.parseInt(args[1]);

    try {
      Socket acc_socket = new Socket(hostname, port); 

      // send an ACCREQUEST
      AccMessage.TaskMsg task_msg = 
        AccMessage.TaskMsg.newBuilder()
        .setType(AccMessage.MsgType.ACCREQUEST)
        .setAccId("example")
        .build();

      send(task_msg, acc_socket);

      System.out.println("sent an ACCREQUEST message.");

      // wait for a reply
      // - byte size first
      AccMessage.TaskMsg result_msg = recv(acc_socket);

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
      send(data_msg, acc_socket);

      task_msg = recv(acc_socket);
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
