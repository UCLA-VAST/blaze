
package org.apache.hadoop.fcs;

import java.io.IOException;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.net.Socket;
import java.net.InetAddress;


public class SocketConnector {
  private final String ip;
  private final int port;
  private Socket socket;
  private DataOutputStream sout;
  private DataInputStream sin;

  public SocketConnector(String ip, int port) {
    this.ip = ip;
    this.port = port;
  }

  public void buildConnection() throws IOException {
    InetAddress addr = InetAddress.getByName(ip); 
    socket = new Socket(addr, port);
    sout = new DataOutputStream(socket.getOutputStream());
    sin = new DataInputStream(socket.getInputStream());
    return;
  }

  public void send(int i) throws IOException {
    sout.write(
        ByteBuffer.allocate(4)
        .order(ByteOrder.nativeOrder())
        .putInt(i)
        .array(),0,4);
    sout.flush();
  }

  public void send(byte[] array) throws IOException {
    sout.write(array, 0, array.length);
    sout.flush();
  }

  public int receive() throws IOException {
    return ByteBuffer.allocate(4)
      .putInt(sin.readInt())
      .order(ByteOrder.nativeOrder())
      .getInt(0);
  }

  public byte[] receive(int len) throws IOException {
    byte[] result = new byte[len];
    sin.readFully(result);
    return result;
  }

  public void closeConnection( ) throws IOException {
    sout.close();
    sin.close();
    socket.close();
  }
}
