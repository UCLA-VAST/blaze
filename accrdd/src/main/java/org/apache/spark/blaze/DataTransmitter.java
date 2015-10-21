/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.blaze;

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
* DataTransmitter is in charge of maintaining the connection between Spark and Accelerator manager.
* DataTransmitter is used for building the connection, send and receive the data between Spark
* and Accelerator manager.
**/
public class DataTransmitter {
	Socket acc_socket = null;

	/**
		* Default setting of DataTransmitter: connect to localhost.
		*/
	public DataTransmitter() {
		init("127.0.0.1", 1027);
	}

	/**
		* User specified host name and port.
		*/
	public DataTransmitter(String hostname, int port) {
		init(hostname, port);
	}

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
	public void init(String hostname, int port) {
		try {
	  	acc_socket = new Socket(hostname, port); 
      // turn off Negal algorithm to improve latency
      acc_socket.setTcpNoDelay(true);
		}
		catch (Exception e) {
			; // do nothing
		}
		finally {
			; // do nothing
		}
	}

	/**
	* Check connection between Manager.
	* Check if the socket is built with Node Manager.
	*
	* @return 
	*		True if the connection is built successfully.
	**/
	public boolean isConnect() {
		if (acc_socket == null)
			return false;
		else
			return true;
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
	public void send(AccMessage.TaskMsg.Builder msgBuilder)
    throws IOException {

		AccMessage.TaskMsg msg = msgBuilder.build();
    int msg_size = msg.getSerializedSize();

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
  public AccMessage.TaskMsg receive()
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
	* Create an empty message with specific type.
	*
	* @param acc_id
	*		The accelerator ID. Should be provided by accelerator library.
	* @param type
	*		The message type.
	* @see AccMessage.MsgType
	**/
	public static AccMessage.TaskMsg.Builder buildMessage(String acc_id, String appId, AccMessage.MsgType type) {
		AccMessage.TaskMsg.Builder msg = AccMessage.TaskMsg.newBuilder()
			.setType(type);

		if (acc_id != null)
			msg.setAccId(acc_id);

		if (appId != null)
			msg.setAppId(appId);

		return msg;
	}

	/**
	* Create an empty message with specific type.
	*
	* @param appId
	* 	Application ID given by Spark or resource manager.
	* @param type
	*		The message type.
	* @see AccMessage.MsgType
	**/
	public static AccMessage.TaskMsg.Builder buildMessage(String appId, AccMessage.MsgType type) {
		return buildMessage(null, appId, type);
	}

	/**
	* Add a data block.
	* Create and add a data block to assigned message with full information. 
	*
	*	@param msg The message that wanted to be added.
	* @param blockInfo The object contains necessary information of the block.
	* @param isSampled Indicate if the block is sampled or not.
	* @param maskPath The path of mask memory mapped file.
	**/
	public static void addData(
		AccMessage.TaskMsg.Builder msg, 
		BlockInfo blockInfo,
		boolean isSampled,
		String maskPath
	) {
		AccMessage.DataMsg.Builder data = AccMessage.DataMsg.newBuilder()
			.setPartitionId(blockInfo.id())
			.setFileOffset(blockInfo.offset());

		if (blockInfo.numElt() != -1) {
			data.setNumElements(blockInfo.numElt());
			data.setElementSize(blockInfo.eltSize());
			data.setElementLength(blockInfo.eltLength());
		}
		else
			data.setFileSize(blockInfo.fileSize());

		if (blockInfo.fileName() != null)
			data.setFilePath(blockInfo.fileName());

		if (isSampled)
			data.setSampled(true);

		if (maskPath != null)
			data.setMaskPath(maskPath);

		msg.addData(data);
		return ;
	}

	/**
	* Add a scalar data block.
	* Create and add a data block with a scalar data. This is only used for 
	* broadcasting scalar variables.
	*
	*	@param msg The message that wanted to be added.
	* @param value The value of the scalar variable.
	**/
	public static void addScalarData(AccMessage.TaskMsg.Builder msg, long value) {
		AccMessage.DataMsg.Builder data = AccMessage.DataMsg.newBuilder()
			.setNumElements(1)
			.setElementLength(1)
			.setElementSize(4)
			.setScalarValue(value);

		msg.addData(data);
		return ;
	}

	/**
	* Add a broadcast block with simple information.
	* Create and add a broadcast block with broadcast ID. This is only used for 
	* indicating the Manager about which broadcast data block should be used.
	*
	* '''Note''' The broadcast data block must be transmitted to the Manager in advance.
	*
	*	@param msg The message that wanted to be added.
	* @param id The unique ID of the data block.
	**/
	public static void addBroadcastData(AccMessage.TaskMsg.Builder msg, long id) {
		AccMessage.DataMsg.Builder data = AccMessage.DataMsg.newBuilder()
			.setPartitionId(id);

		msg.addData(data);
		return ;
	}
}
