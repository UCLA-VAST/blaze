/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.api.records.impl.pb;

import org.apache.hadoop.yarn.proto.YarnServerCommonProtos.AcceleratorProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonProtos.AcceleratorProtoOrBuilder;
import org.apache.hadoop.yarn.server.api.records.Accelerator;

public class AcceleratorPBImpl extends Accelerator {
  AcceleratorProto proto = AcceleratorProto.getDefaultInstance();
  AcceleratorProto.Builder builder = null;
  boolean viaProto = false;
  
  private String accName = "";
  private String deviceName = "";
  
  public AcceleratorPBImpl() {
    builder = AcceleratorProto.newBuilder();
  }

  public AcceleratorPBImpl(AcceleratorProto proto) {
    this.proto = proto;
    viaProto = true;
  }
  
  public synchronized AcceleratorProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private synchronized void mergeLocalToProto() {
    if (viaProto) 
      maybeInitBuilder();
    proto = builder.build();
    viaProto = true;
  }

  private synchronized void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = AcceleratorProto.newBuilder(proto);
    }
    viaProto = false;
  }
    
  @Override
  public int hashCode() {
    return getProto().hashCode();
  }
  
  @Override
  public boolean equals(Object other) {
    if (other == null)
      return false;
    if (other.getClass().isAssignableFrom(this.getClass())) {
      return this.getProto().equals(this.getClass().cast(other).getProto());
    }
    return false;
  }

  @Override
  public synchronized String getAccName() {
    AcceleratorProtoOrBuilder p = viaProto ? proto : builder;
    return p.getAccName();
  }

  @Override
  public synchronized String getDeviceName() {
    AcceleratorProtoOrBuilder p = viaProto ? proto : builder;
    return p.getDeviceName();
  }

  @Override
  public synchronized void setAccName(String accName) {
    maybeInitBuilder();
    builder.setAccName(accName);
  }

  @Override
  public synchronized void setDeviceName(String deviceName) {
    maybeInitBuilder();
    builder.setDeviceName(deviceName);
  }
}
