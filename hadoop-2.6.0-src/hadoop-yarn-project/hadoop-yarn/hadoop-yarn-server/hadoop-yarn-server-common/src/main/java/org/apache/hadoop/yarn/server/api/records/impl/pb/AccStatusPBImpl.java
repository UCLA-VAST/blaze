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


import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.yarn.proto.YarnServerCommonProtos.AccStatusProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonProtos.AccStatusProtoOrBuilder;
import org.apache.hadoop.yarn.server.api.records.AccStatus;

public class AccStatusPBImpl extends AccStatus {
  AccStatusProto proto = AccStatusProto.getDefaultInstance();
  AccStatusProto.Builder builder = null;
  boolean viaProto = false;
  
  private boolean alive = false;
  private boolean isUpdated = false;
  private List<String> accNames = null;
  
  public AccStatusPBImpl() {
    builder = AccStatusProto.newBuilder();
  }

  public AccStatusPBImpl(AccStatusProto proto) {
    this.proto = proto;
    viaProto = true;
  }
  
  public synchronized AccStatusProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private synchronized void mergeLocalToBuilder() {
    if (this.accNames != null) {
      builder.clearAccNames();
      builder.addAllAccNames(this.accNames);
    }
  }

  private synchronized void mergeLocalToProto() {
    if (viaProto) 
      maybeInitBuilder();
    mergeLocalToBuilder();
    proto = builder.build();
    viaProto = true;
  }

  private synchronized void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = AccStatusProto.newBuilder(proto);
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
  public synchronized boolean getAlive() {
    AccStatusProtoOrBuilder p = viaProto ? proto : builder;
    return p.getAlive();
  }
  @Override
  public synchronized void setAlive(boolean alive) {
    maybeInitBuilder();
    builder.setAlive(alive);
  }
  @Override
  public synchronized boolean getIsUpdated() {
    AccStatusProtoOrBuilder p = viaProto ? proto : builder;
    return p.getIsUpdated();
  }
  @Override
  public synchronized void setIsUpdated(boolean isUpdated) {
    maybeInitBuilder();
    builder.setIsUpdated(isUpdated);
  }
  @Override
  public List<String> getAccNames() {
    initAccNames();
    return this.accNames;
  }
  @Override
  public void setAccNames(List<String> accNames) {
    maybeInitBuilder();
    builder.clearAccNames();
    this.accNames = accNames;
  }

  private void initAccNames() {
    if (this.accNames != null) {
      return;
    }
    AccStatusProtoOrBuilder p = viaProto ? proto : builder;
    this.accNames = new ArrayList<String>();
    this.accNames.addAll(p.getAccNamesList());
  }
}
