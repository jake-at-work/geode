/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.geode.redis.internal.services.cluster;

import static org.apache.geode.redis.internal.RedisConstants.MEMBER_INFO_DATA_SERIALIZABLE_ID;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.Objects;

import org.apache.geode.DataSerializable;
import org.apache.geode.DataSerializer;
import org.apache.geode.Instantiator;
import org.apache.geode.distributed.DistributedMember;

public class RedisMemberInfo implements DataSerializable, Serializable {

  static {
    Instantiator
        .register(new Instantiator(RedisMemberInfo.class, MEMBER_INFO_DATA_SERIALIZABLE_ID) {
          public DataSerializable newInstance() {
            return new RedisMemberInfo();
          }
        });
  }

  private static final long serialVersionUID = -10228877687322470L;

  private DistributedMember member;
  private String hostAddress;
  private int redisPort;

  // For serialization
  private RedisMemberInfo() {}

  public RedisMemberInfo(DistributedMember member, String hostAddress, int redisPort) {
    this.member = member;
    this.hostAddress = hostAddress;
    this.redisPort = redisPort;
  }

  public DistributedMember getMember() {
    return member;
  }

  public String getHostAddress() {
    return hostAddress;
  }

  public int getRedisPort() {
    return redisPort;
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    DataSerializer.writeObject(member, out);
    DataSerializer.writeString(hostAddress, out);
    out.writeInt(redisPort);
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    member = DataSerializer.readObject(in);
    hostAddress = DataSerializer.readString(in);
    redisPort = DataSerializer.readPrimitiveInt(in);
  }

  @Override
  public String toString() {
    return member.toString() + " hostAddress: " + hostAddress + " redisPort: " + redisPort;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof RedisMemberInfo)) {
      return false;
    }
    var that = (RedisMemberInfo) o;
    return redisPort == that.redisPort && Objects.equals(member, that.member)
        && Objects.equals(hostAddress, that.hostAddress);
  }

  @Override
  public int hashCode() {
    return Objects.hash(member, hostAddress, redisPort);
  }
}
