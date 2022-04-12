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
package org.apache.geode.internal.cache.tier.sockets;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

import org.apache.geode.DataSerializer;
import org.apache.geode.internal.cache.EnumListenerEvent;
import org.apache.geode.internal.cache.EventID;
import org.apache.geode.internal.cache.tier.MessageType;
import org.apache.geode.internal.serialization.DataSerializableFixedID;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.SerializationContext;

public class ClientDataSerializerMessage extends ClientUpdateMessageImpl {
  private byte[][] serializedDataSerializer;

  private Class<?>[][] supportedClasses;

  public ClientDataSerializerMessage(EnumListenerEvent operation, byte[][] dataSerializer,
      ClientProxyMembershipID memberId, EventID eventIdentifier, Class<?>[][] supportedClasses) {
    super(operation, memberId, eventIdentifier);
    serializedDataSerializer = dataSerializer;
    this.supportedClasses = supportedClasses;
  }

  /**
   * default constructor
   *
   */
  public ClientDataSerializerMessage() {

  }

  @Override
  public boolean shouldBeConflated() {
    return false;
  }

  /**
   * Returns a <code>Message</code> generated from the fields of this
   * <code>ClientDataSerializerMessage</code>.
   *
   * @param latestValue byte[] containing the latest value to use. This could be the original value
   *        if conflation is not enabled, or it could be a conflated value if conflation is enabled.
   * @return a <code>Message</code> generated from the fields of this
   *         <code>ClientDataSerializerMessage</code>
   * @see org.apache.geode.internal.cache.tier.sockets.Message
   */
  @Override
  protected Message getMessage(CacheClientProxy proxy, byte[] latestValue) throws IOException {
    // The format:
    // part 0: serializer1 classname
    // part 1: serializer1 id
    // part 2: serializer1 number of supported classes --|
    // part 3: serializer1 supported class1 name |
    // part 4: serializer1 supported class2 name |---> additional parts since 6.5.1.6
    // part 5: serializer1 supported classN name --|
    // part 6: serializer2 classname
    // part 7: serializer2 id
    // part 8: serializer2 number of supported classes
    // part 9: serializer2 supported class1 name
    // part 10: serializer2 supported classN name
    // ...
    // Last part: event ID

    var dsLength = serializedDataSerializer.length; // multiple of 2
    assert (dsLength % 2) == 0;
    var numOfDS = (supportedClasses != null) ? supportedClasses.length : 0;
    assert (dsLength / 2) == numOfDS;

    // Calculate total number of parts
    var numOfParts = dsLength + numOfDS;
    for (var i = 0; i < numOfDS; i++) {
      if (supportedClasses[i] != null) {
        numOfParts += supportedClasses[i].length;
      }
    }
    numOfParts += 1; // one for eventID

    final var message = new Message(numOfParts, proxy.getVersion());
    // Set message type
    message.setMessageType(MessageType.REGISTER_DATASERIALIZERS);
    for (var i = 0; i < dsLength; i = i + 2) {
      message.addBytesPart(serializedDataSerializer[i]); // part 0
      message.addBytesPart(serializedDataSerializer[i + 1]); // part 1

      var numOfClasses = supportedClasses[i / 2].length;
      var classBytes = new byte[numOfClasses][];
      try {
        for (var j = 0; j < numOfClasses; j++) {
          classBytes[j] = CacheServerHelper.serialize(supportedClasses[i / 2][j].getName());
        }
      } catch (IOException ioe) {
        numOfClasses = 0;
        classBytes = null;
      }
      message.addIntPart(numOfClasses); // part 2
      for (var j = 0; j < numOfClasses; j++) {
        message.addBytesPart(classBytes[j]); // part 3 onwards
      }
    }
    message.setTransactionId(0);
    message.addObjPart(getEventId()); // last part
    return message;
  }

  @Override
  public int getDSFID() {
    return CLIENT_DATASERIALIZER_MESSAGE;
  }

  /**
   * Writes an object to a <code>Datautput</code>.
   *
   * @throws IOException If this serializer cannot write an object to <code>out</code>.
   * @see DataSerializableFixedID#toData(DataOutput, SerializationContext)
   */
  @Override
  public void toData(DataOutput out,
      SerializationContext context) throws IOException {

    out.writeByte(_operation.getEventCode());
    var dataSerializerCount = serializedDataSerializer.length;
    out.writeInt(dataSerializerCount);
    for (final var bytes : serializedDataSerializer) {
      DataSerializer.writeByteArray(bytes, out);
    }
    context.getSerializer().writeObject(_membershipId, out);
    context.getSerializer().writeObject(_eventIdentifier, out);
  }

  /**
   * Reads an object from a <code>DataInput</code>.
   *
   * @throws IOException If this serializer cannot read an object from <code>in</code>.
   * @throws ClassNotFoundException If the class for an object being restored cannot be found.
   * @see DataSerializableFixedID#fromData(DataInput, DeserializationContext)
   */
  @Override
  public void fromData(DataInput in,
      DeserializationContext context) throws IOException, ClassNotFoundException {
    // Note: does not call super.fromData what a HACK
    _operation = EnumListenerEvent.getEnumListenerEvent(in.readByte());
    var dataSerializerCount = in.readInt();
    serializedDataSerializer = new byte[dataSerializerCount][];
    for (var i = 0; i < dataSerializerCount; i++) {
      serializedDataSerializer[i] = DataSerializer.readByteArray(in);
    }
    _membershipId = ClientProxyMembershipID.readCanonicalized(in);
    _eventIdentifier = context.getDeserializer().readObject(in);
  }

  @Override
  public Object getKeyToConflate() {
    return null;
  }

  @Override
  public String getRegionToConflate() {
    return null;
  }

  @Override
  public Object getValueToConflate() {
    return null;
  }

  @Override
  public void setLatestValue(Object value) {}

  @Override
  public boolean isClientInterested(ClientProxyMembershipID clientId) {
    return true;
  }

  @Override
  public boolean needsNoAuthorizationCheck() {
    return true;
  }

  @Override
  public String toString() {
    return "ClientDataSerializerMessage[value="
        + Arrays.deepToString(serializedDataSerializer)
        + ";memberId="
        + getMembershipId() + ";eventId=" + getEventId() + "]";
  }
}
