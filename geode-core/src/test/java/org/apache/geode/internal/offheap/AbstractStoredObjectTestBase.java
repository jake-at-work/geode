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
package org.apache.geode.internal.offheap;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.DataOutput;
import java.io.IOException;

import org.junit.Test;

import org.apache.geode.internal.serialization.DataSerializableFixedID;

public abstract class AbstractStoredObjectTestBase {

  /* Returns Value as an Object Eg: Integer or UserDefinedRegionValue */
  protected abstract Object getValue();

  /* Returns Value as an ByteArray (not serialized) */
  protected abstract byte[] getValueAsByteArray();

  protected abstract Object convertByteArrayToObject(byte[] valueInByteArray);

  protected abstract Object convertSerializedByteArrayToObject(byte[] valueInSerializedByteArray);

  protected abstract StoredObject createValueAsUnserializedStoredObject(Object value);

  protected abstract StoredObject createValueAsSerializedStoredObject(Object value);

  @Test
  public void getValueAsDeserializedHeapObjectShouldReturnDeserializedValueIfValueIsSerialized() {
    var regionEntryValue = getValue();
    var storedObject = createValueAsSerializedStoredObject(regionEntryValue);

    var actualRegionEntryValue = storedObject.getValueAsDeserializedHeapObject();
    assertEquals(regionEntryValue, actualRegionEntryValue);
  }

  @Test
  public void getValueAsDeserializedHeapObjectShouldReturnValueAsIsIfNotSerialized() {
    var regionEntryValue = getValueAsByteArray();
    var storedObject = createValueAsUnserializedStoredObject(regionEntryValue);

    var deserializedValue = (byte[]) storedObject.getValueAsDeserializedHeapObject();
    assertArrayEquals(regionEntryValue, deserializedValue);
  }

  @Test
  public void getValueAsHeapByteArrayShouldReturnSerializedByteArrayIfValueIsSerialized() {
    var regionEntryValue = getValue();
    var storedObject = createValueAsSerializedStoredObject(regionEntryValue);

    var valueInSerializedByteArray = storedObject.getValueAsHeapByteArray();
    var actualRegionEntryValue = convertSerializedByteArrayToObject(valueInSerializedByteArray);

    assertEquals(regionEntryValue, actualRegionEntryValue);
  }

  @Test
  public void getValueAsHeapByteArrayShouldReturnDeserializedByteArrayIfValueIsNotSerialized() {
    var regionEntryValue = getValue();

    var storedObject = createValueAsUnserializedStoredObject(regionEntryValue);

    var valueInByteArray = storedObject.getValueAsHeapByteArray();

    var actualRegionEntryValue = convertByteArrayToObject(valueInByteArray);

    assertEquals(regionEntryValue, actualRegionEntryValue);
  }

  @Test
  public void getStringFormShouldReturnStringFromDeserializedValue() {
    var regionEntryValue = getValue();
    var storedObject = createValueAsSerializedStoredObject(regionEntryValue);

    var stringForm = storedObject.getStringForm();
    assertEquals(String.valueOf(regionEntryValue), stringForm);
  }

  @Test
  public void getValueShouldReturnSerializedValue() {
    var regionEntryValue = getValue();
    var storedObject = createValueAsSerializedStoredObject(regionEntryValue);

    var valueAsSerializedByteArray = (byte[]) storedObject.getValue();

    var actualValue = convertSerializedByteArrayToObject(valueAsSerializedByteArray);

    assertEquals(regionEntryValue, actualValue);
  }

  @Test(expected = IllegalStateException.class)
  public void getValueShouldThrowExceptionIfValueIsNotSerialized() {
    var regionEntryValue = getValue();
    var storedObject = createValueAsUnserializedStoredObject(regionEntryValue);

    var deserializedValue = (byte[]) storedObject.getValue();
  }

  @Test
  public void getDeserializedWritableCopyShouldReturnDeserializedValue() {
    var regionEntryValue = getValueAsByteArray();
    var storedObject = createValueAsSerializedStoredObject(regionEntryValue);

    assertArrayEquals(regionEntryValue,
        (byte[]) storedObject.getDeserializedWritableCopy(null, null));
  }

  @Test
  public void writeValueAsByteArrayWritesToProvidedDataOutput() throws IOException {
    var regionEntryValue = getValueAsByteArray();
    var storedObject = createValueAsSerializedStoredObject(regionEntryValue);

    var dataOutput = mock(DataOutput.class);
    storedObject.writeValueAsByteArray(dataOutput);

    verify(dataOutput, times(1)).write(storedObject.getSerializedValue(), 0,
        storedObject.getSerializedValue().length);
  }

  @Test
  public void sendToShouldWriteSerializedValueToDataOutput() throws IOException {
    var regionEntryValue = getValue();
    var storedObject = createValueAsSerializedStoredObject(regionEntryValue);

    var dataOutput = mock(DataOutput.class);
    storedObject.sendTo(dataOutput);

    verify(dataOutput, times(1)).write(storedObject.getSerializedValue());
  }

  @Test
  public void sendToShouldWriteDeserializedObjectToDataOutput() throws IOException {
    var regionEntryValue = getValueAsByteArray();
    var storedObject = createValueAsUnserializedStoredObject(regionEntryValue);

    var dataOutput = mock(DataOutput.class);
    storedObject.sendTo(dataOutput);

    verify(dataOutput, times(1)).write(regionEntryValue, 0, regionEntryValue.length);
  }

  @Test
  public void sendAsByteArrayShouldWriteSerializedValueToDataOutput() throws IOException {
    var regionEntryValue = getValue();
    var storedObject = createValueAsSerializedStoredObject(regionEntryValue);

    var dataOutput = mock(DataOutput.class);
    storedObject.sendAsByteArray(dataOutput);

    verify(dataOutput, times(1)).write(storedObject.getSerializedValue(), 0,
        storedObject.getSerializedValue().length);
  }

  @Test
  public void sendAsByteArrayShouldWriteDeserializedObjectToDataOutput() throws IOException {
    var regionEntryValue = getValueAsByteArray();
    var storedObject = createValueAsUnserializedStoredObject(regionEntryValue);

    var dataOutput = mock(DataOutput.class);
    storedObject.sendAsByteArray(dataOutput);

    verify(dataOutput, times(1)).write(regionEntryValue, 0, regionEntryValue.length);
  }

  @Test
  public void sendAsCachedDeserializableShouldWriteSerializedValueToDataOutputAndSetsHeader()
      throws IOException {
    var regionEntryValue = getValue();
    var storedObject = createValueAsSerializedStoredObject(regionEntryValue);

    var dataOutput = mock(DataOutput.class);
    storedObject.sendAsCachedDeserializable(dataOutput);

    verify(dataOutput, times(1)).writeByte((DataSerializableFixedID.VM_CACHED_DESERIALIZABLE));
    verify(dataOutput, times(1)).write(storedObject.getSerializedValue(), 0,
        storedObject.getSerializedValue().length);
  }

  @Test(expected = IllegalStateException.class)
  public void sendAsCachedDeserializableShouldThrowExceptionIfValueIsNotSerialized()
      throws IOException {
    var regionEntryValue = getValue();
    var storedObject = createValueAsUnserializedStoredObject(regionEntryValue);

    var dataOutput = mock(DataOutput.class);
    storedObject.sendAsCachedDeserializable(dataOutput);
  }
}
