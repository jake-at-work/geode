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
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.ByteBuffer;

import org.junit.Test;

import org.apache.geode.compression.Compressor;
import org.apache.geode.internal.cache.BytesAndBitsForCompactor;
import org.apache.geode.internal.cache.CachePerfStats;
import org.apache.geode.internal.cache.EntryEventImpl;
import org.apache.geode.internal.cache.RegionEntryContext;
import org.apache.geode.internal.serialization.DSCODE;

public class TinyStoredObjectJUnitTest extends AbstractStoredObjectTestBase {

  @Override
  public Object getValue() {
    return 123456789;
  }

  @Override
  public byte[] getValueAsByteArray() {
    return convertValueToByteArray(getValue());
  }

  private byte[] convertValueToByteArray(Object value) {
    return ByteBuffer.allocate(Integer.SIZE / Byte.SIZE).putInt((Integer) value).array();
  }

  @Override
  public Object convertByteArrayToObject(byte[] valueInByteArray) {
    return ByteBuffer.wrap(valueInByteArray).getInt();
  }

  @Override
  public Object convertSerializedByteArrayToObject(byte[] valueInSerializedByteArray) {
    return EntryEventImpl.deserialize(valueInSerializedByteArray);
  }

  @Override
  public TinyStoredObject createValueAsUnserializedStoredObject(Object value) {
    byte[] valueInByteArray;
    if (value instanceof Integer) {
      valueInByteArray = convertValueToByteArray(value);
    } else {
      valueInByteArray = (byte[]) value;
    }
    // encode a non-serialized entry value to address
    var encodedAddress =
        OffHeapRegionEntryHelper.encodeDataAsAddress(valueInByteArray, false, false);
    return new TinyStoredObject(encodedAddress);
  }

  @Override
  public TinyStoredObject createValueAsSerializedStoredObject(Object value) {
    var valueInSerializedByteArray = EntryEventImpl.serialize(value);
    // encode a serialized entry value to address
    var encodedAddress =
        OffHeapRegionEntryHelper.encodeDataAsAddress(valueInSerializedByteArray, true, false);
    return new TinyStoredObject(encodedAddress);
  }

  public TinyStoredObject createValueAsCompressedStoredObject(Object value) {
    var valueInSerializedByteArray = EntryEventImpl.serialize(value);
    // encode a serialized, compressed entry value to address
    var encodedAddress =
        OffHeapRegionEntryHelper.encodeDataAsAddress(valueInSerializedByteArray, true, true);
    return new TinyStoredObject(encodedAddress);
  }

  public TinyStoredObject createValueAsUncompressedStoredObject(Object value) {
    var valueInSerializedByteArray = EntryEventImpl.serialize(value);
    // encode a serialized, uncompressed entry value to address
    var encodedAddress =
        OffHeapRegionEntryHelper.encodeDataAsAddress(valueInSerializedByteArray, true, false);
    return new TinyStoredObject(encodedAddress);
  }

  @Test
  public void shouldReturnCorrectEncodingAddress() {

    var address1 = new TinyStoredObject(10001L);
    assertNotNull(address1);
    assertEquals("Encoding address should be:", 10001, address1.getAddress());

    var address2 = new TinyStoredObject(10002L);
    assertNotNull(address2);
    assertEquals("Returning always 10001 expected 10002", 10002, address2.getAddress());
  }

  @Test
  public void twoAddressesShouldBeEqualIfEncodingAddressIsSame() {
    var address1 = new TinyStoredObject(10001L);
    var address2 = new TinyStoredObject(10001L);

    assertEquals("Two addresses are equal if encoding address is same", true,
        address1.equals(address2));
  }

  @Test
  public void twoAddressesShouldNotBeEqualIfEncodingAddressIsNotSame() {
    var address1 = new TinyStoredObject(10001L);
    var address2 = new TinyStoredObject(10002L);

    assertEquals("Two addresses are not equal if encoding address is not same", false,
        address1.equals(address2));
  }

  @Test
  public void twoAddressesAreNotEqualIfTheyAreNotTypeDataAsAddress() {
    var address1 = new TinyStoredObject(10001L);
    Long address2 = 10002L;

    assertEquals("Two addresses are not equal if encoding address is not same", false,
        address1.equals(address2));
  }

  @Test
  public void addressHashCodeShouldBe() {
    var address1 = new TinyStoredObject(10001L);
    assertEquals("", 10001, address1.hashCode());
  }

  @Test
  public void getSizeInBytesAlwaysReturnsZero() {
    var address1 = new TinyStoredObject(10001L);
    var address2 = new TinyStoredObject(10002L);

    assertEquals("getSizeInBytes", 0, address1.getSizeInBytes());
    assertEquals("getSizeInBytes", 0, address2.getSizeInBytes());
  }

  @Test
  public void getValueSizeInBytesAlwaysReturnsZero() {
    var address1 = new TinyStoredObject(10001L);
    var address2 = new TinyStoredObject(10002L);

    assertEquals("getSizeInBytes", 0, address1.getValueSizeInBytes());
    assertEquals("getSizeInBytes", 0, address2.getValueSizeInBytes());
  }

  @Test
  public void isCompressedShouldReturnTrueIfCompressed() {
    var regionEntryValue = getValue();

    var offheapAddress = createValueAsCompressedStoredObject(regionEntryValue);

    assertEquals("Should return true as it is compressed", true, offheapAddress.isCompressed());
  }

  @Test
  public void isCompressedShouldReturnFalseIfNotCompressed() {
    var regionEntryValue = getValue();

    var offheapAddress = createValueAsUncompressedStoredObject(regionEntryValue);

    assertEquals("Should return false as it is compressed", false, offheapAddress.isCompressed());
  }

  @Test
  public void isSerializedShouldReturnTrueIfSeriazlied() {
    var regionEntryValue = getValue();

    var offheapAddress = createValueAsSerializedStoredObject(regionEntryValue);

    assertEquals("Should return true as it is serialized", true, offheapAddress.isSerialized());
  }

  @Test
  public void isSerializedShouldReturnFalseIfNotSeriazlied() {
    var regionEntryValue = getValue();

    var offheapAddress = createValueAsUnserializedStoredObject(regionEntryValue);

    assertEquals("Should return false as it is serialized", false, offheapAddress.isSerialized());
  }

  @Test
  public void getDecompressedBytesShouldReturnDecompressedBytesIfCompressed() {
    var regionEntryValue = getValue();
    var regionEntryValueAsBytes = convertValueToByteArray(regionEntryValue);

    // encode a non-serialized and compressed entry value to address - last argument is to let that
    // it is compressed
    var encodedAddress =
        OffHeapRegionEntryHelper.encodeDataAsAddress(regionEntryValueAsBytes, false, true);
    var offheapAddress = new TinyStoredObject(encodedAddress);

    var regionContext = mock(RegionEntryContext.class);
    var cacheStats = mock(CachePerfStats.class);
    var compressor = mock(Compressor.class);

    var startTime = 10000L;

    // mock required things
    when(regionContext.getCompressor()).thenReturn(compressor);
    when(compressor.decompress(regionEntryValueAsBytes)).thenReturn(regionEntryValueAsBytes);
    when(regionContext.getCachePerfStats()).thenReturn(cacheStats);
    when(cacheStats.startDecompression()).thenReturn(startTime);

    // invoke the thing
    var bytes = offheapAddress.getDecompressedBytes(regionContext);

    // verify the thing happened
    verify(cacheStats, atLeastOnce()).startDecompression();
    verify(compressor, times(1)).decompress(regionEntryValueAsBytes);
    verify(cacheStats, atLeastOnce()).endDecompression(startTime);

    assertArrayEquals(regionEntryValueAsBytes, bytes);
  }

  @Test
  public void getDecompressedBytesShouldNotTryToDecompressIfNotCompressed() {
    var regionEntryValue = getValue();

    var offheapAddress = createValueAsUncompressedStoredObject(regionEntryValue);

    // mock the thing
    var regionContext = mock(RegionEntryContext.class);
    var compressor = mock(Compressor.class);
    when(regionContext.getCompressor()).thenReturn(compressor);

    // invoke the thing
    var actualValueInBytes = offheapAddress.getDecompressedBytes(regionContext);

    // createValueAsUncompressedStoredObject does uses a serialized value - so convert it to object
    var actualRegionValue = convertSerializedByteArrayToObject(actualValueInBytes);

    // verify the thing happened
    verify(regionContext, never()).getCompressor();
    assertEquals(regionEntryValue, actualRegionValue);
  }

  @Test
  public void getRawBytesShouldReturnAByteArray() {
    var regionEntryValueAsBytes = getValueAsByteArray();

    var offheapAddress =
        createValueAsUnserializedStoredObject(regionEntryValueAsBytes);
    var actual = offheapAddress.getRawBytes();

    assertArrayEquals(regionEntryValueAsBytes, actual);
  }

  @Test
  public void getSerializedValueShouldReturnASerializedByteArray() {
    var regionEntryValue = getValue();

    var offheapAddress = createValueAsSerializedStoredObject(regionEntryValue);

    var actualSerializedValue = offheapAddress.getSerializedValue();

    var actualRegionEntryValue = convertSerializedByteArrayToObject(actualSerializedValue);

    assertEquals(regionEntryValue, actualRegionEntryValue);
  }

  @Test
  public void getDeserializedObjectShouldReturnADeserializedObject() {
    var regionEntryValue = getValue();

    var offheapAddress = createValueAsSerializedStoredObject(regionEntryValue);

    var actualRegionEntryValue = (Integer) offheapAddress.getDeserializedValue(null, null);

    assertEquals(regionEntryValue, actualRegionEntryValue);
  }

  @Test
  public void getDeserializedObjectShouldReturnAByteArrayAsIsIfNotSerialized() {
    var regionEntryValueAsBytes = getValueAsByteArray();

    var offheapAddress =
        createValueAsUnserializedStoredObject(regionEntryValueAsBytes);

    var deserializeValue = (byte[]) offheapAddress.getDeserializedValue(null, null);

    assertArrayEquals(regionEntryValueAsBytes, deserializeValue);
  }

  @Test
  public void fillSerializedValueShouldFillWrapperWithSerializedValueIfValueIsSerialized() {
    var regionEntryValue = getValue();
    var serializedRegionEntryValue = EntryEventImpl.serialize(regionEntryValue);

    // encode a serialized entry value to address
    var encodedAddress =
        OffHeapRegionEntryHelper.encodeDataAsAddress(serializedRegionEntryValue, true, false);

    var offheapAddress = new TinyStoredObject(encodedAddress);

    // mock the things
    var wrapper = mock(BytesAndBitsForCompactor.class);

    byte userBits = 1;
    offheapAddress.fillSerializedValue(wrapper, userBits);

    verify(wrapper, times(1)).setData(serializedRegionEntryValue, userBits,
        serializedRegionEntryValue.length, true);
  }

  @Test
  public void fillSerializedValueShouldFillWrapperWithDeserializedValueIfValueIsNotSerialized() {
    var regionEntryValue = getValue();
    var regionEntryValueAsBytes = convertValueToByteArray(regionEntryValue);

    // encode a un serialized entry value to address
    var encodedAddress =
        OffHeapRegionEntryHelper.encodeDataAsAddress(regionEntryValueAsBytes, false, false);

    var offheapAddress = new TinyStoredObject(encodedAddress);

    // mock the things
    var wrapper = mock(BytesAndBitsForCompactor.class);

    byte userBits = 1;
    offheapAddress.fillSerializedValue(wrapper, userBits);

    verify(wrapper, times(1)).setData(regionEntryValueAsBytes, userBits,
        regionEntryValueAsBytes.length, true);
  }

  @Test
  public void getStringFormShouldCatchExceptionAndReturnErrorMessageAsString() {
    var regionEntryValueAsBytes = getValue();

    var serializedValue = EntryEventImpl.serialize(regionEntryValueAsBytes);

    // store -127 (DSCODE.ILLEGAL) - in order the deserialize to throw exception
    serializedValue[0] = DSCODE.ILLEGAL.toByte();

    // encode a un serialized entry value to address
    var encodedAddress =
        OffHeapRegionEntryHelper.encodeDataAsAddress(serializedValue, true, false);

    var offheapAddress = new TinyStoredObject(encodedAddress);

    var errorMessage = offheapAddress.getStringForm();

    assertEquals(true, errorMessage.contains("Could not convert object to string because "));
  }
}
