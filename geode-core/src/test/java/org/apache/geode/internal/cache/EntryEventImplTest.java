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
package org.apache.geode.internal.cache;

import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.geode.Delta;
import org.apache.geode.cache.Operation;
import org.apache.geode.cache.SerializedCacheValue;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.EntryEventImpl.NewValueImporter;
import org.apache.geode.internal.cache.EntryEventImpl.OldValueImporter;
import org.apache.geode.internal.cache.versions.VersionTag;
import org.apache.geode.internal.offheap.MemoryAllocator;
import org.apache.geode.internal.offheap.StoredObject;

public class EntryEventImplTest {

  private String key;

  @Before
  public void setUp() throws Exception {
    key = "key1";
  }

  @Test
  public void verifyToStringOutputHasRegionName() {
    // mock a region object
    var region = mock(LocalRegion.class);
    var expectedRegionName = "ExpectedFullRegionPathName";
    var value = "value1";
    var keyInfo = new KeyInfo(key, value, null);
    doReturn(expectedRegionName).when(region).getFullPath();
    doReturn(keyInfo).when(region).getKeyInfo(any(), any(), any());

    var cache = mock(InternalCache.class);
    var ids = mock(InternalDistributedSystem.class);
    when(region.getCache()).thenReturn(cache);
    when(cache.getDistributedSystem()).thenReturn(ids);
    when(ids.getOffHeapStore()).thenReturn(null);


    // create entry event for the region
    var e = createEntryEvent(region, value);

    // The name of the region should be in the toString text
    var toStringValue = e.toString();
    assertTrue("String " + expectedRegionName + " was not in toString text: " + toStringValue,
        toStringValue.indexOf(expectedRegionName) > 0);

    // verify that toString called getFullPath method of region object
    verify(region, times(1)).getFullPath();
  }

  @Test
  public void verifyExportNewValueWithUnserializedStoredObject() {
    var region = mock(LocalRegion.class);
    when(region.getOffHeap()).thenReturn(true);

    var cache = mock(InternalCache.class);
    var ids = mock(InternalDistributedSystem.class);
    var mem = mock(MemoryAllocator.class);
    when(region.getCache()).thenReturn(cache);
    when(cache.getDistributedSystem()).thenReturn(ids);
    when(ids.getOffHeapStore()).thenReturn(mem);

    var newValue = mock(StoredObject.class);
    var newValueBytes = new byte[] {1, 2, 3};
    when(newValue.getValueAsHeapByteArray()).thenReturn(newValueBytes);
    var nvImporter = mock(NewValueImporter.class);
    var e = createEntryEvent(region, newValue);

    e.exportNewValue(nvImporter);

    verify(nvImporter).importNewBytes(newValueBytes, false);
  }

  @Test
  public void verifyExportNewValueWithByteArray() {
    var region = mock(LocalRegion.class);

    var cache = mock(InternalCache.class);
    var ids = mock(InternalDistributedSystem.class);
    when(region.getCache()).thenReturn(cache);
    when(cache.getDistributedSystem()).thenReturn(ids);
    when(ids.getOffHeapStore()).thenReturn(null);

    var newValue = new byte[] {1, 2, 3};
    var nvImporter = mock(NewValueImporter.class);
    var e = createEntryEvent(region, newValue);

    e.exportNewValue(nvImporter);

    verify(nvImporter).importNewBytes(newValue, false);
  }

  @Test
  public void verifyExportNewValueWithStringIgnoresNewValueBytes() {
    var region = mock(LocalRegion.class);

    var cache = mock(InternalCache.class);
    var ids = mock(InternalDistributedSystem.class);
    when(region.getCache()).thenReturn(cache);
    when(cache.getDistributedSystem()).thenReturn(ids);
    when(ids.getOffHeapStore()).thenReturn(null);

    var newValue = "newValue";
    var nvImporter = mock(NewValueImporter.class);
    when(nvImporter.prefersNewSerialized()).thenReturn(true);
    var e = createEntryEvent(region, newValue);
    var newValueBytes = new byte[] {1, 2};
    e.newValueBytes = newValueBytes;

    e.exportNewValue(nvImporter);

    verify(nvImporter).importNewBytes(newValueBytes, true);
  }

  @Test
  public void verifyExportNewValueWithByteArrayCachedDeserializable() {
    var region = mock(LocalRegion.class);

    var cache = mock(InternalCache.class);
    var ids = mock(InternalDistributedSystem.class);
    when(region.getCache()).thenReturn(cache);
    when(cache.getDistributedSystem()).thenReturn(ids);
    when(ids.getOffHeapStore()).thenReturn(null);

    var newValue = mock(CachedDeserializable.class);
    var newValueBytes = new byte[] {1, 2, 3};
    when(newValue.getValue()).thenReturn(newValueBytes);
    var nvImporter = mock(NewValueImporter.class);
    var e = createEntryEvent(region, newValue);

    e.exportNewValue(nvImporter);

    verify(nvImporter).importNewBytes(newValueBytes, true);
  }

  @Test
  public void verifyExportNewValueWithStringCachedDeserializable() {
    var region = mock(LocalRegion.class);

    var cache = mock(InternalCache.class);
    var ids = mock(InternalDistributedSystem.class);
    when(region.getCache()).thenReturn(cache);
    when(cache.getDistributedSystem()).thenReturn(ids);
    when(ids.getOffHeapStore()).thenReturn(null);

    var newValue = mock(CachedDeserializable.class);
    Object newValueObj = "newValueObj";
    when(newValue.getValue()).thenReturn(newValueObj);
    var nvImporter = mock(NewValueImporter.class);
    var e = createEntryEvent(region, newValue);
    var newValueBytes = new byte[] {1, 2};
    e.newValueBytes = newValueBytes;
    e.setCachedSerializedNewValue(newValueBytes);

    e.exportNewValue(nvImporter);

    verify(nvImporter).importNewObject(newValueObj, true);
  }

  @Test
  public void verifyExportNewValueWithStringCachedDeserializablePrefersNewValueBytes() {
    var region = mock(LocalRegion.class);

    var cache = mock(InternalCache.class);
    var ids = mock(InternalDistributedSystem.class);
    when(region.getCache()).thenReturn(cache);
    when(cache.getDistributedSystem()).thenReturn(ids);
    when(ids.getOffHeapStore()).thenReturn(null);

    var newValue = mock(CachedDeserializable.class);
    Object newValueObj = "newValueObj";
    when(newValue.getValue()).thenReturn(newValueObj);
    var nvImporter = mock(NewValueImporter.class);
    when(nvImporter.prefersNewSerialized()).thenReturn(true);
    var e = createEntryEvent(region, newValue);
    var newValueBytes = new byte[] {1, 2};
    e.newValueBytes = newValueBytes;

    e.exportNewValue(nvImporter);

    verify(nvImporter).importNewBytes(newValueBytes, true);
  }

  @Test
  public void verifyExportNewValueWithStringCachedDeserializablePrefersCachedSerializedNewValue() {
    var region = mock(LocalRegion.class);

    var cache = mock(InternalCache.class);
    var ids = mock(InternalDistributedSystem.class);
    when(region.getCache()).thenReturn(cache);
    when(cache.getDistributedSystem()).thenReturn(ids);
    when(ids.getOffHeapStore()).thenReturn(null);

    var newValue = mock(CachedDeserializable.class);
    Object newValueObj = "newValueObj";
    when(newValue.getValue()).thenReturn(newValueObj);
    var nvImporter = mock(NewValueImporter.class);
    when(nvImporter.prefersNewSerialized()).thenReturn(true);
    var e = createEntryEvent(region, newValue);
    var newValueBytes = new byte[] {1, 2};
    e.setCachedSerializedNewValue(newValueBytes);

    e.exportNewValue(nvImporter);

    verify(nvImporter).importNewBytes(newValueBytes, true);
  }

  @Test
  public void verifyExportNewValueWithSerializedStoredObjectAndImporterPrefersSerialized() {
    var region = mock(LocalRegion.class);
    when(region.getOffHeap()).thenReturn(true);

    var cache = mock(InternalCache.class);
    var ids = mock(InternalDistributedSystem.class);
    var mem = mock(MemoryAllocator.class);
    when(region.getCache()).thenReturn(cache);
    when(cache.getDistributedSystem()).thenReturn(ids);
    when(ids.getOffHeapStore()).thenReturn(mem);

    var newValue = mock(StoredObject.class);
    when(newValue.isSerialized()).thenReturn(true);
    var newValueBytes = new byte[] {1, 2, 3};
    when(newValue.getValueAsHeapByteArray()).thenReturn(newValueBytes);
    var nvImporter = mock(NewValueImporter.class);
    when(nvImporter.prefersNewSerialized()).thenReturn(true);
    var e = createEntryEvent(region, newValue);

    e.exportNewValue(nvImporter);

    verify(nvImporter).importNewBytes(newValueBytes, true);
  }

  @Test
  public void verifyExportNewValueWithSerializedStoredObject() {
    var region = mock(LocalRegion.class);
    when(region.getOffHeap()).thenReturn(true);

    var cache = mock(InternalCache.class);
    var ids = mock(InternalDistributedSystem.class);
    var mem = mock(MemoryAllocator.class);
    when(region.getCache()).thenReturn(cache);
    when(cache.getDistributedSystem()).thenReturn(ids);
    when(ids.getOffHeapStore()).thenReturn(mem);

    var newValue = mock(StoredObject.class);
    when(newValue.isSerialized()).thenReturn(true);
    Object newValueDeserialized = "newValueDeserialized";
    when(newValue.getValueAsDeserializedHeapObject()).thenReturn(newValueDeserialized);
    var nvImporter = mock(NewValueImporter.class);
    var e = createEntryEvent(region, newValue);

    e.exportNewValue(nvImporter);

    verify(nvImporter).importNewObject(newValueDeserialized, true);
  }

  @Test
  public void verifyExportNewValueWithSerializedStoredObjectAndUnretainedNewReferenceOk() {
    var region = mock(LocalRegion.class);
    when(region.getOffHeap()).thenReturn(true);

    var cache = mock(InternalCache.class);
    var ids = mock(InternalDistributedSystem.class);
    var mem = mock(MemoryAllocator.class);
    when(region.getCache()).thenReturn(cache);
    when(cache.getDistributedSystem()).thenReturn(ids);
    when(ids.getOffHeapStore()).thenReturn(mem);

    var newValue = mock(StoredObject.class);
    when(newValue.isSerialized()).thenReturn(true);
    Object newValueDeserialized = "newValueDeserialized";
    when(newValue.getValueAsDeserializedHeapObject()).thenReturn(newValueDeserialized);
    var nvImporter = mock(NewValueImporter.class);
    when(nvImporter.isUnretainedNewReferenceOk()).thenReturn(true);
    var e = createEntryEvent(region, newValue);

    e.exportNewValue(nvImporter);

    verify(nvImporter).importNewObject(newValue, true);
  }

  @Test
  public void verifyExportOldValueWithUnserializedStoredObject() {
    var region = mock(LocalRegion.class);
    when(region.getOffHeap()).thenReturn(true);

    var cache = mock(InternalCache.class);
    var ids = mock(InternalDistributedSystem.class);
    var mem = mock(MemoryAllocator.class);
    when(region.getCache()).thenReturn(cache);
    when(cache.getDistributedSystem()).thenReturn(ids);
    when(ids.getOffHeapStore()).thenReturn(mem);

    var oldValue = mock(StoredObject.class);
    var oldValueBytes = new byte[] {1, 2, 3};
    when(oldValue.getValueAsHeapByteArray()).thenReturn(oldValueBytes);
    var ovImporter = mock(OldValueImporter.class);
    var e = createEntryEvent(region, null);
    e.setOldValue(oldValue);

    e.exportOldValue(ovImporter);

    verify(ovImporter).importOldBytes(oldValueBytes, false);
  }

  @Test
  public void verifyExportOldValueWithByteArray() {
    var region = mock(LocalRegion.class);

    var cache = mock(InternalCache.class);
    var ids = mock(InternalDistributedSystem.class);
    when(region.getCache()).thenReturn(cache);
    when(cache.getDistributedSystem()).thenReturn(ids);
    when(ids.getOffHeapStore()).thenReturn(null);

    var oldValue = new byte[] {1, 2, 3};
    var ovImporter = mock(OldValueImporter.class);
    var e = createEntryEvent(region, null);
    e.setOldValue(oldValue);

    e.exportOldValue(ovImporter);

    verify(ovImporter).importOldBytes(oldValue, false);
  }

  @Test
  public void verifyExportOldValueWithStringIgnoresOldValueBytes() {
    var region = mock(LocalRegion.class);

    var cache = mock(InternalCache.class);
    var ids = mock(InternalDistributedSystem.class);
    when(region.getCache()).thenReturn(cache);
    when(cache.getDistributedSystem()).thenReturn(ids);
    when(ids.getOffHeapStore()).thenReturn(null);

    var oldValue = "oldValue";
    var ovImporter = mock(OldValueImporter.class);
    when(ovImporter.prefersOldSerialized()).thenReturn(true);
    var e = createEntryEvent(region, null);
    var oldValueBytes = new byte[] {1, 2, 3};
    e.setSerializedOldValue(oldValueBytes);
    e.setOldValue(oldValue);

    e.exportOldValue(ovImporter);

    verify(ovImporter).importOldObject(oldValue, true);
  }

  @Test
  public void verifyExportOldValuePrefersOldValueBytes() {
    var region = mock(LocalRegion.class);

    var cache = mock(InternalCache.class);
    var ids = mock(InternalDistributedSystem.class);
    when(region.getCache()).thenReturn(cache);
    when(cache.getDistributedSystem()).thenReturn(ids);
    when(ids.getOffHeapStore()).thenReturn(null);

    var ovImporter = mock(OldValueImporter.class);
    when(ovImporter.prefersOldSerialized()).thenReturn(true);
    var e = createEntryEvent(region, null);
    var oldValueBytes = new byte[] {1, 2, 3};
    e.setSerializedOldValue(oldValueBytes);

    e.exportOldValue(ovImporter);

    verify(ovImporter).importOldBytes(oldValueBytes, true);
  }

  @Test
  public void verifyExportOldValueWithCacheDeserializableByteArray() {
    var region = mock(LocalRegion.class);

    var cache = mock(InternalCache.class);
    var ids = mock(InternalDistributedSystem.class);
    when(region.getCache()).thenReturn(cache);
    when(cache.getDistributedSystem()).thenReturn(ids);
    when(ids.getOffHeapStore()).thenReturn(null);

    var oldValue = mock(CachedDeserializable.class);
    var oldValueBytes = new byte[] {1, 2, 3};
    when(oldValue.getValue()).thenReturn(oldValueBytes);
    var ovImporter = mock(OldValueImporter.class);
    var e = createEntryEvent(region, null);
    e.setOldValue(oldValue);

    e.exportOldValue(ovImporter);

    verify(ovImporter).importOldBytes(oldValueBytes, true);
  }

  @Test
  public void verifyExportOldValueWithCacheDeserializableString() {
    var region = mock(LocalRegion.class);
    var oldValue = mock(CachedDeserializable.class);
    Object oldValueObj = "oldValueObj";

    var cache = mock(InternalCache.class);
    var ids = mock(InternalDistributedSystem.class);
    when(region.getCache()).thenReturn(cache);
    when(cache.getDistributedSystem()).thenReturn(ids);
    when(ids.getOffHeapStore()).thenReturn(null);

    when(oldValue.getValue()).thenReturn(oldValueObj);
    var ovImporter = mock(OldValueImporter.class);
    var e = createEntryEvent(region, null);
    e.setOldValue(oldValue);

    e.exportOldValue(ovImporter);

    verify(ovImporter).importOldObject(oldValueObj, true);
  }

  @Test
  public void verifyExportOldValueWithCacheDeserializableOk() {
    var region = mock(LocalRegion.class);
    var oldValue = mock(CachedDeserializable.class);
    Object oldValueObj = "oldValueObj";

    var cache = mock(InternalCache.class);
    var ids = mock(InternalDistributedSystem.class);
    when(region.getCache()).thenReturn(cache);
    when(cache.getDistributedSystem()).thenReturn(ids);
    when(ids.getOffHeapStore()).thenReturn(null);

    when(oldValue.getValue()).thenReturn(oldValueObj);
    var ovImporter = mock(OldValueImporter.class);
    when(ovImporter.isCachedDeserializableValueOk()).thenReturn(true);
    var e = createEntryEvent(region, null);
    e.setOldValue(oldValue);

    e.exportOldValue(ovImporter);

    verify(ovImporter).importOldObject(oldValue, true);
  }

  @Test
  public void verifyExportOldValueWithSerializedStoredObjectAndImporterPrefersSerialized() {
    var region = mock(LocalRegion.class);
    when(region.getOffHeap()).thenReturn(true);

    var cache = mock(InternalCache.class);
    var ids = mock(InternalDistributedSystem.class);
    var mem = mock(MemoryAllocator.class);
    when(region.getCache()).thenReturn(cache);
    when(cache.getDistributedSystem()).thenReturn(ids);
    when(ids.getOffHeapStore()).thenReturn(mem);

    var oldValue = mock(StoredObject.class);
    when(oldValue.isSerialized()).thenReturn(true);
    var oldValueBytes = new byte[] {1, 2, 3};
    when(oldValue.getValueAsHeapByteArray()).thenReturn(oldValueBytes);
    var ovImporter = mock(OldValueImporter.class);
    when(ovImporter.prefersOldSerialized()).thenReturn(true);
    var e = createEntryEvent(region, null);
    e.setOldValue(oldValue);

    e.exportOldValue(ovImporter);

    verify(ovImporter).importOldBytes(oldValueBytes, true);
  }

  @Test
  public void verifyExportOldValueWithSerializedStoredObject() {
    var region = mock(LocalRegion.class);
    when(region.getOffHeap()).thenReturn(true);

    var cache = mock(InternalCache.class);
    var ids = mock(InternalDistributedSystem.class);
    var mem = mock(MemoryAllocator.class);
    when(region.getCache()).thenReturn(cache);
    when(cache.getDistributedSystem()).thenReturn(ids);
    when(ids.getOffHeapStore()).thenReturn(mem);

    var oldValue = mock(StoredObject.class);
    when(oldValue.isSerialized()).thenReturn(true);
    Object oldValueDeserialized = "newValueDeserialized";
    when(oldValue.getValueAsDeserializedHeapObject()).thenReturn(oldValueDeserialized);
    var ovImporter = mock(OldValueImporter.class);
    var e = createEntryEvent(region, null);
    e.setOldValue(oldValue);

    e.exportOldValue(ovImporter);

    verify(ovImporter).importOldObject(oldValueDeserialized, true);
  }

  @Test
  public void verifyExportOldValueWithSerializedStoredObjectAndUnretainedOldReferenceOk() {
    var region = mock(LocalRegion.class);
    when(region.getOffHeap()).thenReturn(true);

    var cache = mock(InternalCache.class);
    var ids = mock(InternalDistributedSystem.class);
    var mem = mock(MemoryAllocator.class);
    when(region.getCache()).thenReturn(cache);
    when(cache.getDistributedSystem()).thenReturn(ids);
    when(ids.getOffHeapStore()).thenReturn(mem);

    var oldValue = mock(StoredObject.class);
    when(oldValue.isSerialized()).thenReturn(true);
    Object oldValueDeserialized = "oldValueDeserialized";
    when(oldValue.getValueAsDeserializedHeapObject()).thenReturn(oldValueDeserialized);
    var ovImporter = mock(OldValueImporter.class);
    when(ovImporter.isUnretainedOldReferenceOk()).thenReturn(true);
    var e = createEntryEvent(region, null);
    e.setOldValue(oldValue);

    e.exportOldValue(ovImporter);

    verify(ovImporter).importOldObject(oldValue, true);
  }

  @Test
  public void setOldValueUnforcedWithRemoveTokenChangesOldValueToNull() {
    var region = mock(LocalRegion.class);

    var cache = mock(InternalCache.class);
    var ids = mock(InternalDistributedSystem.class);
    when(region.getCache()).thenReturn(cache);
    when(cache.getDistributedSystem()).thenReturn(ids);
    when(ids.getOffHeapStore()).thenReturn(null);

    var e = createEntryEvent(region, null);
    var UNINITIALIZED = "Uninitialized";
    e.basicSetOldValue(UNINITIALIZED);
    e.setOldValue(Token.REMOVED_PHASE1, false);
    assertEquals(null, e.basicGetOldValue());
  }

  @Test
  public void setOldValueForcedWithRemoveTokenChangesOldValueToNull() {
    var region = mock(LocalRegion.class);

    var cache = mock(InternalCache.class);
    var ids = mock(InternalDistributedSystem.class);
    when(region.getCache()).thenReturn(cache);
    when(cache.getDistributedSystem()).thenReturn(ids);
    when(ids.getOffHeapStore()).thenReturn(null);

    var e = createEntryEvent(region, null);
    var UNINITIALIZED = "Uninitialized";
    e.basicSetOldValue(UNINITIALIZED);
    e.setOldValue(Token.REMOVED_PHASE1, true);
    assertEquals(null, e.basicGetOldValue());
  }

  @Test
  public void setOldValueUnforcedWithInvalidTokenNullsOldValue() {
    var region = mock(LocalRegion.class);

    var cache = mock(InternalCache.class);
    var ids = mock(InternalDistributedSystem.class);
    when(region.getCache()).thenReturn(cache);
    when(cache.getDistributedSystem()).thenReturn(ids);
    when(ids.getOffHeapStore()).thenReturn(null);

    var e = createEntryEvent(region, null);
    var UNINITIALIZED = "Uninitialized";
    e.basicSetOldValue(UNINITIALIZED);
    e.setOldValue(Token.INVALID, false);
    assertEquals(null, e.basicGetOldValue());
  }

  @Test
  public void setOldValueForcedWithInvalidTokenNullsOldValue() {
    var region = mock(LocalRegion.class);

    var cache = mock(InternalCache.class);
    var ids = mock(InternalDistributedSystem.class);
    when(region.getCache()).thenReturn(cache);
    when(cache.getDistributedSystem()).thenReturn(ids);
    when(ids.getOffHeapStore()).thenReturn(null);

    var e = createEntryEvent(region, null);
    var UNINITIALIZED = "Uninitialized";
    e.basicSetOldValue(UNINITIALIZED);
    e.setOldValue(Token.INVALID, true);
    assertEquals(null, e.basicGetOldValue());
  }

  @Test
  public void setOldValueUnforcedWithNullChangesOldValueToNull() {
    var region = mock(LocalRegion.class);

    var cache = mock(InternalCache.class);
    var ids = mock(InternalDistributedSystem.class);
    when(region.getCache()).thenReturn(cache);
    when(cache.getDistributedSystem()).thenReturn(ids);
    when(ids.getOffHeapStore()).thenReturn(null);

    var e = createEntryEvent(region, null);
    var UNINITIALIZED = "Uninitialized";
    e.basicSetOldValue(UNINITIALIZED);
    e.setOldValue(null, false);
    assertEquals(null, e.basicGetOldValue());
  }

  @Test
  public void setOldValueForcedWithNullChangesOldValueToNull() {
    var region = mock(LocalRegion.class);

    var cache = mock(InternalCache.class);
    var ids = mock(InternalDistributedSystem.class);
    when(region.getCache()).thenReturn(cache);
    when(cache.getDistributedSystem()).thenReturn(ids);
    when(ids.getOffHeapStore()).thenReturn(null);

    var e = createEntryEvent(region, null);
    var UNINITIALIZED = "Uninitialized";
    e.basicSetOldValue(UNINITIALIZED);
    e.setOldValue(null, true);
    assertEquals(null, e.basicGetOldValue());
  }

  @Test
  public void setOldValueForcedWithNotAvailableTokenSetsOldValue() {
    var region = mock(LocalRegion.class);

    var cache = mock(InternalCache.class);
    var ids = mock(InternalDistributedSystem.class);
    when(region.getCache()).thenReturn(cache);
    when(cache.getDistributedSystem()).thenReturn(ids);
    when(ids.getOffHeapStore()).thenReturn(null);

    var e = createEntryEvent(region, null);
    var UNINITIALIZED = "Uninitialized";
    e.basicSetOldValue(UNINITIALIZED);
    e.setOldValue(Token.NOT_AVAILABLE, true);
    assertEquals(Token.NOT_AVAILABLE, e.basicGetOldValue());
  }

  @Test
  public void setOldValueUnforcedWithNotAvailableTokenSetsOldValue() {
    var region = mock(LocalRegion.class);

    var cache = mock(InternalCache.class);
    var ids = mock(InternalDistributedSystem.class);
    when(region.getCache()).thenReturn(cache);
    when(cache.getDistributedSystem()).thenReturn(ids);
    when(ids.getOffHeapStore()).thenReturn(null);

    var e = createEntryEvent(region, null);
    var UNINITIALIZED = "Uninitialized";
    e.basicSetOldValue(UNINITIALIZED);
    e.setOldValue(Token.NOT_AVAILABLE, false);
    assertEquals(Token.NOT_AVAILABLE, e.basicGetOldValue());
  }

  @Test
  public void setOldUnforcedValueSetsOldValue() {
    var region = mock(LocalRegion.class);

    var cache = mock(InternalCache.class);
    var ids = mock(InternalDistributedSystem.class);
    when(region.getCache()).thenReturn(cache);
    when(cache.getDistributedSystem()).thenReturn(ids);
    when(ids.getOffHeapStore()).thenReturn(null);

    var e = createEntryEvent(region, null);
    var UNINITIALIZED = "Uninitialized";
    e.basicSetOldValue(UNINITIALIZED);
    e.setOldValue("oldValue", false);
    assertEquals("oldValue", e.basicGetOldValue());
  }

  @Test
  public void setOldValueForcedSetsOldValue() {
    var region = mock(LocalRegion.class);

    var cache = mock(InternalCache.class);
    var ids = mock(InternalDistributedSystem.class);
    when(region.getCache()).thenReturn(cache);
    when(cache.getDistributedSystem()).thenReturn(ids);
    when(ids.getOffHeapStore()).thenReturn(null);

    var e = createEntryEvent(region, null);
    var UNINITIALIZED = "Uninitialized";
    e.basicSetOldValue(UNINITIALIZED);
    e.setOldValue("oldValue", true);
    assertEquals("oldValue", e.basicGetOldValue());
  }

  @Test
  public void setOldValueUnforcedWithDisabledSetsNotAvailable() {
    EntryEventImpl e = new EntryEventImplWithOldValuesDisabled();
    var UNINITIALIZED = "Uninitialized";
    e.basicSetOldValue(UNINITIALIZED);
    e.setOldValue("oldValue", false);
    assertEquals(Token.NOT_AVAILABLE, e.basicGetOldValue());
  }

  @Test
  public void setOldValueForcedWithDisabledSetsOldValue() {
    EntryEventImpl e = new EntryEventImplWithOldValuesDisabled();
    var UNINITIALIZED = "Uninitialized";
    e.basicSetOldValue(UNINITIALIZED);
    e.setOldValue("oldValue", true);
    assertEquals("oldValue", e.basicGetOldValue());
  }

  @Test
  public void verifyExternalReadMethodsBlockedByRelease() throws InterruptedException {
    var region = mock(LocalRegion.class);
    when(region.getOffHeap()).thenReturn(true);

    var cache = mock(InternalCache.class);
    var ids = mock(InternalDistributedSystem.class);
    var mem = mock(MemoryAllocator.class);
    when(region.getCache()).thenReturn(cache);
    when(cache.getDistributedSystem()).thenReturn(ids);
    when(ids.getOffHeapStore()).thenReturn(mem);

    var newValue = mock(StoredObject.class);
    when(newValue.hasRefCount()).thenReturn(true);
    when(newValue.isSerialized()).thenReturn(true);
    when(newValue.retain()).thenReturn(true);
    when(newValue.getDeserializedValue(any(), any())).thenReturn("newValue");
    final var serializedNewValue = new byte[] {(byte) 'n', (byte) 'e', (byte) 'w'};
    when(newValue.getSerializedValue()).thenReturn(serializedNewValue);
    var oldValue = mock(StoredObject.class);
    when(oldValue.hasRefCount()).thenReturn(true);
    when(oldValue.isSerialized()).thenReturn(true);
    when(oldValue.retain()).thenReturn(true);
    when(oldValue.getDeserializedValue(any(), any())).thenReturn("oldValue");
    final var serializedOldValue = new byte[] {(byte) 'o', (byte) 'l', (byte) 'd'};
    when(oldValue.getSerializedValue()).thenReturn(serializedOldValue);
    final var releaseCountDown = new CountDownLatch(1);
    final var e =
        new TestableEntryEventImpl(region, key, newValue, releaseCountDown);
    e.setOldValue(oldValue);
    assertEquals("newValue", e.getNewValue());
    assertEquals("oldValue", e.getOldValue());
    final var serializableNewValue = e.getSerializedNewValue();
    assertEquals(serializedNewValue, serializableNewValue.getSerializedValue());
    assertEquals("newValue", serializableNewValue.getDeserializedValue());
    final var serializableOldValue = e.getSerializedOldValue();
    assertEquals(serializedOldValue, serializableOldValue.getSerializedValue());
    assertEquals("oldValue", serializableOldValue.getDeserializedValue());
    var doRelease = new Thread(e::release);
    doRelease.start(); // release thread will be stuck until releaseCountDown changes
    await()
        .timeout(15, TimeUnit.SECONDS)
        .untilAsserted(() -> assertEquals(true, e.isWaitingOnRelease()));
    assertEquals(true, e.offHeapOk);
    assertEquals(true, doRelease.isAlive());

    // Now start a getNewValue. It should block on the release.
    var doGetNewValue = new Thread(e::getAndCacheNewValue);
    doGetNewValue.start();
    // Now start a getOldValue. It should block on the release.
    var doGetOldValue = new Thread(e::getAndCacheOldValue);
    doGetOldValue.start();
    // Now start a getSerializedValue on serializableNewValue. It should block on the release.
    var doSNVgetSerializedValue = new Thread(() -> {
      e.getAndCacheSerializedNew(serializableNewValue);
    });
    doSNVgetSerializedValue.start();
    // Now start a getDeserializedValue on serializableNewValue. It should block on the release.
    var doSNVgetDeserializedValue = new Thread(() -> {
      e.getAndCachDeserializedNew(serializableNewValue);
    });
    doSNVgetDeserializedValue.start();
    // Now start a getSerializedValue on serializableOldValue. It should block on the release.
    var doSOVgetSerializedValue = new Thread(() -> {
      e.getAndCacheSerializedOld(serializableOldValue);
    });
    doSOVgetSerializedValue.start();
    // Now start a getDeserializedValue on serializableOldValue. It should block on the release.
    var doSOVgetDeserializedValue = new Thread(() -> {
      e.getAndCachDeserializedOld(serializableOldValue);
    });
    doSOVgetDeserializedValue.start();

    await()
        .timeout(15, TimeUnit.SECONDS)
        .untilAsserted(() -> assertEquals(true,
            e.isAboutToCallGetNewValue() && e.isAboutToCallGetOldValue()
                && e.isAboutToCallSerializedNew() && e.isAboutToCallDeserializedNew()
                && e.isAboutToCallSerializedOld() && e.isAboutToCallDeserializedOld()));
    // all the threads should now be hung waiting on release; so just wait for a little bit for it
    // to improperly finish
    doGetNewValue.join(50);
    if (e.hasFinishedCallOfGetNewValue()) {
      fail("expected doGetNewValue thread to be hung. It completed with " + e.getCachedNewValue());
    }
    if (e.hasFinishedCallOfGetOldValue()) {
      fail("expected doGetOldValue thread to be hung. It completed with " + e.getCachedOldValue());
    }
    if (e.hasFinishedCallOfSerializedNew()) {
      fail("expected doSNVgetSerializedValue thread to be hung. It completed with "
          + e.getTestCachedSerializedNew());
    }
    if (e.hasFinishedCallOfDeserializedNew()) {
      fail("expected doSNVgetDeserializedValue thread to be hung. It completed with "
          + e.getCachedDeserializedNew());
    }
    if (e.hasFinishedCallOfSerializedOld()) {
      fail("expected doSOVgetSerializedValue thread to be hung. It completed with "
          + e.getCachedSerializedOld());
    }
    if (e.hasFinishedCallOfDeserializedOld()) {
      fail("expected doSOVgetDeserializedValue thread to be hung. It completed with "
          + e.getCachedDeserializedOld());
    }
    // now signal the release to go ahead
    releaseCountDown.countDown();
    doRelease.join();
    assertEquals(false, e.offHeapOk);
    // which should allow getNewValue to complete
    await()
        .timeout(15, TimeUnit.SECONDS)
        .untilAsserted(() -> assertEquals(true, e.hasFinishedCallOfGetNewValue()));
    doGetNewValue.join();
    if (!(e.getCachedNewValue() instanceof IllegalStateException)) {
      // since the release happened before getNewValue we expect it to get an exception
      fail("unexpected success of getNewValue. It returned " + e.getCachedNewValue());
    }
    // which should allow getOldValue to complete
    await()
        .timeout(15, TimeUnit.SECONDS)
        .untilAsserted(() -> assertEquals(true, e.hasFinishedCallOfGetOldValue()));
    doGetOldValue.join();
    if (!(e.getCachedOldValue() instanceof IllegalStateException)) {
      fail("unexpected success of getOldValue. It returned " + e.getCachedOldValue());
    }
    // which should allow doSNVgetSerializedValue to complete
    await()
        .timeout(15, TimeUnit.SECONDS)
        .untilAsserted(() -> assertEquals(true, e.hasFinishedCallOfSerializedNew()));
    doSNVgetSerializedValue.join();
    if (!(e.getTestCachedSerializedNew() instanceof IllegalStateException)) {
      fail("unexpected success of new getSerializedValue. It returned "
          + e.getTestCachedSerializedNew());
    }
    // which should allow doSNVgetDeserializedValue to complete
    await()
        .timeout(15, TimeUnit.SECONDS)
        .untilAsserted(() -> assertEquals(true, e.hasFinishedCallOfDeserializedNew()));
    doSNVgetDeserializedValue.join();
    if (!(e.getCachedDeserializedNew() instanceof IllegalStateException)) {
      fail("unexpected success of new getDeserializedValue. It returned "
          + e.getCachedDeserializedNew());
    }
    // which should allow doSOVgetSerializedValue to complete
    await()
        .timeout(15, TimeUnit.SECONDS)
        .untilAsserted(() -> assertEquals(true, e.hasFinishedCallOfSerializedOld()));
    doSOVgetSerializedValue.join();
    if (!(e.getCachedSerializedOld() instanceof IllegalStateException)) {
      fail("unexpected success of old getSerializedValue. It returned "
          + e.getCachedSerializedOld());
    }
    // which should allow doSOVgetDeserializedValue to complete
    await()
        .timeout(15, TimeUnit.SECONDS)
        .untilAsserted(() -> assertEquals(true, e.hasFinishedCallOfDeserializedOld()));
    doSOVgetDeserializedValue.join();
    if (!(e.getCachedDeserializedOld() instanceof IllegalStateException)) {
      fail("unexpected success of old getDeserializedValue. It returned "
          + e.getCachedDeserializedOld());
    }
  }

  @Test
  public void testGetEventTimeWithNullVersionTag() {
    var timestamp = System.currentTimeMillis();
    var region = mock(LocalRegion.class);
    when(region.cacheTimeMillis()).thenReturn(timestamp);

    var cache = mock(InternalCache.class);
    var ids = mock(InternalDistributedSystem.class);
    when(region.getCache()).thenReturn(cache);
    when(cache.getDistributedSystem()).thenReturn(ids);
    when(ids.getOffHeapStore()).thenReturn(null);

    var e = createEntryEvent(region, null);
    assertThat(e.getEventTime(0l)).isEqualTo(timestamp);
  }

  @Test
  public void testGetEventTimeWithVersionTagConcurrencyChecksEnabled() {
    var timestamp = System.currentTimeMillis();
    var region = mock(LocalRegion.class);
    when(region.getConcurrencyChecksEnabled()).thenReturn(true);

    var cache = mock(InternalCache.class);
    var ids = mock(InternalDistributedSystem.class);
    when(region.getCache()).thenReturn(cache);
    when(cache.getDistributedSystem()).thenReturn(ids);
    when(ids.getOffHeapStore()).thenReturn(null);

    var e = createEntryEvent(region, null);
    var tag = VersionTag.create(mock(InternalDistributedMember.class));
    tag.setVersionTimeStamp(timestamp);
    e.setVersionTag(tag);
    assertThat(e.getEventTime(0l)).isEqualTo(timestamp);
  }

  @Test
  public void testGetEventTimeWithVersionTagConcurrencyChecksEnabledWithSuggestedTime() {
    var timestamp = System.currentTimeMillis();
    var timestampPlus1 = timestamp + 1000l;
    var timestampPlus2 = timestamp + 2000l;
    var region = mock(LocalRegion.class);
    when(region.getConcurrencyChecksEnabled()).thenReturn(true);
    when(region.cacheTimeMillis()).thenReturn(timestamp);

    var cache = mock(InternalCache.class);
    var ids = mock(InternalDistributedSystem.class);
    when(region.getCache()).thenReturn(cache);
    when(cache.getDistributedSystem()).thenReturn(ids);
    when(ids.getOffHeapStore()).thenReturn(null);

    var e = createEntryEvent(region, null);
    var tag = VersionTag.create(mock(InternalDistributedMember.class));
    tag.setVersionTimeStamp(timestampPlus1);
    e.setVersionTag(tag);
    assertThat(e.getEventTime(timestampPlus2)).isEqualTo(timestampPlus2);
    assertThat(tag.getVersionTimeStamp()).isEqualTo(timestampPlus2);
  }

  @Test
  public void testGetEventTimeWithVersionTagConcurrencyChecksDisabledNoSuggestedTime() {
    var timestamp = System.currentTimeMillis();
    var region = mock(LocalRegion.class);
    when(region.getConcurrencyChecksEnabled()).thenReturn(false);
    when(region.cacheTimeMillis()).thenReturn(timestamp);

    var cache = mock(InternalCache.class);
    var ids = mock(InternalDistributedSystem.class);
    when(region.getCache()).thenReturn(cache);
    when(cache.getDistributedSystem()).thenReturn(ids);
    when(ids.getOffHeapStore()).thenReturn(null);

    var e = createEntryEvent(region, null);
    var tag = VersionTag.create(mock(InternalDistributedMember.class));
    tag.setVersionTimeStamp(timestamp + 1000l);
    e.setVersionTag(tag);
    assertThat(e.getEventTime(0l)).isEqualTo(timestamp);
  }

  @Test
  public void testGetEventTimeWithVersionTagConcurrencyChecksDisabledWithSuggestedTime() {
    var timestamp = System.currentTimeMillis();
    var region = mock(LocalRegion.class);
    when(region.getConcurrencyChecksEnabled()).thenReturn(false);

    var cache = mock(InternalCache.class);
    var ids = mock(InternalDistributedSystem.class);
    when(region.getCache()).thenReturn(cache);
    when(cache.getDistributedSystem()).thenReturn(ids);
    when(ids.getOffHeapStore()).thenReturn(null);

    var e = createEntryEvent(region, null);
    var tag = VersionTag.create(mock(InternalDistributedMember.class));
    tag.setVersionTimeStamp(timestamp + 1000l);
    e.setVersionTag(tag);
    assertThat(e.getEventTime(timestamp)).isEqualTo(timestamp);
  }

  @Test
  public void shouldRecalculateSize_returnsTrue_ifGetForceRecalculateSizeIsTrue_andDELTAS_RECALCULATE_SIZEisTrue() {
    GemFireCacheImpl.DELTAS_RECALCULATE_SIZE = true;
    var deltaValue = mock(Delta.class);
    when(deltaValue.getForceRecalculateSize())
        .thenReturn(true);

    var value = EntryEventImpl.shouldRecalculateSize(deltaValue);

    assertThat(value).isTrue();
  }

  @Test
  public void shouldRecalculateSize_returnsTrue_ifDELTAS_RECALCULATE_SIZEisTrue_andGetForceRecalculateSizeIsFalse() {
    GemFireCacheImpl.DELTAS_RECALCULATE_SIZE = true;
    var deltaValue = mock(Delta.class);
    when(deltaValue.getForceRecalculateSize())
        .thenReturn(false);

    var value = EntryEventImpl.shouldRecalculateSize(deltaValue);

    assertThat(value).isTrue();
  }

  @Test
  public void shouldRecalculateSize_returnsTrue_ifGetForceRecalculateSizeIsTrue_andDELTAS_RECALCULATE_SIZEIsFalse() {
    GemFireCacheImpl.DELTAS_RECALCULATE_SIZE = false;
    var deltaValue = mock(Delta.class);
    when(deltaValue.getForceRecalculateSize())
        .thenReturn(true);

    var value = EntryEventImpl.shouldRecalculateSize(deltaValue);

    assertThat(value).isTrue();
  }


  @Test
  public void shouldRecalculateSize_returnsFalse_ifBothDELTAS_RECALCULATE_SIZEIsFalse_andGetForceRecalculateSizeIsFalse() {
    GemFireCacheImpl.DELTAS_RECALCULATE_SIZE = false;
    var deltaValue = mock(Delta.class);
    when(deltaValue.getForceRecalculateSize())
        .thenReturn(false);

    var value = EntryEventImpl.shouldRecalculateSize(deltaValue);

    assertThat(value).isFalse();
  }

  @After
  public void tearDown() {
    GemFireCacheImpl.DELTAS_RECALCULATE_SIZE = false;
  }

  private static class EntryEventImplWithOldValuesDisabled extends EntryEventImpl {
    @Override
    protected boolean areOldValuesEnabled() {
      return false;
    }
  }

  private static class TestableEntryEventImpl extends EntryEventImpl {
    private final CountDownLatch releaseCountDown;
    private volatile boolean waitingOnRelease = false;
    private volatile boolean aboutToCallGetNewValue = false;
    private volatile boolean finishedCallOfGetNewValue = false;
    private volatile Object cachedNewValue = null;
    private volatile boolean aboutToCallGetOldValue = false;
    private volatile boolean finishedCallOfGetOldValue = false;
    private volatile Object cachedOldValue = null;
    private volatile boolean aboutToCallSerializedNew = false;
    private volatile Object testCachedSerializedNew = null;
    private volatile boolean finishedCallOfSerializedNew = false;
    private volatile boolean aboutToCallDeserializedNew = false;
    private volatile Object cachedDeserializedNew = null;
    private volatile boolean finishedCallOfDeserializedNew = false;
    private volatile boolean aboutToCallSerializedOld = false;
    private volatile Object cachedSerializedOld = null;
    private volatile boolean finishedCallOfSerializedOld = false;
    private volatile boolean aboutToCallDeserializedOld = false;
    private volatile Object cachedDeserializedOld = null;
    private volatile boolean finishedCallOfDeserializedOld = false;

    public TestableEntryEventImpl(LocalRegion region, Object key, Object newValue,
        CountDownLatch releaseCountDown) {
      super(region, Operation.CREATE, key, newValue, null, false, null, false, createEventID());
      callbacksInvoked(true);
      this.releaseCountDown = releaseCountDown;
    }

    public Object getCachedDeserializedOld() {
      return cachedDeserializedOld;
    }

    public boolean hasFinishedCallOfDeserializedOld() {
      return finishedCallOfDeserializedOld;
    }

    public Object getCachedSerializedOld() {
      return cachedSerializedOld;
    }

    public boolean hasFinishedCallOfSerializedOld() {
      return finishedCallOfSerializedOld;
    }

    public Object getCachedDeserializedNew() {
      return cachedDeserializedNew;
    }

    public Object getTestCachedSerializedNew() {
      return testCachedSerializedNew;
    }

    public boolean hasFinishedCallOfDeserializedNew() {
      return finishedCallOfDeserializedNew;
    }

    public boolean hasFinishedCallOfSerializedNew() {
      return finishedCallOfSerializedNew;
    }

    public boolean isAboutToCallDeserializedOld() {
      return aboutToCallDeserializedOld;
    }

    public boolean isAboutToCallSerializedOld() {
      return aboutToCallSerializedOld;
    }

    public boolean isAboutToCallDeserializedNew() {
      return aboutToCallDeserializedNew;
    }

    public boolean isAboutToCallSerializedNew() {
      return aboutToCallSerializedNew;
    }

    public void getAndCachDeserializedOld(SerializedCacheValue<?> serializableOldValue) {
      try {
        aboutToCallDeserializedOld = true;
        cachedDeserializedOld = serializableOldValue.getDeserializedValue();
      } catch (IllegalStateException ex) {
        cachedDeserializedOld = ex;
      } finally {
        finishedCallOfDeserializedOld = true;
      }
    }

    public void getAndCacheSerializedOld(SerializedCacheValue<?> serializableOldValue) {
      try {
        aboutToCallSerializedOld = true;
        cachedSerializedOld = serializableOldValue.getSerializedValue();
      } catch (IllegalStateException ex) {
        cachedSerializedOld = ex;
      } finally {
        finishedCallOfSerializedOld = true;
      }
    }

    public void getAndCachDeserializedNew(SerializedCacheValue<?> serializableNewValue) {
      try {
        aboutToCallDeserializedNew = true;
        cachedDeserializedNew = serializableNewValue.getDeserializedValue();
      } catch (IllegalStateException ex) {
        cachedDeserializedNew = ex;
      } finally {
        finishedCallOfDeserializedNew = true;
      }
    }

    public void getAndCacheSerializedNew(SerializedCacheValue<?> serializableNewValue) {
      try {
        aboutToCallSerializedNew = true;
        testCachedSerializedNew = serializableNewValue.getSerializedValue();
      } catch (IllegalStateException ex) {
        testCachedSerializedNew = ex;
      } finally {
        finishedCallOfSerializedNew = true;
      }
    }

    public Object getCachedNewValue() {
      return cachedNewValue;
    }

    public void getAndCacheNewValue() {
      try {
        aboutToCallGetNewValue = true;
        cachedNewValue = getNewValue();
      } catch (IllegalStateException ex) {
        cachedNewValue = ex;
      } finally {
        finishedCallOfGetNewValue = true;
      }
    }

    public Object getCachedOldValue() {
      return cachedOldValue;
    }

    public void getAndCacheOldValue() {
      try {
        aboutToCallGetOldValue = true;
        cachedOldValue = getOldValue();
      } catch (IllegalStateException ex) {
        cachedOldValue = ex;
      } finally {
        finishedCallOfGetOldValue = true;
      }
    }

    public boolean isWaitingOnRelease() {
      return waitingOnRelease;
    }

    public boolean isAboutToCallGetNewValue() {
      return aboutToCallGetNewValue;
    }

    public boolean hasFinishedCallOfGetNewValue() {
      return finishedCallOfGetNewValue;
    }

    public boolean isAboutToCallGetOldValue() {
      return aboutToCallGetOldValue;
    }

    public boolean hasFinishedCallOfGetOldValue() {
      return finishedCallOfGetOldValue;
    }

    @Override
    void testHookReleaseInProgress() {
      try {
        waitingOnRelease = true;
        releaseCountDown.await();
      } catch (InterruptedException e) {
        // quit waiting
      }
    }
  }

  private static EventID createEventID() {
    byte[] memId = {1, 2, 3};
    return new EventID(memId, 11, 12, 13);
  }

  private EntryEventImpl createEntryEvent(LocalRegion l, Object newValue) {
    // create an event
    var event = EntryEventImpl.create(l, Operation.CREATE, key, newValue, null,
        false /* origin remote */, null, false /* generateCallbacks */, createEventID());
    // avoid calling invokeCallbacks
    event.callbacksInvoked(true);

    return event;
  }
}
