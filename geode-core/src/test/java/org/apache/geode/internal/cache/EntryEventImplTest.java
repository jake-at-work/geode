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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.concurrent.CountDownLatch;

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

public class EntryEventImplTest {

  private String key;

  @Before
  public void setUp() throws Exception {
    key = "key1";
  }

  @Test
  public void verifyToStringOutputHasRegionName() {
    // mock a region object
    LocalRegion region = mock(LocalRegion.class);
    String expectedRegionName = "ExpectedFullRegionPathName";
    String value = "value1";
    KeyInfo keyInfo = new KeyInfo(key, value, null);
    doReturn(expectedRegionName).when(region).getFullPath();
    doReturn(keyInfo).when(region).getKeyInfo(any(), any(), any());

    InternalCache cache = mock(InternalCache.class);
    InternalDistributedSystem ids = mock(InternalDistributedSystem.class);
    when(region.getCache()).thenReturn(cache);
    when(cache.getDistributedSystem()).thenReturn(ids);


    // create entry event for the region
    EntryEventImpl e = createEntryEvent(region, value);

    // The name of the region should be in the toString text
    String toStringValue = e.toString();
    assertTrue("String " + expectedRegionName + " was not in toString text: " + toStringValue,
        toStringValue.indexOf(expectedRegionName) > 0);

    // verify that toString called getFullPath method of region object
    verify(region, times(1)).getFullPath();
  }

  @Test
  public void verifyExportNewValueWithByteArray() {
    LocalRegion region = mock(LocalRegion.class);

    InternalCache cache = mock(InternalCache.class);
    InternalDistributedSystem ids = mock(InternalDistributedSystem.class);
    when(region.getCache()).thenReturn(cache);
    when(cache.getDistributedSystem()).thenReturn(ids);

    byte[] newValue = new byte[] {1, 2, 3};
    NewValueImporter nvImporter = mock(NewValueImporter.class);
    EntryEventImpl e = createEntryEvent(region, newValue);

    e.exportNewValue(nvImporter);

    verify(nvImporter).importNewBytes(newValue, false);
  }

  @Test
  public void verifyExportNewValueWithStringIgnoresNewValueBytes() {
    LocalRegion region = mock(LocalRegion.class);

    InternalCache cache = mock(InternalCache.class);
    InternalDistributedSystem ids = mock(InternalDistributedSystem.class);
    when(region.getCache()).thenReturn(cache);
    when(cache.getDistributedSystem()).thenReturn(ids);

    String newValue = "newValue";
    NewValueImporter nvImporter = mock(NewValueImporter.class);
    when(nvImporter.prefersNewSerialized()).thenReturn(true);
    EntryEventImpl e = createEntryEvent(region, newValue);
    byte[] newValueBytes = new byte[] {1, 2};
    e.newValueBytes = newValueBytes;

    e.exportNewValue(nvImporter);

    verify(nvImporter).importNewBytes(newValueBytes, true);
  }

  @Test
  public void verifyExportNewValueWithByteArrayCachedDeserializable() {
    LocalRegion region = mock(LocalRegion.class);

    InternalCache cache = mock(InternalCache.class);
    InternalDistributedSystem ids = mock(InternalDistributedSystem.class);
    when(region.getCache()).thenReturn(cache);
    when(cache.getDistributedSystem()).thenReturn(ids);

    CachedDeserializable newValue = mock(CachedDeserializable.class);
    byte[] newValueBytes = new byte[] {1, 2, 3};
    when(newValue.getValue()).thenReturn(newValueBytes);
    NewValueImporter nvImporter = mock(NewValueImporter.class);
    EntryEventImpl e = createEntryEvent(region, newValue);

    e.exportNewValue(nvImporter);

    verify(nvImporter).importNewBytes(newValueBytes, true);
  }

  @Test
  public void verifyExportNewValueWithStringCachedDeserializable() {
    LocalRegion region = mock(LocalRegion.class);

    InternalCache cache = mock(InternalCache.class);
    InternalDistributedSystem ids = mock(InternalDistributedSystem.class);
    when(region.getCache()).thenReturn(cache);
    when(cache.getDistributedSystem()).thenReturn(ids);

    CachedDeserializable newValue = mock(CachedDeserializable.class);
    Object newValueObj = "newValueObj";
    when(newValue.getValue()).thenReturn(newValueObj);
    NewValueImporter nvImporter = mock(NewValueImporter.class);
    EntryEventImpl e = createEntryEvent(region, newValue);
    byte[] newValueBytes = new byte[] {1, 2};
    e.newValueBytes = newValueBytes;
    e.setCachedSerializedNewValue(newValueBytes);

    e.exportNewValue(nvImporter);

    verify(nvImporter).importNewObject(newValueObj, true);
  }

  @Test
  public void verifyExportNewValueWithStringCachedDeserializablePrefersNewValueBytes() {
    LocalRegion region = mock(LocalRegion.class);

    InternalCache cache = mock(InternalCache.class);
    InternalDistributedSystem ids = mock(InternalDistributedSystem.class);
    when(region.getCache()).thenReturn(cache);
    when(cache.getDistributedSystem()).thenReturn(ids);

    CachedDeserializable newValue = mock(CachedDeserializable.class);
    Object newValueObj = "newValueObj";
    when(newValue.getValue()).thenReturn(newValueObj);
    NewValueImporter nvImporter = mock(NewValueImporter.class);
    when(nvImporter.prefersNewSerialized()).thenReturn(true);
    EntryEventImpl e = createEntryEvent(region, newValue);
    byte[] newValueBytes = new byte[] {1, 2};
    e.newValueBytes = newValueBytes;

    e.exportNewValue(nvImporter);

    verify(nvImporter).importNewBytes(newValueBytes, true);
  }

  @Test
  public void verifyExportNewValueWithStringCachedDeserializablePrefersCachedSerializedNewValue() {
    LocalRegion region = mock(LocalRegion.class);

    InternalCache cache = mock(InternalCache.class);
    InternalDistributedSystem ids = mock(InternalDistributedSystem.class);
    when(region.getCache()).thenReturn(cache);
    when(cache.getDistributedSystem()).thenReturn(ids);

    CachedDeserializable newValue = mock(CachedDeserializable.class);
    Object newValueObj = "newValueObj";
    when(newValue.getValue()).thenReturn(newValueObj);
    NewValueImporter nvImporter = mock(NewValueImporter.class);
    when(nvImporter.prefersNewSerialized()).thenReturn(true);
    EntryEventImpl e = createEntryEvent(region, newValue);
    byte[] newValueBytes = new byte[] {1, 2};
    e.setCachedSerializedNewValue(newValueBytes);

    e.exportNewValue(nvImporter);

    verify(nvImporter).importNewBytes(newValueBytes, true);
  }

  @Test
  public void verifyExportOldValueWithByteArray() {
    LocalRegion region = mock(LocalRegion.class);

    InternalCache cache = mock(InternalCache.class);
    InternalDistributedSystem ids = mock(InternalDistributedSystem.class);
    when(region.getCache()).thenReturn(cache);
    when(cache.getDistributedSystem()).thenReturn(ids);

    byte[] oldValue = new byte[] {1, 2, 3};
    OldValueImporter ovImporter = mock(OldValueImporter.class);
    EntryEventImpl e = createEntryEvent(region, null);
    e.setOldValue(oldValue);

    e.exportOldValue(ovImporter);

    verify(ovImporter).importOldBytes(oldValue, false);
  }

  @Test
  public void verifyExportOldValueWithStringIgnoresOldValueBytes() {
    LocalRegion region = mock(LocalRegion.class);

    InternalCache cache = mock(InternalCache.class);
    InternalDistributedSystem ids = mock(InternalDistributedSystem.class);
    when(region.getCache()).thenReturn(cache);
    when(cache.getDistributedSystem()).thenReturn(ids);

    String oldValue = "oldValue";
    OldValueImporter ovImporter = mock(OldValueImporter.class);
    when(ovImporter.prefersOldSerialized()).thenReturn(true);
    EntryEventImpl e = createEntryEvent(region, null);
    byte[] oldValueBytes = new byte[] {1, 2, 3};
    e.setSerializedOldValue(oldValueBytes);
    e.setOldValue(oldValue);

    e.exportOldValue(ovImporter);

    verify(ovImporter).importOldObject(oldValue, true);
  }

  @Test
  public void verifyExportOldValuePrefersOldValueBytes() {
    LocalRegion region = mock(LocalRegion.class);

    InternalCache cache = mock(InternalCache.class);
    InternalDistributedSystem ids = mock(InternalDistributedSystem.class);
    when(region.getCache()).thenReturn(cache);
    when(cache.getDistributedSystem()).thenReturn(ids);

    OldValueImporter ovImporter = mock(OldValueImporter.class);
    when(ovImporter.prefersOldSerialized()).thenReturn(true);
    EntryEventImpl e = createEntryEvent(region, null);
    byte[] oldValueBytes = new byte[] {1, 2, 3};
    e.setSerializedOldValue(oldValueBytes);

    e.exportOldValue(ovImporter);

    verify(ovImporter).importOldBytes(oldValueBytes, true);
  }

  @Test
  public void verifyExportOldValueWithCacheDeserializableByteArray() {
    LocalRegion region = mock(LocalRegion.class);

    InternalCache cache = mock(InternalCache.class);
    InternalDistributedSystem ids = mock(InternalDistributedSystem.class);
    when(region.getCache()).thenReturn(cache);
    when(cache.getDistributedSystem()).thenReturn(ids);

    CachedDeserializable oldValue = mock(CachedDeserializable.class);
    byte[] oldValueBytes = new byte[] {1, 2, 3};
    when(oldValue.getValue()).thenReturn(oldValueBytes);
    OldValueImporter ovImporter = mock(OldValueImporter.class);
    EntryEventImpl e = createEntryEvent(region, null);
    e.setOldValue(oldValue);

    e.exportOldValue(ovImporter);

    verify(ovImporter).importOldBytes(oldValueBytes, true);
  }

  @Test
  public void verifyExportOldValueWithCacheDeserializableString() {
    LocalRegion region = mock(LocalRegion.class);
    CachedDeserializable oldValue = mock(CachedDeserializable.class);
    Object oldValueObj = "oldValueObj";

    InternalCache cache = mock(InternalCache.class);
    InternalDistributedSystem ids = mock(InternalDistributedSystem.class);
    when(region.getCache()).thenReturn(cache);
    when(cache.getDistributedSystem()).thenReturn(ids);

    when(oldValue.getValue()).thenReturn(oldValueObj);
    OldValueImporter ovImporter = mock(OldValueImporter.class);
    EntryEventImpl e = createEntryEvent(region, null);
    e.setOldValue(oldValue);

    e.exportOldValue(ovImporter);

    verify(ovImporter).importOldObject(oldValueObj, true);
  }

  @Test
  public void verifyExportOldValueWithCacheDeserializableOk() {
    LocalRegion region = mock(LocalRegion.class);
    CachedDeserializable oldValue = mock(CachedDeserializable.class);
    Object oldValueObj = "oldValueObj";

    InternalCache cache = mock(InternalCache.class);
    InternalDistributedSystem ids = mock(InternalDistributedSystem.class);
    when(region.getCache()).thenReturn(cache);
    when(cache.getDistributedSystem()).thenReturn(ids);

    when(oldValue.getValue()).thenReturn(oldValueObj);
    OldValueImporter ovImporter = mock(OldValueImporter.class);
    when(ovImporter.isCachedDeserializableValueOk()).thenReturn(true);
    EntryEventImpl e = createEntryEvent(region, null);
    e.setOldValue(oldValue);

    e.exportOldValue(ovImporter);

    verify(ovImporter).importOldObject(oldValue, true);
  }

  @Test
  public void setOldValueUnforcedWithRemoveTokenChangesOldValueToNull() {
    LocalRegion region = mock(LocalRegion.class);

    InternalCache cache = mock(InternalCache.class);
    InternalDistributedSystem ids = mock(InternalDistributedSystem.class);
    when(region.getCache()).thenReturn(cache);
    when(cache.getDistributedSystem()).thenReturn(ids);

    EntryEventImpl e = createEntryEvent(region, null);
    String UNINITIALIZED = "Uninitialized";
    e.basicSetOldValue(UNINITIALIZED);
    e.setOldValue(Token.REMOVED_PHASE1, false);
    assertEquals(null, e.basicGetOldValue());
  }

  @Test
  public void setOldValueForcedWithRemoveTokenChangesOldValueToNull() {
    LocalRegion region = mock(LocalRegion.class);

    InternalCache cache = mock(InternalCache.class);
    InternalDistributedSystem ids = mock(InternalDistributedSystem.class);
    when(region.getCache()).thenReturn(cache);
    when(cache.getDistributedSystem()).thenReturn(ids);

    EntryEventImpl e = createEntryEvent(region, null);
    String UNINITIALIZED = "Uninitialized";
    e.basicSetOldValue(UNINITIALIZED);
    e.setOldValue(Token.REMOVED_PHASE1, true);
    assertEquals(null, e.basicGetOldValue());
  }

  @Test
  public void setOldValueUnforcedWithInvalidTokenNullsOldValue() {
    LocalRegion region = mock(LocalRegion.class);

    InternalCache cache = mock(InternalCache.class);
    InternalDistributedSystem ids = mock(InternalDistributedSystem.class);
    when(region.getCache()).thenReturn(cache);
    when(cache.getDistributedSystem()).thenReturn(ids);

    EntryEventImpl e = createEntryEvent(region, null);
    String UNINITIALIZED = "Uninitialized";
    e.basicSetOldValue(UNINITIALIZED);
    e.setOldValue(Token.INVALID, false);
    assertEquals(null, e.basicGetOldValue());
  }

  @Test
  public void setOldValueForcedWithInvalidTokenNullsOldValue() {
    LocalRegion region = mock(LocalRegion.class);

    InternalCache cache = mock(InternalCache.class);
    InternalDistributedSystem ids = mock(InternalDistributedSystem.class);
    when(region.getCache()).thenReturn(cache);
    when(cache.getDistributedSystem()).thenReturn(ids);

    EntryEventImpl e = createEntryEvent(region, null);
    String UNINITIALIZED = "Uninitialized";
    e.basicSetOldValue(UNINITIALIZED);
    e.setOldValue(Token.INVALID, true);
    assertEquals(null, e.basicGetOldValue());
  }

  @Test
  public void setOldValueUnforcedWithNullChangesOldValueToNull() {
    LocalRegion region = mock(LocalRegion.class);

    InternalCache cache = mock(InternalCache.class);
    InternalDistributedSystem ids = mock(InternalDistributedSystem.class);
    when(region.getCache()).thenReturn(cache);
    when(cache.getDistributedSystem()).thenReturn(ids);

    EntryEventImpl e = createEntryEvent(region, null);
    String UNINITIALIZED = "Uninitialized";
    e.basicSetOldValue(UNINITIALIZED);
    e.setOldValue(null, false);
    assertEquals(null, e.basicGetOldValue());
  }

  @Test
  public void setOldValueForcedWithNullChangesOldValueToNull() {
    LocalRegion region = mock(LocalRegion.class);

    InternalCache cache = mock(InternalCache.class);
    InternalDistributedSystem ids = mock(InternalDistributedSystem.class);
    when(region.getCache()).thenReturn(cache);
    when(cache.getDistributedSystem()).thenReturn(ids);

    EntryEventImpl e = createEntryEvent(region, null);
    String UNINITIALIZED = "Uninitialized";
    e.basicSetOldValue(UNINITIALIZED);
    e.setOldValue(null, true);
    assertEquals(null, e.basicGetOldValue());
  }

  @Test
  public void setOldValueForcedWithNotAvailableTokenSetsOldValue() {
    LocalRegion region = mock(LocalRegion.class);

    InternalCache cache = mock(InternalCache.class);
    InternalDistributedSystem ids = mock(InternalDistributedSystem.class);
    when(region.getCache()).thenReturn(cache);
    when(cache.getDistributedSystem()).thenReturn(ids);

    EntryEventImpl e = createEntryEvent(region, null);
    String UNINITIALIZED = "Uninitialized";
    e.basicSetOldValue(UNINITIALIZED);
    e.setOldValue(Token.NOT_AVAILABLE, true);
    assertEquals(Token.NOT_AVAILABLE, e.basicGetOldValue());
  }

  @Test
  public void setOldValueUnforcedWithNotAvailableTokenSetsOldValue() {
    LocalRegion region = mock(LocalRegion.class);

    InternalCache cache = mock(InternalCache.class);
    InternalDistributedSystem ids = mock(InternalDistributedSystem.class);
    when(region.getCache()).thenReturn(cache);
    when(cache.getDistributedSystem()).thenReturn(ids);

    EntryEventImpl e = createEntryEvent(region, null);
    String UNINITIALIZED = "Uninitialized";
    e.basicSetOldValue(UNINITIALIZED);
    e.setOldValue(Token.NOT_AVAILABLE, false);
    assertEquals(Token.NOT_AVAILABLE, e.basicGetOldValue());
  }

  @Test
  public void setOldUnforcedValueSetsOldValue() {
    LocalRegion region = mock(LocalRegion.class);

    InternalCache cache = mock(InternalCache.class);
    InternalDistributedSystem ids = mock(InternalDistributedSystem.class);
    when(region.getCache()).thenReturn(cache);
    when(cache.getDistributedSystem()).thenReturn(ids);

    EntryEventImpl e = createEntryEvent(region, null);
    String UNINITIALIZED = "Uninitialized";
    e.basicSetOldValue(UNINITIALIZED);
    e.setOldValue("oldValue", false);
    assertEquals("oldValue", e.basicGetOldValue());
  }

  @Test
  public void setOldValueForcedSetsOldValue() {
    LocalRegion region = mock(LocalRegion.class);

    InternalCache cache = mock(InternalCache.class);
    InternalDistributedSystem ids = mock(InternalDistributedSystem.class);
    when(region.getCache()).thenReturn(cache);
    when(cache.getDistributedSystem()).thenReturn(ids);

    EntryEventImpl e = createEntryEvent(region, null);
    String UNINITIALIZED = "Uninitialized";
    e.basicSetOldValue(UNINITIALIZED);
    e.setOldValue("oldValue", true);
    assertEquals("oldValue", e.basicGetOldValue());
  }

  @Test
  public void setOldValueUnforcedWithDisabledSetsNotAvailable() {
    EntryEventImpl e = new EntryEventImplWithOldValuesDisabled();
    String UNINITIALIZED = "Uninitialized";
    e.basicSetOldValue(UNINITIALIZED);
    e.setOldValue("oldValue", false);
    assertEquals(Token.NOT_AVAILABLE, e.basicGetOldValue());
  }

  @Test
  public void setOldValueForcedWithDisabledSetsOldValue() {
    EntryEventImpl e = new EntryEventImplWithOldValuesDisabled();
    String UNINITIALIZED = "Uninitialized";
    e.basicSetOldValue(UNINITIALIZED);
    e.setOldValue("oldValue", true);
    assertEquals("oldValue", e.basicGetOldValue());
  }

  @Test
  public void testGetEventTimeWithNullVersionTag() {
    long timestamp = System.currentTimeMillis();
    LocalRegion region = mock(LocalRegion.class);
    when(region.cacheTimeMillis()).thenReturn(timestamp);

    InternalCache cache = mock(InternalCache.class);
    InternalDistributedSystem ids = mock(InternalDistributedSystem.class);
    when(region.getCache()).thenReturn(cache);
    when(cache.getDistributedSystem()).thenReturn(ids);

    EntryEventImpl e = createEntryEvent(region, null);
    assertThat(e.getEventTime(0l)).isEqualTo(timestamp);
  }

  @Test
  public void testGetEventTimeWithVersionTagConcurrencyChecksEnabled() {
    long timestamp = System.currentTimeMillis();
    LocalRegion region = mock(LocalRegion.class);
    when(region.getConcurrencyChecksEnabled()).thenReturn(true);

    InternalCache cache = mock(InternalCache.class);
    InternalDistributedSystem ids = mock(InternalDistributedSystem.class);
    when(region.getCache()).thenReturn(cache);
    when(cache.getDistributedSystem()).thenReturn(ids);

    EntryEventImpl e = createEntryEvent(region, null);
    VersionTag tag = VersionTag.create(mock(InternalDistributedMember.class));
    tag.setVersionTimeStamp(timestamp);
    e.setVersionTag(tag);
    assertThat(e.getEventTime(0l)).isEqualTo(timestamp);
  }

  @Test
  public void testGetEventTimeWithVersionTagConcurrencyChecksEnabledWithSuggestedTime() {
    long timestamp = System.currentTimeMillis();
    long timestampPlus1 = timestamp + 1000l;
    long timestampPlus2 = timestamp + 2000l;
    LocalRegion region = mock(LocalRegion.class);
    when(region.getConcurrencyChecksEnabled()).thenReturn(true);
    when(region.cacheTimeMillis()).thenReturn(timestamp);

    InternalCache cache = mock(InternalCache.class);
    InternalDistributedSystem ids = mock(InternalDistributedSystem.class);
    when(region.getCache()).thenReturn(cache);
    when(cache.getDistributedSystem()).thenReturn(ids);

    EntryEventImpl e = createEntryEvent(region, null);
    VersionTag tag = VersionTag.create(mock(InternalDistributedMember.class));
    tag.setVersionTimeStamp(timestampPlus1);
    e.setVersionTag(tag);
    assertThat(e.getEventTime(timestampPlus2)).isEqualTo(timestampPlus2);
    assertThat(tag.getVersionTimeStamp()).isEqualTo(timestampPlus2);
  }

  @Test
  public void testGetEventTimeWithVersionTagConcurrencyChecksDisabledNoSuggestedTime() {
    long timestamp = System.currentTimeMillis();
    LocalRegion region = mock(LocalRegion.class);
    when(region.getConcurrencyChecksEnabled()).thenReturn(false);
    when(region.cacheTimeMillis()).thenReturn(timestamp);

    InternalCache cache = mock(InternalCache.class);
    InternalDistributedSystem ids = mock(InternalDistributedSystem.class);
    when(region.getCache()).thenReturn(cache);
    when(cache.getDistributedSystem()).thenReturn(ids);

    EntryEventImpl e = createEntryEvent(region, null);
    VersionTag tag = VersionTag.create(mock(InternalDistributedMember.class));
    tag.setVersionTimeStamp(timestamp + 1000l);
    e.setVersionTag(tag);
    assertThat(e.getEventTime(0l)).isEqualTo(timestamp);
  }

  @Test
  public void testGetEventTimeWithVersionTagConcurrencyChecksDisabledWithSuggestedTime() {
    long timestamp = System.currentTimeMillis();
    LocalRegion region = mock(LocalRegion.class);
    when(region.getConcurrencyChecksEnabled()).thenReturn(false);

    InternalCache cache = mock(InternalCache.class);
    InternalDistributedSystem ids = mock(InternalDistributedSystem.class);
    when(region.getCache()).thenReturn(cache);
    when(cache.getDistributedSystem()).thenReturn(ids);

    EntryEventImpl e = createEntryEvent(region, null);
    VersionTag tag = VersionTag.create(mock(InternalDistributedMember.class));
    tag.setVersionTimeStamp(timestamp + 1000l);
    e.setVersionTag(tag);
    assertThat(e.getEventTime(timestamp)).isEqualTo(timestamp);
  }

  @Test
  public void shouldRecalculateSize_returnsTrue_ifGetForceRecalculateSizeIsTrue_andDELTAS_RECALCULATE_SIZEisTrue() {
    GemFireCacheImpl.DELTAS_RECALCULATE_SIZE = true;
    Delta deltaValue = mock(Delta.class);
    when(deltaValue.getForceRecalculateSize())
        .thenReturn(true);

    boolean value = EntryEventImpl.shouldRecalculateSize(deltaValue);

    assertThat(value).isTrue();
  }

  @Test
  public void shouldRecalculateSize_returnsTrue_ifDELTAS_RECALCULATE_SIZEisTrue_andGetForceRecalculateSizeIsFalse() {
    GemFireCacheImpl.DELTAS_RECALCULATE_SIZE = true;
    Delta deltaValue = mock(Delta.class);
    when(deltaValue.getForceRecalculateSize())
        .thenReturn(false);

    boolean value = EntryEventImpl.shouldRecalculateSize(deltaValue);

    assertThat(value).isTrue();
  }

  @Test
  public void shouldRecalculateSize_returnsTrue_ifGetForceRecalculateSizeIsTrue_andDELTAS_RECALCULATE_SIZEIsFalse() {
    GemFireCacheImpl.DELTAS_RECALCULATE_SIZE = false;
    Delta deltaValue = mock(Delta.class);
    when(deltaValue.getForceRecalculateSize())
        .thenReturn(true);

    boolean value = EntryEventImpl.shouldRecalculateSize(deltaValue);

    assertThat(value).isTrue();
  }


  @Test
  public void shouldRecalculateSize_returnsFalse_ifBothDELTAS_RECALCULATE_SIZEIsFalse_andGetForceRecalculateSizeIsFalse() {
    GemFireCacheImpl.DELTAS_RECALCULATE_SIZE = false;
    Delta deltaValue = mock(Delta.class);
    when(deltaValue.getForceRecalculateSize())
        .thenReturn(false);

    boolean value = EntryEventImpl.shouldRecalculateSize(deltaValue);

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

  }

  private static EventID createEventID() {
    byte[] memId = {1, 2, 3};
    return new EventID(memId, 11, 12, 13);
  }

  private EntryEventImpl createEntryEvent(LocalRegion l, Object newValue) {
    // create an event
    EntryEventImpl event = EntryEventImpl.create(l, Operation.CREATE, key, newValue, null,
        false /* origin remote */, null, false /* generateCallbacks */, createEventID());
    // avoid calling invokeCallbacks
    event.callbacksInvoked(true);

    return event;
  }
}
