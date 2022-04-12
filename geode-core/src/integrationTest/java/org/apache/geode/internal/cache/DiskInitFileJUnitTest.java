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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.EnumSet;
import java.util.concurrent.locks.ReentrantLock;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.apache.geode.CancelCriterion;
import org.apache.geode.Statistics;
import org.apache.geode.StatisticsFactory;
import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.DiskAccessException;
import org.apache.geode.internal.cache.persistence.DiskRegionView;
import org.apache.geode.internal.cache.persistence.DiskStoreID;

public class DiskInitFileJUnitTest {
  private DiskStoreImpl mockedDiskStoreImpl;
  private DiskRegionView mockDiskRegionView;

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Before
  public void setUp() throws Exception {
    var testDirectory = temporaryFolder.newFolder("_" + getClass().getSimpleName());

    // Mock statistics factory for creating directory holders.
    final var mockStatisticsFactory = mock(StatisticsFactory.class);
    when(mockStatisticsFactory.createStatistics(any(), anyString()))
        .thenReturn(mock(Statistics.class));

    // Mock disk store impl. All we need to do is return this init file directory.
    mockedDiskStoreImpl = mock(DiskStoreImpl.class);
    var holder = new DirectoryHolder(mockStatisticsFactory, testDirectory, 0, 0);
    when(mockedDiskStoreImpl.getInfoFileDir()).thenReturn(holder);
    when(mockedDiskStoreImpl.getDiskStoreID()).thenReturn(mock(DiskStoreID.class));
    when(mockedDiskStoreImpl.getBackupLock()).thenReturn(mock(ReentrantLock.class));

    // Mock required for the init file so it doesn't delete the file when the init file is closed.
    mockDiskRegionView = spy(DiskRegionView.class);
    when(mockDiskRegionView.getName()).thenReturn("diskRegionView");
    when(mockDiskRegionView.getPartitionName()).thenReturn("diskRegionViewPartition");
    when(mockDiskRegionView.getFlags())
        .thenReturn(EnumSet.noneOf(DiskInitFile.DiskRegionFlag.class));
  }

  /**
   * Test the behavior of canonical ids in the init file.
   */
  @Test
  public void testCanonicalIds() {
    // Create an init file and add some canonical ids.
    var dif =
        new DiskInitFile("testFile", mockedDiskStoreImpl, false, Collections.emptySet());
    assertThat(dif.getCanonicalObject(5)).isNull();
    assertThat(dif.getCanonicalObject(0)).isNull();
    var id1 = dif.getOrCreateCanonicalId("object1");
    var id2 = dif.getOrCreateCanonicalId("object2");
    assertThat(dif.getCanonicalObject(id1)).isEqualTo("object1");
    assertThat(dif.getCanonicalObject(id2)).isEqualTo("object2");
    assertThat(dif.getOrCreateCanonicalId("object2")).isEqualTo(id2);
    dif.createRegion(mockDiskRegionView);

    // Close the init file and recover the init file from disk
    dif.close();
    dif = new DiskInitFile("testFile", mockedDiskStoreImpl, true, Collections.emptySet());

    // Make sure we can recover the ids from disk
    assertThat(dif.getCanonicalObject(id1)).isEqualTo("object1");
    assertThat(dif.getCanonicalObject(id2)).isEqualTo("object2");
    assertThat(dif.getOrCreateCanonicalId("object2")).isEqualTo(id2);

    // Make sure we can add new ids
    var id3 = dif.getOrCreateCanonicalId("object3");
    assertThat(id3).isGreaterThan(id2);
    assertThat(dif.getCanonicalObject(id1)).isEqualTo("object1");
    assertThat(dif.getCanonicalObject(id2)).isEqualTo("object2");
    assertThat(dif.getCanonicalObject(id3)).isEqualTo("object3");

    dif.close();
  }

  @Test
  public void testKrfIds() {
    var dif =
        new DiskInitFile("testKrfIds", mockedDiskStoreImpl, false, Collections.emptySet());
    assertThat(dif.hasKrf(1)).isFalse();
    dif.cmnKrfCreate(1);
    assertThat(dif.hasKrf(1)).isTrue();
    assertThat(dif.hasKrf(2)).isFalse();
    dif.cmnKrfCreate(2);
    assertThat(dif.hasKrf(2)).isTrue();
    dif.createRegion(mockDiskRegionView);
    dif.forceCompaction();
    dif.close();

    dif = new DiskInitFile("testKrfIds", mockedDiskStoreImpl, true, Collections.emptySet());
    assertThat(dif.hasKrf(1)).isTrue();
    assertThat(dif.hasKrf(2)).isTrue();
    dif.cmnCrfDelete(1);
    assertThat(dif.hasKrf(1)).isFalse();
    assertThat(dif.hasKrf(2)).isTrue();
    dif.cmnCrfDelete(2);
    assertThat(dif.hasKrf(2)).isFalse();
    dif.createRegion(mockDiskRegionView);
    dif.forceCompaction();
    dif.close();

    dif = new DiskInitFile("testKrfIds", mockedDiskStoreImpl, true, Collections.emptySet());
    assertThat(dif.hasKrf(1)).isFalse();
    assertThat(dif.hasKrf(2)).isFalse();
    dif.destroy();
  }

  @Test
  public void markInitializedThrowsDiskAccessExceptionWhenInitFileClosedAndParentAndCacheNotClosing() {
    markInitializedTestSetup();

    var diskInitFile =
        new DiskInitFile("testThrows", mockedDiskStoreImpl, false, Collections.emptySet());
    diskInitFile.close();

    assertThatThrownBy(() -> diskInitFile.markInitialized(mockDiskRegionView)).isInstanceOf(
        DiskAccessException.class);
  }

  @Test
  public void markInitializedThrowsCacheClosedExceptionWhenInitFileClosedAndParentIsClosedOrClosing() {
    markInitializedTestSetup();
    when(mockedDiskStoreImpl.isClosed()).thenReturn(Boolean.TRUE);

    var diskInitFile =
        new DiskInitFile("testThrows", mockedDiskStoreImpl, false, Collections.emptySet());
    diskInitFile.close();

    assertThatThrownBy(() -> diskInitFile.markInitialized(mockDiskRegionView)).isInstanceOf(
        CacheClosedException.class);
  }

  @Test
  public void markInitializedThrowsCacheClosedExceptionWhenCacheIsClosing() {
    var cancelCriterion = markInitializedTestSetup();
    var cacheClosedException = new CacheClosedException("boom");
    doThrow(cacheClosedException).when(cancelCriterion).checkCancelInProgress();

    var diskInitFile =
        new DiskInitFile("testThrows", mockedDiskStoreImpl, false, Collections.emptySet());
    diskInitFile.close();

    assertThatThrownBy(() -> diskInitFile.markInitialized(mockDiskRegionView)).isEqualTo(
        cacheClosedException);
  }

  @Test
  public void markInitializedCacheCloseIsCalledWhenParentHandlesDiskAccessException() {
    markInitializedTestSetup();

    var diskInitFile =
        new DiskInitFile("testThrows", mockedDiskStoreImpl, false, Collections.emptySet());
    diskInitFile.close();

    assertThatThrownBy(() -> diskInitFile.markInitialized(mockDiskRegionView))
        .isInstanceOf(DiskAccessException.class);
    verify(mockedDiskStoreImpl, times(1)).handleDiskAccessException(any(DiskAccessException.class));
  }

  private CancelCriterion markInitializedTestSetup() {
    var internalCache = mock(InternalCache.class);
    var cancelCriterion = mock(CancelCriterion.class);
    var diskRegion = mock(DiskRegion.class);

    when(mockedDiskStoreImpl.getCache()).thenReturn(internalCache);
    when(mockedDiskStoreImpl.getById(anyLong())).thenReturn(diskRegion);
    when(internalCache.getCancelCriterion()).thenReturn(cancelCriterion);

    return cancelCriterion;
  }
}
