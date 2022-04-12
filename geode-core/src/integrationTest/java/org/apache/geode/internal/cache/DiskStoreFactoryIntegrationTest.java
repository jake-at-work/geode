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

import static org.apache.geode.cache.DiskStoreFactory.DEFAULT_DISK_DIR_SIZE;
import static org.apache.geode.distributed.ConfigurationProperties.ENABLE_TIME_STATISTICS;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.LOG_LEVEL;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.STATISTIC_ARCHIVE_FILE;
import static org.apache.geode.distributed.ConfigurationProperties.STATISTIC_SAMPLING_ENABLED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.DiskStore;
import org.apache.geode.cache.DiskStoreFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;

/**
 * Tests DiskStoreFactory
 */
public class DiskStoreFactoryIntegrationTest {

  private static Cache cache = null;
  private static final Properties props = new Properties();

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  static {
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "");
    props.setProperty(LOG_LEVEL, "config"); // to keep diskPerf logs smaller
    props.setProperty(STATISTIC_SAMPLING_ENABLED, "true");
    props.setProperty(ENABLE_TIME_STATISTICS, "true");
  }

  @Before
  public void setUp() throws Exception {
    props.setProperty(STATISTIC_ARCHIVE_FILE,
        temporaryFolder.getRoot().getAbsolutePath() + File.separator + "stats.gfs");
    createCache();
  }

  private Cache createCache() {
    cache = new CacheFactory(props).create();

    return cache;
  }

  @After
  public void tearDown() throws Exception {
    cache.close();
  }

  /**
   * Test method for 'org.apache.geode.cache.DiskWriteAttributes.getDefaultInstance()'
   */
  @Test
  public void testGetDefaultInstance() {
    var dsf = cache.createDiskStoreFactory();
    var name = "testGetDefaultInstance";
    assertThat(cache.findDiskStore(name)).isNull();
    var ds = dsf.create(name);

    assertThat(cache.findDiskStore(name)).isEqualTo(ds);
    assertThat(ds.getName()).isEqualTo(name);
    assertThat(ds.getAutoCompact()).isEqualTo(DiskStoreFactory.DEFAULT_AUTO_COMPACT);
    assertThat(ds.getCompactionThreshold())
        .isEqualTo(DiskStoreFactory.DEFAULT_COMPACTION_THRESHOLD);
    assertThat(ds.getAllowForceCompaction())
        .isEqualTo(DiskStoreFactory.DEFAULT_ALLOW_FORCE_COMPACTION);
    assertThat(ds.getMaxOplogSize()).isEqualTo(DiskStoreFactory.DEFAULT_MAX_OPLOG_SIZE);
    assertThat(ds.getTimeInterval()).isEqualTo(DiskStoreFactory.DEFAULT_TIME_INTERVAL);
    assertThat(ds.getWriteBufferSize()).isEqualTo(DiskStoreFactory.DEFAULT_WRITE_BUFFER_SIZE);
    assertThat(ds.getQueueSize()).isEqualTo(DiskStoreFactory.DEFAULT_QUEUE_SIZE);
    assertThat(Arrays.equals(ds.getDiskDirs(), DiskStoreFactory.DEFAULT_DISK_DIRS))
        .as("expected=" + Arrays.toString(DiskStoreFactory.DEFAULT_DISK_DIRS) + " had="
            + Arrays.toString(ds.getDiskDirs()))
        .isTrue();
    assertThat(Arrays.equals(ds.getDiskDirSizes(), DiskStoreFactory.DEFAULT_DISK_DIR_SIZES))
        .as("expected=" + Arrays.toString(DiskStoreFactory.DEFAULT_DISK_DIR_SIZES) + " had="
            + Arrays.toString(ds.getDiskDirSizes()))
        .isTrue();
  }

  @Test
  public void testNonDefaults() {
    var dsf = cache.createDiskStoreFactory();
    var name = "testNonDefaults";
    var ds = dsf.setAutoCompact(!DiskStoreFactory.DEFAULT_AUTO_COMPACT)
        .setCompactionThreshold(DiskStoreFactory.DEFAULT_COMPACTION_THRESHOLD / 2)
        .setAllowForceCompaction(!DiskStoreFactory.DEFAULT_ALLOW_FORCE_COMPACTION)
        .setMaxOplogSize(DiskStoreFactory.DEFAULT_MAX_OPLOG_SIZE + 1)
        .setTimeInterval(DiskStoreFactory.DEFAULT_TIME_INTERVAL + 1)
        .setWriteBufferSize(DiskStoreFactory.DEFAULT_WRITE_BUFFER_SIZE + 1)
        .setQueueSize(DiskStoreFactory.DEFAULT_QUEUE_SIZE + 1).create(name);

    assertThat(ds.getAutoCompact()).isEqualTo(!DiskStoreFactory.DEFAULT_AUTO_COMPACT);
    assertThat(ds.getCompactionThreshold())
        .isEqualTo(DiskStoreFactory.DEFAULT_COMPACTION_THRESHOLD / 2);
    assertThat(ds.getAllowForceCompaction())
        .isEqualTo(!DiskStoreFactory.DEFAULT_ALLOW_FORCE_COMPACTION);
    assertThat(ds.getMaxOplogSize()).isEqualTo(DiskStoreFactory.DEFAULT_MAX_OPLOG_SIZE + 1);
    assertThat(ds.getTimeInterval()).isEqualTo(DiskStoreFactory.DEFAULT_TIME_INTERVAL + 1);
    assertThat(ds.getWriteBufferSize()).isEqualTo(DiskStoreFactory.DEFAULT_WRITE_BUFFER_SIZE + 1);
    assertThat(ds.getQueueSize()).isEqualTo(DiskStoreFactory.DEFAULT_QUEUE_SIZE + 1);
  }

  @Test
  public void testCompactionThreshold() {
    var dsf = cache.createDiskStoreFactory();
    var name = "testCompactionThreshold1";
    var ds = dsf.setCompactionThreshold(0).create(name);
    assertThat(ds.getCompactionThreshold()).isEqualTo(0);
    name = "testCompactionThreshold2";
    ds = dsf.setCompactionThreshold(100).create(name);
    assertThat(ds.getCompactionThreshold()).isEqualTo(100);

    // check illegal stuff
    assertThatThrownBy(() -> dsf.setCompactionThreshold(-1))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> dsf.setCompactionThreshold(101))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void testQueueSize() {
    var dsf = cache.createDiskStoreFactory();
    var name = "testQueueSize";
    var ds = dsf.setQueueSize(0).create(name);
    assertThat(ds.getQueueSize()).isEqualTo(0);
    name = "testQueueSize2";
    ds = dsf.setQueueSize(Integer.MAX_VALUE).create(name);
    assertThat(ds.getQueueSize()).isEqualTo(Integer.MAX_VALUE);

    // check illegal stuff
    assertThatThrownBy(() -> dsf.setQueueSize(-1)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void testWriteBufferSize() {
    var dsf = cache.createDiskStoreFactory();
    var name = "testWriteBufferSize";
    var ds = dsf.setWriteBufferSize(0).create(name);
    assertThat(ds.getWriteBufferSize()).isEqualTo(0);
    name = "testWriteBufferSize2";
    ds = dsf.setWriteBufferSize(Integer.MAX_VALUE).create(name);
    assertThat(ds.getWriteBufferSize()).isEqualTo(Integer.MAX_VALUE);

    // check illegal stuff
    assertThatThrownBy(() -> dsf.setWriteBufferSize(-1))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void testTimeInterval() {
    var dsf = cache.createDiskStoreFactory();
    var name = "testTimeInterval";
    var ds = dsf.setTimeInterval(0).create(name);
    assertThat(ds.getTimeInterval()).isEqualTo(0);
    name = "testTimeInterval2";
    ds = dsf.setTimeInterval(Long.MAX_VALUE).create(name);
    assertThat(ds.getTimeInterval()).isEqualTo(Long.MAX_VALUE);

    // check illegal stuff
    assertThatThrownBy(() -> dsf.setTimeInterval(-1)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void testMaxOplogSize() {
    var dsf = cache.createDiskStoreFactory();
    var name = "testMaxOplogSize";
    var ds = dsf.setMaxOplogSize(0).create(name);
    assertThat(ds.getMaxOplogSize()).isEqualTo(0);
    name = "testMaxOplogSize2";
    var max = Long.MAX_VALUE / (1024 * 1024);
    ds = dsf.setMaxOplogSize(max).create(name);
    assertThat(ds.getMaxOplogSize()).isEqualTo(max);

    // check illegal stuff
    assertThatThrownBy(() -> dsf.setMaxOplogSize(-1)).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> dsf.setMaxOplogSize(max + 1))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void testFlush() {
    var dsf = cache.createDiskStoreFactory();
    var name = "testFlush";
    var ds = dsf.create(name);
    ds.flush();
  }

  @Test
  public void testForceRoll() {
    var dsf = cache.createDiskStoreFactory();
    var name = "testForceRoll";
    var ds = dsf.create(name);
    ds.forceRoll();
  }

  @Test
  public void testDestroyWithPersistentRegion() {
    var dsf = cache.createDiskStoreFactory();
    var name = "testDestroy";
    var ds = dsf.create(name);
    Region region = cache.createRegionFactory(RegionShortcut.LOCAL_PERSISTENT)
        .setDiskStoreName("testDestroy").create("region");
    assertThatThrownBy(ds::destroy).isInstanceOf(IllegalStateException.class);

    // This should now work
    region.destroyRegion();
    assertThatCode(ds::destroy).doesNotThrowAnyException();
  }

  @Test
  public void testDestroyWithClosedRegion() {
    var dsf = cache.createDiskStoreFactory();
    var name = "testDestroy";
    var ds = dsf.create(name);
    Region region = cache.createRegionFactory(RegionShortcut.LOCAL_PERSISTENT)
        .setDiskStoreName("testDestroy").create("region");

    // This should now work
    region.close();
    assertThatCode(ds::destroy).doesNotThrowAnyException();
  }

  @Test
  public void testDestroyWithOverflowRegion() {
    var dsf = cache.createDiskStoreFactory();
    var name = "testDestroy";
    var ds = dsf.create(name);

    Region region = cache.createRegionFactory(RegionShortcut.LOCAL_OVERFLOW)
        .setDiskStoreName("testDestroy").create("region");
    assertThatThrownBy(ds::destroy).isInstanceOf(IllegalStateException.class);

    // The destroy should now work.
    region.close();
    assertThatCode(ds::destroy).doesNotThrowAnyException();
  }

  @Test
  public void testForceCompaction() {
    var dsf = cache.createDiskStoreFactory();
    dsf.setAllowForceCompaction(true);
    var name = "testForceCompaction";
    var ds = dsf.create(name);
    assertThat(ds.forceCompaction()).isFalse();
  }

  @Test
  @SuppressWarnings("deprecation")
  public void testMissingInitFile() {
    var dsf = cache.createDiskStoreFactory();
    var name = "testMissingInitFile";
    var diskStore = dsf.create(name);
    var ifFile = new File(diskStore.getDiskDirs()[0], "BACKUP" + name + DiskInitFile.IF_FILE_EXT);
    assertThat(ifFile.exists()).isTrue();
    var af = new AttributesFactory<String, String>();
    af.setDiskStoreName(name);
    af.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
    cache.createRegion("r", af.create());
    cache.close();
    assertThat(ifFile.exists()).isTrue();
    assertThat(ifFile.delete()).isTrue();
    assertThat(ifFile.exists()).isFalse();
    cache = createCache();
    dsf = cache.createDiskStoreFactory();
    assertThat(cache.findDiskStore(name)).isNull();

    try {
      dsf.create(name);
      fail("expected IllegalStateException");
    } catch (IllegalStateException ignored) {
    }
    // if test passed clean up files
    removeFiles(diskStore);
  }

  private void removeFiles(DiskStore diskStore) {
    final var diskStoreName = diskStore.getName();
    var dirs = diskStore.getDiskDirs();

    for (var dir : dirs) {
      var files = dir.listFiles((file, name) -> name.startsWith("BACKUP" + diskStoreName));
      assertThat(files).isNotNull();
      Arrays.stream(files).forEach(file -> assertThat(file.delete()).isTrue());
    }
  }

  @Test
  @SuppressWarnings("deprecation")
  public void testMissingCrfFile() {
    var dsf = cache.createDiskStoreFactory();
    var name = "testMissingCrfFile";
    var diskStore = dsf.create(name);
    var crfFile = new File(diskStore.getDiskDirs()[0], "BACKUP" + name + "_1.crf");
    var af = new AttributesFactory<String, String>();
    af.setDiskStoreName(name);
    af.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
    var r = cache.createRegion("r", af.create());
    r.put("key", "value");
    assertThat(crfFile.exists()).isTrue();
    cache.close();
    assertThat(crfFile.exists()).isTrue();
    assertThat(crfFile.delete()).isTrue();
    assertThat(crfFile.exists()).isFalse();
    cache = createCache();
    dsf = cache.createDiskStoreFactory();
    assertThat(cache.findDiskStore(name)).isNull();

    try {
      dsf.create(name);
      fail("expected IllegalStateException");
    } catch (IllegalStateException ignored) {
    }
    // if test passed clean up files
    removeFiles(diskStore);
  }

  @Test
  @SuppressWarnings("deprecation")
  public void testMissingDrfFile() {
    var dsf = cache.createDiskStoreFactory();
    var name = "testMissingDrfFile";
    var diskStore = dsf.create(name);
    var drfFile = new File(diskStore.getDiskDirs()[0], "BACKUP" + name + "_1.drf");
    var af = new AttributesFactory<String, String>();
    af.setDiskStoreName(name);
    af.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
    var r = cache.createRegion("r", af.create());
    r.put("key", "value");
    assertThat(drfFile.exists()).isTrue();
    cache.close();
    assertThat(drfFile.exists()).isTrue();
    assertThat(drfFile.delete()).isTrue();
    assertThat(drfFile.exists()).isFalse();
    cache = createCache();
    dsf = cache.createDiskStoreFactory();
    assertThat(cache.findDiskStore(name)).isNull();

    try {
      dsf.create(name);
      fail("expected IllegalStateException");
    } catch (IllegalStateException ignored) {
    }
    // if test passed clean up files
    removeFiles(diskStore);
  }

  @Test
  @SuppressWarnings("deprecation")
  public void testRedefiningDefaultDiskStore() {
    var dsf = cache.createDiskStoreFactory();
    dsf.setAutoCompact(!DiskStoreFactory.DEFAULT_AUTO_COMPACT);
    assertThat(cache.findDiskStore(DiskStoreFactory.DEFAULT_DISK_STORE_NAME)).isNull();
    var diskStore = dsf.create(DiskStoreFactory.DEFAULT_DISK_STORE_NAME);
    var af = new AttributesFactory<String, String>();
    af.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
    var r = cache.createRegion("r", af.create());
    r.put("key", "value");
    DiskStore ds = ((LocalRegion) r).getDiskStore();
    assertThat(cache.findDiskStore(DiskStoreFactory.DEFAULT_DISK_STORE_NAME)).isEqualTo(ds);
    assertThat(ds.getName()).isEqualTo(DiskStoreFactory.DEFAULT_DISK_STORE_NAME);
    assertThat(ds.getAutoCompact()).isEqualTo(!DiskStoreFactory.DEFAULT_AUTO_COMPACT);
    cache.close();
    // if test passed clean up files
    removeFiles(diskStore);
  }

  @Test
  public void failedDiskStoreInitialRecoveryCleansUpDiskStore() {
    var dsf = cache.createDiskStoreFactory();
    var diskStore = mock(DiskStoreImpl.class);
    doThrow(RuntimeException.class).when(diskStore).doInitialRecovery();
    var threwException = false;
    try {
      ((DiskStoreFactoryImpl) dsf).initializeDiskStore(diskStore);
    } catch (RuntimeException e) {
      threwException = true;
    }
    assertThat(threwException).isTrue();
    verify(diskStore, times(1)).close();
  }

  @Test
  public void checkIfDirectoriesExistShouldCreateDirectoriesWhenTheyDoNotExist()
      throws IOException {
    var diskDirs = new File[3];
    diskDirs[0] = new File(
        temporaryFolder.getRoot().getAbsolutePath() + File.separator + "randomNonExistingDiskDir");
    diskDirs[1] = temporaryFolder.newFolder("existingDiskDir");
    diskDirs[2] = new File(temporaryFolder.getRoot().getAbsolutePath() + File.separator
        + "anotherRandomNonExistingDiskDir");

    assertThat(Files.exists(diskDirs[0].toPath())).isFalse();
    assertThat(Files.exists(diskDirs[1].toPath())).isTrue();
    assertThat(Files.exists(diskDirs[2].toPath())).isFalse();
    DiskStoreFactoryImpl.checkIfDirectoriesExist(diskDirs);
    Arrays.stream(diskDirs).forEach(diskDir -> assertThat(Files.exists(diskDir.toPath())).isTrue());
  }

  @Test
  public void setDiskDirsShouldInitializeInternalMetadataAndCreateDirectoriesWhenTheyDoNotExist()
      throws IOException {
    var diskDirs = new File[3];
    diskDirs[0] =
        new File(temporaryFolder.getRoot().getAbsolutePath() + File.separator + "randomDiskDir");
    diskDirs[1] = temporaryFolder.newFolder("existingDiskDir");
    diskDirs[2] = new File(
        temporaryFolder.getRoot().getAbsolutePath() + File.separator + "anotherRandomDiskDir");
    assertThat(Files.exists(diskDirs[0].toPath())).isFalse();
    assertThat(Files.exists(diskDirs[1].toPath())).isTrue();
    assertThat(Files.exists(diskDirs[2].toPath())).isFalse();

    var factoryImpl =
        (DiskStoreFactoryImpl) cache.createDiskStoreFactory().setDiskDirs(diskDirs);
    Arrays.stream(diskDirs).forEach(diskDir -> assertThat(Files.exists(diskDir.toPath())).isTrue());
    assertThat(factoryImpl.getDiskStoreAttributes().diskDirs).isEqualTo(diskDirs);
    assertThat(factoryImpl.getDiskStoreAttributes().diskDirSizes)
        .isEqualTo(new int[] {DEFAULT_DISK_DIR_SIZE, DEFAULT_DISK_DIR_SIZE, DEFAULT_DISK_DIR_SIZE});
  }
}
