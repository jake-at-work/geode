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

import static org.apache.geode.cache.Region.SEPARATOR;
import static org.apache.geode.distributed.ConfigurationProperties.CACHE_XML_FILE;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.NAME;
import static org.apache.geode.test.util.ResourceUtils.createTempFileFromResource;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.util.Arrays;
import java.util.Properties;

import org.junit.After;
import org.junit.Test;

import org.apache.geode.SystemFailure;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.DiskStore;
import org.apache.geode.cache.Region;
import org.apache.geode.distributed.DistributedSystem;

/**
 * This test is for testing Disk attributes set via xml.
 *
 * A cache and region are created using an xml. The regions are then verified to make sure that all
 * the attributes have been correctly set
 *
 * @since GemFire 5.1
 */
public class DiskRegCacheXmlJUnitTest {

  private Cache cache = null;

  private DistributedSystem ds = null;

  private static File[] dirs = null;

  private void mkDirAndConnectDs() {
    var file1 = new File("d1");
    file1.mkdir();
    file1.deleteOnExit();
    var file2 = new File("d2");
    file2.mkdir();
    file2.deleteOnExit();
    var file3 = new File("d3");
    file3.mkdir();
    file3.deleteOnExit();
    dirs = new File[3];
    dirs[0] = file1;
    dirs[1] = file2;
    dirs[2] = file3;
    // Connect to the GemFire distributed system
    var props = new Properties();
    props.setProperty(NAME, "test");
    var path =
        createTempFileFromResource(getClass(), "DiskRegCacheXmlJUnitTest.xml")
            .getAbsolutePath();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(CACHE_XML_FILE, path);
    ds = DistributedSystem.connect(props);
    // Create the cache which causes the cache-xml-file to be parsed
    cache = CacheFactory.create(ds);
  }

  @Test
  public void testDiskRegCacheXml() throws Exception {
    mkDirAndConnectDs();
    // Get the region1 which is a subregion of /root
    Region region1 = cache.getRegion(SEPARATOR + "root1" + SEPARATOR + "PersistSynchRollingOplog1");
    DiskStore ds = ((LocalRegion) region1).getDiskStore();
    if (ds != null) {
      if (!Arrays.equals(dirs, ds.getDiskDirs())) {
        fail("expected=" + Arrays.toString(dirs) + " actual=" + ds.getDiskDirs());
      }
    } else {
      if (!Arrays.equals(dirs, region1.getAttributes().getDiskDirs())) {
        fail("expected=" + Arrays.toString(dirs) + " actual="
            + region1.getAttributes().getDiskDirs());
      }
    }

    var ra1 = region1.getAttributes();
    assertTrue(ra1.isDiskSynchronous() == true);
    var ds1 = cache.findDiskStore(((LocalRegion) region1).getDiskStoreName());
    assertTrue(ds1 != null);
    assertTrue(ds1.getAutoCompact() == true);
    assertTrue(ds1.getMaxOplogSize() == 2);

    // Get the region2 which is a subregion of /root
    Region region2 = cache.getRegion(SEPARATOR + "root2" + SEPARATOR + "PersistSynchFixedOplog2");
    var ra2 = region2.getAttributes();
    assertTrue(ra2.isDiskSynchronous() == true);
    var ds2 = cache.findDiskStore(((LocalRegion) region2).getDiskStoreName());
    assertTrue(ds2 != null);
    assertTrue(ds2.getAutoCompact() == false);
    assertTrue(ds2.getMaxOplogSize() == 0);

    // Get the region3 which is a subregion of /root
    Region region3 =
        cache.getRegion(SEPARATOR + "root3" + SEPARATOR + "PersistASynchBufferRollingOplog3");
    var ra3 = region3.getAttributes();
    assertTrue(ra3.isDiskSynchronous() == false);
    var ds3 = cache.findDiskStore(((LocalRegion) region3).getDiskStoreName());
    assertTrue(ds3 != null);
    assertTrue(ds3.getAutoCompact() == true);
    assertTrue(ds3.getMaxOplogSize() == 2);
    assertTrue(ds3.getQueueSize() == 10000);
    assertTrue(ds3.getTimeInterval() == 15);

    // Get the region4 which is a subregion of /root
    Region region4 =
        cache.getRegion(SEPARATOR + "root4" + SEPARATOR + "PersistASynchNoBufferFixedOplog4");
    var ra4 = region4.getAttributes();
    assertTrue(ra4.isDiskSynchronous() == false);
    var ds4 = cache.findDiskStore(((LocalRegion) region4).getDiskStoreName());
    assertTrue(ds4 != null);
    assertTrue(ds4.getAutoCompact() == false);
    assertTrue(ds4.getMaxOplogSize() == 2);
    assertTrue(ds4.getQueueSize() == 0);

    // Get the region5 which is a subregion of /root
    Region region5 =
        cache.getRegion(SEPARATOR + "root5" + SEPARATOR + "OverflowSynchRollingOplog5");
    var ra5 = region5.getAttributes();
    assertTrue(ra5.isDiskSynchronous() == true);
    var ds5 = cache.findDiskStore(((LocalRegion) region5).getDiskStoreName());
    assertTrue(ds5 != null);
    assertTrue(ds5.getAutoCompact() == true);
    assertTrue(ds5.getMaxOplogSize() == 2);

    // Get the region6 which is a subregion of /root
    Region region6 = cache.getRegion(SEPARATOR + "root6" + SEPARATOR + "OverflowSynchFixedOplog6");
    var ra6 = region6.getAttributes();
    assertTrue(ra6.isDiskSynchronous() == true);
    var ds6 = cache.findDiskStore(((LocalRegion) region6).getDiskStoreName());
    assertTrue(ds6 != null);
    assertTrue(ds6.getAutoCompact() == false);
    assertTrue(ds6.getMaxOplogSize() == 0);

    // Get the region7 which is a subregion of /root
    Region region7 =
        cache.getRegion(SEPARATOR + "root7" + SEPARATOR + "OverflowASynchBufferRollingOplog7");
    var ra7 = region7.getAttributes();
    assertTrue(ra7.isDiskSynchronous() == false);
    var ds7 = cache.findDiskStore(((LocalRegion) region7).getDiskStoreName());
    assertTrue(ds7 != null);
    assertTrue(ds7.getAutoCompact() == true);
    assertTrue(ds7.getMaxOplogSize() == 2);

    // Get the region8 which is a subregion of /root
    Region region8 =
        cache.getRegion(SEPARATOR + "root8" + SEPARATOR + "OverflowASynchNoBufferFixedOplog8");
    var ra8 = region8.getAttributes();
    assertTrue(ra8.isDiskSynchronous() == false);
    var ds8 = cache.findDiskStore(((LocalRegion) region8).getDiskStoreName());
    assertTrue(ds8 != null);
    assertTrue(ds8.getAutoCompact() == false);

    // Get the region9 which is a subregion of /root
    Region region9 =
        cache.getRegion(SEPARATOR + "root9" + SEPARATOR + "PersistOverflowSynchRollingOplog9");
    var ra9 = region9.getAttributes();
    assertTrue(ra9.isDiskSynchronous() == true);
    var ds9 = cache.findDiskStore(((LocalRegion) region9).getDiskStoreName());
    assertTrue(ds9 != null);
    assertTrue(ds9.getAutoCompact() == true);
    assertTrue(ds9.getMaxOplogSize() == 2);

    // Get the region10 which is a subregion of /root
    Region region10 =
        cache.getRegion(SEPARATOR + "root10" + SEPARATOR + "PersistOverflowSynchFixedOplog10");
    var ra10 = region10.getAttributes();
    assertTrue(ra10.isDiskSynchronous() == true);
    var ds10 = cache.findDiskStore(((LocalRegion) region10).getDiskStoreName());
    assertTrue(ds10 != null);
    assertTrue(ds10.getAutoCompact() == false);

    // Get the region11 which is a subregion of /root
    Region region11 = cache
        .getRegion(SEPARATOR + "root11" + SEPARATOR + "PersistOverflowASynchBufferRollingOplog11");
    var ra11 = region11.getAttributes();
    assertTrue(ra11.isDiskSynchronous() == false);
    var ds11 = cache.findDiskStore(((LocalRegion) region11).getDiskStoreName());
    assertTrue(ds11 != null);
    assertTrue(ds11.getAutoCompact() == true);
    assertTrue(ds11.getMaxOplogSize() == 2);

    // Get the region12 which is a subregion of /root
    Region region12 = cache
        .getRegion(SEPARATOR + "root12" + SEPARATOR + "PersistOverflowASynchNoBufferFixedOplog12");
    var ra12 = region12.getAttributes();
    assertTrue(ra12.isDiskSynchronous() == false);
    var ds12 = cache.findDiskStore(((LocalRegion) region12).getDiskStoreName());
    assertTrue(ds12 != null);
    assertTrue(ds12.getTimeInterval() == 15);
    assertTrue(ds12.getQueueSize() == 0);

    deleteFiles();
  }

  private static void deleteFiles() {
    for (final var dir : dirs) {
      var files = dir.listFiles();
      for (final var file : files) {
        file.delete();
      }
    }
  }

  @After
  public void tearDown() throws Exception {
    try {
      if (cache != null && !cache.isClosed()) {
        for (final var region : cache.rootRegions()) {
          var root = (Region) region;
          if (root.isDestroyed() || root instanceof HARegion) {
            continue;
          }
          try {
            root.localDestroyRegion("teardown");
          } catch (VirtualMachineError e) {
            SystemFailure.initiateFailure(e);
            throw e;
          } catch (Throwable t) {
            ds.getLogWriter().error(t);
          }
        }
      }
    } finally {
      closeCache();
    }
  }

  /** Close the cache */
  private synchronized void closeCache() {
    if (cache != null) {
      try {
        if (!cache.isClosed()) {
          var txMgr = cache.getCacheTransactionManager();
          if (txMgr != null) {
            if (txMgr.exists()) {
              // make sure we cleanup this threads txid stored in a thread local
              txMgr.rollback();
            }
          }
          cache.close();
        }
      } finally {
        cache = null;
      }
    }
  }
}// end of DiskRegCacheXmlJUnitTest
