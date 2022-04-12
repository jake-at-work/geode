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
package org.apache.geode.internal.cache.diskPerf;

import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.LOG_LEVEL;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;

import java.io.File;
import java.util.Properties;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.EvictionAction;
import org.apache.geode.cache.EvictionAttributes;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.Scope;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.internal.cache.DiskStoreFactoryImpl;
import org.apache.geode.test.dunit.ThreadUtils;

public class MultiThreadedOplogPerJUnitPerformanceTest {

  @Rule
  public TestName name = new TestName();

  protected Region region;

  private static File[] dirs;

  private final int numberOfThreads = 3;

  private volatile int counter = 0;

  private volatile long totalTime = 0;

  private volatile int startPoint = 0;

  private final int numberOfKeysPerThread = 1000; // 1000

  private final int numberOfIterations = 5; // 50

  public MultiThreadedOplogPerJUnitPerformanceTest() {
    var file0 = new File("testingDirectory");
    file0.mkdir();
    file0.deleteOnExit();
    var file1 = new File("testingDirectory/" + name.getMethodName() + "1");
    file1.mkdir();
    file1.deleteOnExit();
    var file2 = new File("testingDirectory/" + name.getMethodName() + "2");
    file2.mkdir();
    file2.deleteOnExit();
    var file3 = new File("testingDirectory/" + name.getMethodName() + "3");
    file3.mkdir();
    file3.deleteOnExit();
    var file4 = new File("testingDirectory/" + name.getMethodName() + "4");
    file4.mkdir();
    file4.deleteOnExit();
    dirs = new File[4];
    dirs[0] = file1;
    dirs[1] = file2;
    dirs[2] = file3;
    dirs[3] = file4;
    deleteFiles();
  }

  /**
   * cleans all the directory of all the files present in them
   *
   */
  protected static void deleteFiles() {
    for (var i = 0; i < 4; i++) {
      var files = dirs[i].listFiles();
      if (files != null) {
        for (final var file : files) {
          file.delete();
        }
      }
    }
  }

  @Test
  public void testPerf() {
    var props = new Properties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "");
    props.setProperty(LOG_LEVEL, "info");
    var ds = DistributedSystem.connect(props);
    Cache cache = null;
    try {
      cache = CacheFactory.create(ds);
    } catch (Exception e) {
      e.printStackTrace();
    }
    var dsf = cache.createDiskStoreFactory();
    var factory = new AttributesFactory();
    factory.setPersistBackup(false);
    factory.setScope(Scope.LOCAL);
    factory.setEvictionAttributes(
        EvictionAttributes.createLRUEntryAttributes(10, EvictionAction.OVERFLOW_TO_DISK));
    // Properties props1 = new Properties();
    factory.setDiskSynchronous(true);
    dsf.setAutoCompact(false);
    ((DiskStoreFactoryImpl) dsf).setMaxOplogSizeInBytes(200000000);
    dsf.setDiskDirs(dirs);
    factory.setDiskStoreName(dsf.create("perfTestRegion").getName());
    try {
      region = cache.createVMRegion("perfTestRegion", factory.createRegionAttributes());
    } catch (Exception e) {
      e.printStackTrace();
    }

    var threads = new Thread[numberOfThreads];

    for (var i = 0; i < numberOfThreads; i++) {
      threads[i] = new Thread(new Writer(i)); // .start();
      threads[i].start();
    }

    for (var i = 0; i < numberOfThreads; i++) {
      ThreadUtils.join(threads[i], 30 * 1000);
    }

    var totalPuts = ((long) numberOfIterations * numberOfKeysPerThread * numberOfThreads);

    System.out.println(" total puts is " + totalPuts);
    System.out.println(" total time in milliseconds is " + totalTime);
    System.out
        .println(" writes per second is " + (totalPuts * 1000 * numberOfThreads) / (totalTime));
    region.destroyRegion();
  }

  synchronized void increaseCounter() {
    counter++;
  }

  synchronized void increaseTotalTime(long time) {
    totalTime = (totalTime + time);
  }

  synchronized int getStartPoint() {
    startPoint++;
    return startPoint;
  }

  class Writer implements Runnable {
    private int num = 0;

    private final byte[] bytes;

    Writer(int num1) {
      bytes = new byte[1024];
      bytes[0] = 1;
      bytes[1000] = 2;
      num = num1 * 10000;

    }

    @Override
    public void run() {
      long startTime, endTime;
      startTime = System.currentTimeMillis();
      var startPoint = getStartPoint();
      startPoint = startPoint * numberOfKeysPerThread;
      for (var j = 0; j < numberOfIterations; j++) {
        for (var i = 0; i < numberOfKeysPerThread; i++) {
          region.put((i + 1) + num, bytes);
          /*
           * DiskRegion dr =((LocalRegion)region).getDiskRegion();
           *
           * DiskEntry entry = (DiskEntry)(((LocalRegion)region).basicGetEntry(new
           * Integer((i+1)+num))); try{ byte[] val = (byte[])dr.getNoBuffer(entry.getDiskId());
           * }catch(Exception e) {
           *
           * System.out.print("EROR. "+ " Count ="+i ); e.printStackTrace(); }
           */

        }
      }
      endTime = System.currentTimeMillis();
      var time = (endTime - startTime);
      increaseTotalTime(time);
      increaseCounter();
    }
  }
}
