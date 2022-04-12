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
package org.apache.geode.internal.statistics;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.junit.Test;

import org.apache.geode.internal.NanoTimer;
import org.apache.geode.internal.util.StopWatch;

/**
 * Unit tests for {@link StatMonitorHandler}.
 *
 * @since GemFire 7.0
 */
public class StatMonitorHandlerTest {

  @Test
  public void testAddNewMonitor() throws Exception {
    var handler = new StatMonitorHandler();
    assertTrue(handler.getMonitorsSnapshot().isEmpty());
    var monitor = new TestStatisticsMonitor();
    assertTrue(handler.addMonitor(monitor));
    assertFalse(handler.getMonitorsSnapshot().isEmpty());
    assertTrue(handler.getMonitorsSnapshot().contains(monitor));
    handler.sampled(NanoTimer.getTime(), Collections.emptyList());
    waitForNotificationCount(monitor, 1, 2 * 1000, 10, false);
    assertEquals(1, monitor.getNotificationCount());
  }

  @Test
  public void testAddExistingMonitorReturnsFalse() throws Exception {
    var handler = new StatMonitorHandler();
    assertTrue(handler.getMonitorsSnapshot().isEmpty());
    StatisticsMonitor monitor = new TestStatisticsMonitor();
    assertTrue(handler.addMonitor(monitor));
    assertFalse(handler.getMonitorsSnapshot().isEmpty());
    assertTrue(handler.getMonitorsSnapshot().contains(monitor));
    assertFalse(handler.addMonitor(monitor));
  }

  @Test
  public void testRemoveExistingMonitor() throws Exception {
    var handler = new StatMonitorHandler();
    assertTrue(handler.getMonitorsSnapshot().isEmpty());
    var monitor = new TestStatisticsMonitor();
    assertTrue(handler.addMonitor(monitor));
    assertFalse(handler.getMonitorsSnapshot().isEmpty());
    assertTrue(handler.getMonitorsSnapshot().contains(monitor));
    assertTrue(handler.removeMonitor(monitor));
    assertFalse(handler.getMonitorsSnapshot().contains(monitor));
    assertTrue(handler.getMonitorsSnapshot().isEmpty());
    handler.sampled(NanoTimer.getTime(), Collections.emptyList());
    assertEquals(0, monitor.getNotificationCount());
  }

  @Test
  public void testRemoveMissingMonitorReturnsFalse() throws Exception {
    var handler = new StatMonitorHandler();
    assertTrue(handler.getMonitorsSnapshot().isEmpty());
    StatisticsMonitor monitor = new TestStatisticsMonitor();
    assertFalse(handler.getMonitorsSnapshot().contains(monitor));
    assertFalse(handler.removeMonitor(monitor));
    assertTrue(handler.getMonitorsSnapshot().isEmpty());
  }

  @Test
  public void testNotificationSampleFrequencyDefault() throws Exception {
    final var sampleFrequency = 1;
    var handler = new StatMonitorHandler();
    var monitor = new TestStatisticsMonitor();
    handler.addMonitor(monitor);
    final var sampleCount = 100;
    for (var i = 0; i < sampleCount; i++) {
      handler.sampled(NanoTimer.getTime(), Collections.emptyList());
      waitForNotificationCount(monitor, 1 + i, 2 * 1000, 10, false);
    }
    assertEquals(sampleCount / sampleFrequency, monitor.getNotificationCount());
  }

  @Test
  public void testNotificationSampleTimeMillis() throws Exception {
    final var currentTime = System.currentTimeMillis();
    var handler = new StatMonitorHandler();
    var monitor = new TestStatisticsMonitor();
    handler.addMonitor(monitor);
    var nanoTimeStamp = NanoTimer.getTime();
    handler.sampled(nanoTimeStamp, Collections.emptyList());
    waitForNotificationCount(monitor, 1, 2 * 1000, 10, false);
    assertTrue(monitor.getTimeStamp() != nanoTimeStamp);
    assertTrue(monitor.getTimeStamp() >= currentTime);
  }

  @Test
  public void testNotificationResourceInstances() throws Exception {
    final var resourceInstanceCount = 100;
    final List<ResourceInstance> resourceInstances = new ArrayList<>();
    for (var i = 0; i < resourceInstanceCount; i++) {
      resourceInstances.add(new ResourceInstance(i, null, null));
    }

    var handler = new StatMonitorHandler();
    var monitor = new TestStatisticsMonitor();
    handler.addMonitor(monitor);
    handler.sampled(NanoTimer.getTime(), Collections.unmodifiableList(resourceInstances));

    waitForNotificationCount(monitor, 1, 2 * 1000, 10, false);

    final var notificationResourceInstances = monitor.getResourceInstances();
    assertNotNull(notificationResourceInstances);
    assertEquals(resourceInstances, notificationResourceInstances);
    assertEquals(resourceInstanceCount, notificationResourceInstances.size());

    var i = 0;
    for (var resourceInstance : notificationResourceInstances) {
      assertEquals(i, resourceInstance.getId());
      i++;
    }
  }

  private static void waitForNotificationCount(final TestStatisticsMonitor monitor,
      final int expected, long ms, long interval, boolean throwOnTimeout)
      throws InterruptedException {
    var done = false;
    for (var time = new StopWatch(true); !done && time.elapsedTimeMillis() < ms; done =
        (monitor.getNotificationCount() >= expected)) {
      Thread.sleep(interval);
    }
    if (throwOnTimeout) {
      assertTrue("waiting for notification count to be " + expected, done);
    }
  }

  /**
   * @since GemFire 7.0
   */
  private static class TestStatisticsMonitor extends StatisticsMonitor {
    private volatile long timeStamp;
    private volatile List<ResourceInstance> resourceInstances;
    private volatile int notificationCount;

    public TestStatisticsMonitor() {
      super();
    }

    @Override
    protected void monitor(long timeStamp, List<ResourceInstance> resourceInstances) {
      this.timeStamp = timeStamp;
      this.resourceInstances = resourceInstances;
      notificationCount++;
    }

    long getTimeStamp() {
      return timeStamp;
    }

    List<ResourceInstance> getResourceInstances() {
      return resourceInstances;
    }

    int getNotificationCount() {
      return notificationCount;
    }
  }
}
