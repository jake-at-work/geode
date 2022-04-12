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
package org.apache.geode.internal.stats50;

import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.STATISTIC_SAMPLING_ENABLED;
import static org.junit.Assert.assertEquals;

import java.util.Properties;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Test;

import org.apache.geode.StatisticDescriptor;
import org.apache.geode.Statistics;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.internal.statistics.StatisticsTypeFactoryImpl;

public class AtomicStatsJUnitTest {

  /**
   * Test for bug 41340. Do two gets at the same time of a dirty stat, and make sure we get the
   * correct value for the stat.
   *
   */
  @Test
  public void testConcurrentGets() throws Throwable {

    var props = new Properties();
    props.setProperty(MCAST_PORT, "0");
    // props.setProperty("statistic-sample-rate", "60000");
    props.setProperty(STATISTIC_SAMPLING_ENABLED, "false");
    var ds = DistributedSystem.connect(props);

    var statName = "TestStats";
    var statDescription = "Tests stats";

    final var statDesc = "blah blah blah";

    var f = StatisticsTypeFactoryImpl.singleton();

    var type = f.createType(statName, statDescription, new StatisticDescriptor[] {
        f.createIntGauge("stat", statDesc, "bottles of beer on the wall"),});

    final var statId = type.nameToId("stat");

    try {

      final var statsRef = new AtomicReference<Statistics>();
      final var beforeIncrement = new CyclicBarrier(3);
      final var afterIncrement = new CyclicBarrier(3);
      var thread1 = new Thread("thread1") {
        @Override
        public void run() {
          try {
            while (true) {
              beforeIncrement.await();
              statsRef.get().incInt(statId, 1);
              afterIncrement.await();
            }
          } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
          } catch (BrokenBarrierException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
          }
        }
      };
      var thread3 = new Thread("thread1") {
        @Override
        public void run() {
          try {
            while (true) {
              beforeIncrement.await();
              afterIncrement.await();
              statsRef.get().getInt(statId);
            }
          } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
          } catch (BrokenBarrierException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
          }
        }
      };
      thread1.start();
      thread3.start();
      for (var i = 0; i < 5000; i++) {
        var stats = ds.createAtomicStatistics(type, "stats");
        statsRef.set(stats);
        beforeIncrement.await();
        afterIncrement.await();
        assertEquals("On loop " + i, 1, stats.getInt(statId));
        stats.close();
      }

    } finally {
      ds.disconnect();
    }
  }
}
