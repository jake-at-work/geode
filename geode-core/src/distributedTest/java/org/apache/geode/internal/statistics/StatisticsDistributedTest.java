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

import static org.apache.geode.distributed.ConfigurationProperties.STATISTIC_ARCHIVE_FILE;
import static org.apache.geode.distributed.ConfigurationProperties.STATISTIC_SAMPLE_RATE;
import static org.apache.geode.distributed.ConfigurationProperties.STATISTIC_SAMPLING_ENABLED;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.dunit.Assert.assertEquals;
import static org.apache.geode.test.dunit.Assert.assertFalse;
import static org.apache.geode.test.dunit.Assert.assertNotNull;
import static org.apache.geode.test.dunit.Assert.assertTrue;
import static org.apache.geode.test.dunit.Assert.fail;
import static org.apache.geode.test.dunit.Host.getHost;
import static org.apache.geode.test.dunit.Invoke.invokeInEveryVM;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.regex.Pattern;

import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.StatisticDescriptor;
import org.apache.geode.Statistics;
import org.apache.geode.StatisticsFactory;
import org.apache.geode.StatisticsType;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheListener;
import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionEvent;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.util.CacheListenerAdapter;
import org.apache.geode.cache.util.RegionMembershipListenerAdapter;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.NanoTimer;
import org.apache.geode.internal.statistics.StatArchiveReader.ResourceInst;
import org.apache.geode.internal.statistics.StatArchiveReader.StatSpec;
import org.apache.geode.internal.statistics.StatArchiveReader.StatValue;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;
import org.apache.geode.test.junit.categories.StatisticsTest;
import org.apache.geode.test.junit.rules.serializable.SerializableTemporaryFolder;

/**
 * Distributed tests for {@link Statistics}.
 *
 * <p>
 * VM0 performs puts and VM1 receives updates. Both use custom statistics for start/end with
 * increment to add up puts and updates. Then validation tests values in stat resource instances and
 * uses StatArchiveReader. Both are tested against static counters in both VMs.
 *
 * <p>
 * This test mimics hydratest/locators/cacheDS.conf in an attempt to reproduce bug #45478. So far
 * this test passes consistently.
 *
 * @since GemFire 7.0
 */
@Category({StatisticsTest.class})
@SuppressWarnings({"rawtypes", "serial", "unused"})
public class StatisticsDistributedTest extends JUnit4CacheTestCase {

  private static final int MAX_PUTS = 1000;
  private static final int NUM_KEYS = 100;
  private static final int NUM_PUB_THREADS = 2;
  private static final int NUM_PUBS = 2;
  private static final boolean RANDOMIZE_PUTS = true;

  private static final AtomicInteger updateEvents = new AtomicInteger();
  private static final AtomicInteger puts = new AtomicInteger();
  private static final AtomicReference<PubSubStats> subStatsRef = new AtomicReference<>();
  private static final AtomicReferenceArray<PubSubStats> pubStatsRef =
      new AtomicReferenceArray<>(NUM_PUB_THREADS);
  private static final AtomicReference<RegionMembershipListener> rmlRef = new AtomicReference<>();

  private File directory;

  @Rule
  public SerializableTemporaryFolder temporaryFolder = new SerializableTemporaryFolder();

  @Override
  public final void postSetUp() throws Exception {
    directory = temporaryFolder.getRoot();
  }

  @Override
  public final void preTearDownCacheTestCase() throws Exception {
    invokeInEveryVM(StatisticsDistributedTest::cleanup);
    disconnectAllFromDS(); // because this test enabled stat sampling!
  }

  @Test
  public void testPubAndSubCustomStats() throws Exception {
    var regionName = "region_" + getName();
    var pubs = new VM[NUM_PUBS];
    for (var pubVM = 0; pubVM < NUM_PUBS; pubVM++) {
      pubs[pubVM] = getHost(0).getVM(pubVM);
    }
    var sub = getHost(0).getVM(NUM_PUBS);

    for (var pub : pubs) {
      pub.invoke(() -> puts.set(0));
    }

    var subArchive =
        directory.getAbsolutePath() + File.separator + getName() + "_sub" + ".gfs";
    var pubArchives = new String[NUM_PUBS];
    for (var pubVM = 0; pubVM < NUM_PUBS; pubVM++) {
      pubArchives[pubVM] =
          directory.getAbsolutePath() + File.separator + getName() + "_pub-" + pubVM + ".gfs";
    }

    for (var i = 0; i < NUM_PUBS; i++) {
      final var pubVM = i;
      pubs[pubVM].invoke("pub-connect-and-create-data-" + pubVM, () -> {
        var props = new Properties();
        props.setProperty(STATISTIC_SAMPLING_ENABLED, "true");
        props.setProperty(STATISTIC_SAMPLE_RATE, "1000");
        props.setProperty(STATISTIC_ARCHIVE_FILE, pubArchives[pubVM]);

        var system = getSystem(props);

        // assert that sampler is working as expected
        var sampler = system.getStatSampler();
        assertTrue(sampler.isSamplingEnabled());
        assertTrue(sampler.isAlive());
        assertEquals(new File(pubArchives[pubVM]), sampler.getArchiveFileName());

        await("awaiting SampleCollector to exist")
            .until(() -> sampler.getSampleCollector() != null);

        var sampleCollector = sampler.getSampleCollector();
        assertNotNull(sampleCollector);

        var archiveHandler = sampleCollector.getStatArchiveHandler();
        assertNotNull(archiveHandler);
        assertTrue(archiveHandler.isArchiving());

        // create cache and region
        Cache cache = getCache();
        RegionFactory<String, Number> factory = cache.createRegionFactory();
        factory.setScope(Scope.DISTRIBUTED_ACK);

        var rml = new RegionMembershipListener();
        rmlRef.set(rml);
        factory.addCacheListener(rml);
        var region = factory.create(regionName);

        // create the keys
        if (region.getAttributes().getScope() == Scope.DISTRIBUTED_ACK) {
          for (var key = 0; key < NUM_KEYS; key++) {
            region.create("KEY-" + key, null);
          }
        }
      });
    }

    DistributedMember subMember = sub.invoke("sub-connect-and-create-keys", () -> {
      var props = new Properties();
      props.setProperty(STATISTIC_SAMPLING_ENABLED, "true");
      props.setProperty(STATISTIC_SAMPLE_RATE, "1000");
      props.setProperty(STATISTIC_ARCHIVE_FILE, subArchive);

      var system = getSystem(props);

      var statistics = new PubSubStats(system, "sub-1", 1);
      subStatsRef.set(statistics);

      // assert that sampler is working as expected
      var sampler = system.getStatSampler();
      assertTrue(sampler.isSamplingEnabled());
      assertTrue(sampler.isAlive());
      assertEquals(new File(subArchive), sampler.getArchiveFileName());

      await("awaiting SampleCollector to exist")
          .until(() -> sampler.getSampleCollector() != null);

      var sampleCollector = sampler.getSampleCollector();
      assertNotNull(sampleCollector);

      var archiveHandler = sampleCollector.getStatArchiveHandler();
      assertNotNull(archiveHandler);
      assertTrue(archiveHandler.isArchiving());

      // create cache and region with UpdateListener
      Cache cache = getCache();
      RegionFactory<String, Number> factory = cache.createRegionFactory();
      factory.setScope(Scope.DISTRIBUTED_ACK);

      CacheListener<String, Number> cl = new UpdateListener(statistics);
      factory.addCacheListener(cl);
      var region = factory.create(regionName);

      // create the keys
      if (region.getAttributes().getScope() == Scope.DISTRIBUTED_ACK) {
        for (var key = 0; key < NUM_KEYS; key++) {
          region.create("KEY-" + key, null);
        }
      }

      assertEquals(0, statistics.getUpdateEvents());
      return system.getDistributedMember();
    });

    for (var i = 0; i < NUM_PUBS; i++) {
      final var pubVM = i;
      var publishers = new AsyncInvocation[NUM_PUB_THREADS];
      for (var j = 0; j < NUM_PUB_THREADS; j++) {
        final var pubThread = j;
        publishers[pubThread] = pubs[pubVM]
            .invokeAsync("pub-connect-and-put-data-" + pubVM + "-thread-" + pubThread, () -> {
              var statistics = new PubSubStats(basicGetSystem(), "pub-" + pubThread, pubVM);
              pubStatsRef.set(pubThread, statistics);

              var rml = rmlRef.get();
              Region<String, Number> region = getCache().getRegion(regionName);

              // assert that sub is in rml membership
              assertNotNull(rml);

              await("awaiting Membership to contain subMember")
                  .until(() -> rml.contains(subMember) && rml.size() == NUM_PUBS);

              // publish lots of puts cycling through the NUM_KEYS
              assertEquals(0, statistics.getPuts());

              // cycle through the keys randomly
              if (RANDOMIZE_PUTS) {
                var randomGenerator = new Random();
                var key = 0;
                for (var idx = 0; idx < MAX_PUTS; idx++) {
                  var start = statistics.startPut();
                  key = randomGenerator.nextInt(NUM_KEYS);
                  region.put("KEY-" + key, idx);
                  statistics.endPut(start);
                }

                // cycle through the keys in order and wrapping back around
              } else {
                var key = 0;
                for (var idx = 0; idx < MAX_PUTS; idx++) {
                  var start = statistics.startPut();
                  region.put("KEY-" + key, idx);
                  key++; // cycle through the keys...
                  if (key >= NUM_KEYS) {
                    key = 0;
                  }
                  statistics.endPut(start);
                }
              }
              assertEquals(MAX_PUTS, statistics.getPuts());

              // wait for 2 samples to ensure all stats have been archived
              var statSamplerType = getSystem().findType("StatSampler");
              var statsArray = getSystem().findStatisticsByType(statSamplerType);
              assertEquals(1, statsArray.length);

              var statSamplerStats = statsArray[0];
              var initialSampleCount = statSamplerStats.getInt(StatSamplerStats.SAMPLE_COUNT);

              await("awaiting sampleCount >= 2").until(() -> statSamplerStats
                  .getInt(StatSamplerStats.SAMPLE_COUNT) >= initialSampleCount + 2);
            });
      }

      for (final var publisher : publishers) {
        publisher.join();
        if (publisher.exceptionOccurred()) {
          fail("Test failed", publisher.getException());
        }
      }
    }

    sub.invoke("sub-wait-for-samples", () -> {
      // wait for 2 samples to ensure all stats have been archived
      var statSamplerType = getSystem().findType("StatSampler");
      var statsArray = getSystem().findStatisticsByType(statSamplerType);
      assertEquals(1, statsArray.length);

      var statSamplerStats = statsArray[0];
      var initialSampleCount = statSamplerStats.getInt(StatSamplerStats.SAMPLE_COUNT);

      await("awaiting sampleCount >= 2").until(
          () -> statSamplerStats.getInt(StatSamplerStats.SAMPLE_COUNT) >= initialSampleCount + 2);

      // now post total updateEvents to static
      var statistics = subStatsRef.get();
      assertNotNull(statistics);
      updateEvents.set(statistics.getUpdateEvents());
    });

    // validate pub values against sub values
    int totalUpdateEvents = sub.invoke(StatisticsDistributedTest::getUpdateEvents);

    // validate pub values against pub statistics against pub archive
    for (var i = 0; i < NUM_PUBS; i++) {
      final var pubIdx = i;
      pubs[pubIdx].invoke("pub-validation", () -> {
        // add up all the puts
        assertEquals(NUM_PUB_THREADS, pubStatsRef.length());
        var totalPuts = 0;
        for (var pubThreadIdx = 0; pubThreadIdx < NUM_PUB_THREADS; pubThreadIdx++) {
          var statistics = pubStatsRef.get(pubThreadIdx);
          assertNotNull(statistics);
          totalPuts += statistics.getPuts();
        }

        // assert that total puts adds up to max puts times num threads
        assertEquals(MAX_PUTS * NUM_PUB_THREADS, totalPuts);

        // assert that archive file contains same values as statistics
        var archive = new File(pubArchives[pubIdx]);
        assertTrue(archive.exists());

        var reader = new StatArchiveReader(new File[] {archive}, null, false);

        double combinedPuts = 0;

        var resources = reader.getResourceInstList();
        assertNotNull(resources);
        assertFalse(resources.isEmpty());

        for (var ri : (Iterable<ResourceInst>) resources) {
          if (!ri.getType().getName().equals(PubSubStats.TYPE_NAME)) {
            continue;
          }

          var statValues = ri.getStatValues();
          for (var idx = 0; idx < statValues.length; idx++) {
            var statName = ri.getType().getStats()[idx].getName();
            assertNotNull(statName);

            if (statName.equals(PubSubStats.PUTS)) {
              var sv = statValues[idx];
              sv.setFilter(StatValue.FILTER_NONE);

              var mostRecent = sv.getSnapshotsMostRecent();
              var min = sv.getSnapshotsMinimum();
              var max = sv.getSnapshotsMaximum();
              var maxMinusMin = sv.getSnapshotsMaximum() - sv.getSnapshotsMinimum();
              var mean = sv.getSnapshotsAverage();
              var stdDev = sv.getSnapshotsStandardDeviation();

              assertEquals(mostRecent, max, 0f);

              double summation = 0;
              var rawSnapshots = sv.getRawSnapshots();
              for (final var rawSnapshot : rawSnapshots) {
                summation += rawSnapshot;
              }
              assertEquals(mean, summation / sv.getSnapshotsSize(), 0);

              combinedPuts += mostRecent;
            }
          }
        }

        // assert that sum of mostRecent values for all puts equals totalPuts
        assertEquals(totalPuts, combinedPuts, 0);
        puts.getAndAdd(totalPuts);
      });
    }

    // validate pub values against sub values
    var totalCombinedPuts = 0;
    for (var i = 0; i < NUM_PUBS; i++) {
      var pubIdx = i;
      int totalPuts = pubs[pubIdx].invoke(StatisticsDistributedTest::getPuts);
      assertEquals(MAX_PUTS * NUM_PUB_THREADS, totalPuts);
      totalCombinedPuts += totalPuts;
    }
    assertEquals(totalCombinedPuts, totalUpdateEvents);
    assertEquals(MAX_PUTS * NUM_PUB_THREADS * NUM_PUBS, totalCombinedPuts);

    // validate sub values against sub statistics against sub archive
    final var totalPuts = totalCombinedPuts;
    sub.invoke("sub-validation", () -> {
      var statistics = subStatsRef.get();
      assertNotNull(statistics);
      var updateEvents = statistics.getUpdateEvents();
      assertEquals(totalPuts, updateEvents);
      assertEquals(totalUpdateEvents, updateEvents);
      assertEquals(MAX_PUTS * NUM_PUB_THREADS * NUM_PUBS, updateEvents);

      // assert that archive file contains same values as statistics
      var archive = new File(subArchive);
      assertTrue(archive.exists());

      var reader = new StatArchiveReader(new File[] {archive}, null, false);

      double combinedUpdateEvents = 0;

      var resources = reader.getResourceInstList();
      for (var ri : (Iterable<ResourceInst>) resources) {
        if (!ri.getType().getName().equals(PubSubStats.TYPE_NAME)) {
          continue;
        }

        var statValues = ri.getStatValues();
        for (var i = 0; i < statValues.length; i++) {
          var statName = ri.getType().getStats()[i].getName();
          assertNotNull(statName);

          if (statName.equals(PubSubStats.UPDATE_EVENTS)) {
            var sv = statValues[i];
            sv.setFilter(StatValue.FILTER_NONE);

            var mostRecent = sv.getSnapshotsMostRecent();
            var min = sv.getSnapshotsMinimum();
            var max = sv.getSnapshotsMaximum();
            var maxMinusMin = sv.getSnapshotsMaximum() - sv.getSnapshotsMinimum();
            var mean = sv.getSnapshotsAverage();
            var stdDev = sv.getSnapshotsStandardDeviation();

            assertEquals(mostRecent, max, 0);

            double summation = 0;
            var rawSnapshots = sv.getRawSnapshots();
            for (final var rawSnapshot : rawSnapshots) {
              summation += rawSnapshot;
            }
            assertEquals(mean, summation / sv.getSnapshotsSize(), 0);

            combinedUpdateEvents += mostRecent;
          }
        }
      }
      assertEquals(totalUpdateEvents, combinedUpdateEvents, 0);
    });

    int updateEvents =
        sub.invoke(() -> readIntStat(new File(subArchive), "PubSubStats", "updateEvents"));
    assertTrue(updateEvents > 0);
    assertEquals(MAX_PUTS * NUM_PUB_THREADS * NUM_PUBS, updateEvents);

    var puts = 0;
    for (var pubVM = 0; pubVM < NUM_PUBS; pubVM++) {
      var currentPubVM = pubVM;
      int vmPuts = pubs[pubVM]
          .invoke(() -> readIntStat(new File(pubArchives[currentPubVM]), "PubSubStats", "puts"));
      assertTrue(vmPuts > 0);
      assertEquals(MAX_PUTS * NUM_PUB_THREADS, vmPuts);
      puts += vmPuts;
    }
    assertTrue(puts > 0);
    assertEquals(MAX_PUTS * NUM_PUB_THREADS * NUM_PUBS, puts);

    // use regex "testPubAndSubCustomStats"

    var reader =
        new MultipleArchiveReader(directory, ".*" + getTestMethodName() + ".*\\.gfs");

    var combinedUpdateEvents = reader.readIntStat(PubSubStats.TYPE_NAME, PubSubStats.UPDATE_EVENTS);
    assertTrue("Failed to read updateEvents stat values", combinedUpdateEvents > 0);

    var combinedPuts = reader.readIntStat(PubSubStats.TYPE_NAME, PubSubStats.PUTS);
    assertTrue("Failed to read puts stat values", combinedPuts > 0);

    assertTrue("updateEvents is " + combinedUpdateEvents + " but puts is " + combinedPuts,
        combinedUpdateEvents == combinedPuts);
  }

  static int readIntStat(final File archive, final String typeName, final String statName)
      throws IOException {
    var reader = new MultipleArchiveReader(archive);
    return reader.readIntStat(typeName, statName);
  }

  /** invoked by reflection */
  private static void cleanup() {
    updateEvents.set(0);
    rmlRef.set(null);
  }

  /** invoked by reflection */
  private static int getUpdateEvents() {
    return updateEvents.get();
  }

  /** invoked by reflection */
  private static int getPuts() {
    return puts.get();
  }

  public static void main(final String[] args) throws Exception {
    if (args.length == 2) {
      final var statType = args[0];
      final var statName = args[1];

      var reader = new MultipleArchiveReader(new File("."));
      var value = reader.readIntStat(statType, statName);
      System.out.println(statType + "#" + statName + "=" + value);

    } else if (args.length == 3) {
      final var archiveName = args[0];
      final var statType = args[1];
      final var statName = args[2];

      var archive = new File(archiveName).getAbsoluteFile();
      assertTrue("File " + archive + " does not exist!", archive.exists());
      assertTrue(archive + " exists but is not a file!", archive.isFile());

      var reader = new MultipleArchiveReader(archive);
      var value = reader.readIntStat(statType, statName);
      System.out.println(archive + ": " + statType + "#" + statName + "=" + value);

    } else if (args.length == 4) {
      final var statType1 = args[0];
      final var statName1 = args[1];
      final var statType2 = args[2];
      final var statName2 = args[3];

      var reader = new MultipleArchiveReader(new File("."));
      var value1 = reader.readIntStat(statType1, statName1);
      var value2 = reader.readIntStat(statType2, statName2);

      assertTrue(statType1 + "#" + statName1 + "=" + value1 + " does not equal " + statType2 + "#"
          + statName2 + "=" + value2, value1 == value2);
    } else {
      assertEquals("Minimum two args are required: statType statName", 2, args.length);
    }
  }

  /**
   * @since GemFire 7.0
   */
  static class PubSubStats {

    private static final String TYPE_NAME = "PubSubStats";
    private static final String TYPE_DESCRIPTION =
        "Statistics for StatisticsDistributedTest with Pub/Sub.";

    private static final String INSTANCE_PREFIX = "pubSubStats_";

    private static final String PUTS = "puts";
    private static final String PUT_TIME = "putTime";

    private static final String UPDATE_EVENTS = "updateEvents";

    private static StatisticsType createType(final StatisticsFactory f) {
      var stf = StatisticsTypeFactoryImpl.singleton();
      var type = stf.createType(TYPE_NAME, TYPE_DESCRIPTION, createDescriptors(f));
      return type;
    }

    private static StatisticDescriptor[] createDescriptors(final StatisticsFactory f) {
      var largerIsBetter = true;
      return new StatisticDescriptor[] {
          f.createIntCounter(PUTS, "Number of puts completed.", "operations", largerIsBetter),
          f.createLongCounter(PUT_TIME, "Total time spent doing puts.", "nanoseconds",
              !largerIsBetter),
          f.createIntCounter(UPDATE_EVENTS, "Number of update events.", "events", largerIsBetter)};
    }

    private final Statistics statistics;

    PubSubStats(final StatisticsFactory f, final String name, final int id) {
      statistics = f.createAtomicStatistics(createType(f), INSTANCE_PREFIX + "_" + name, id);
    }

    Statistics statistics() {
      return statistics;
    }

    void close() {
      statistics.close();
    }

    int getUpdateEvents() {
      return statistics().getInt(UPDATE_EVENTS);
    }

    void incUpdateEvents() {
      incUpdateEvents(1);
    }

    void incUpdateEvents(final int amount) {
      incStat(UPDATE_EVENTS, amount);
    }

    int getPuts() {
      return statistics().getInt(PUTS);
    }

    void incPuts() {
      incPuts(1);
    }

    void incPuts(final int amount) {
      incStat(PUTS, amount);
    }

    void incPutTime(final long amount) {
      incStat(PUT_TIME, amount);
    }

    long startPut() {
      return NanoTimer.getTime();
    }

    void endPut(final long start) {
      endPut(start, 1);
    }

    void endPut(final long start, final int amount) {
      var elapsed = NanoTimer.getTime() - start;
      incPuts(amount);
      incPutTime(elapsed);
    }

    private void incStat(final String statName, final int intValue) {
      statistics().incInt(statName, intValue);
    }

    private void incStat(final String statName, final long longValue) {
      statistics().incLong(statName, longValue);
    }
  }

  /**
   * @since GemFire 7.0
   */
  static class UpdateListener extends CacheListenerAdapter<String, Number> {

    private final PubSubStats statistics;

    UpdateListener(final PubSubStats statistics) {
      this.statistics = statistics;
    }

    @Override
    public void afterUpdate(final EntryEvent<String, Number> event) {
      statistics.incUpdateEvents(1);
    }
  }

  /**
   * @since GemFire 7.0
   */
  static class RegionMembershipListener extends RegionMembershipListenerAdapter<String, Number> {

    private final List<DistributedMember> members = new ArrayList<>();

    int size() {
      return members.size();
    }

    List<DistributedMember> getMembers() {
      return Collections.unmodifiableList(new ArrayList<>(members));
    }

    boolean containsId(final DistributedMember member) {
      for (var peer : getMembers()) {
        if (peer.getId().equals(member.getId())) {
          return true;
        }
      }
      return false;
    }

    boolean contains(final DistributedMember member) {
      return members.contains(member);
    }

    String debugContains(final DistributedMember member) {
      var sb = new StringBuilder();
      for (var peer : getMembers()) {
        if (!peer.equals(member)) {
          var peerIDM = (InternalDistributedMember) peer;
          var memberIDM = (InternalDistributedMember) member;
          sb.append("peer port=").append(peerIDM.getMembershipPort()).append(" ");
          sb.append("member port=").append(memberIDM.getMembershipPort()).append(" ");
        }
      }
      return sb.toString();
    }

    @Override
    public void initialMembers(final Region<String, Number> region,
        final DistributedMember[] initialMembers) {
      for (final var initialMember : initialMembers) {
        members.add(initialMember);
      }
    }

    @Override
    public void afterRemoteRegionCreate(final RegionEvent<String, Number> event) {
      members.add(event.getDistributedMember());
    }

    @Override
    public void afterRemoteRegionDeparture(final RegionEvent<String, Number> event) {
      members.remove(event.getDistributedMember());
    }

    @Override
    public void afterRemoteRegionCrash(final RegionEvent<String, Number> event) {
      members.remove(event.getDistributedMember());
    }
  }

  static class MultipleArchiveReader {

    private final File dir;
    private final String regex;

    MultipleArchiveReader(final File dir, final String regex) {
      this.dir = dir;
      this.regex = regex;
    }

    MultipleArchiveReader(final File dir) {
      this.dir = dir;
      regex = null;
    }

    int readIntStat(final String typeName, final String statName) throws IOException {
      // directory (maybe directories) with one or more archives
      if (dir.exists() && dir.isDirectory()) {
        var archives = findFilesWithSuffix(dir, regex, ".gfs");
        return readIntStatFromArchives(archives, typeName, statName);

        // one archive file
      } else if (dir.exists() && dir.isFile()) {
        List<File> archives = new ArrayList<>();
        archives.add(dir);
        return readIntStatFromArchives(archives, typeName, statName);

        // failure
      } else {
        throw new IllegalStateException(dir + " does not exist!");
      }
    }

    private int readIntStatFromArchives(final List<File> archives, final String typeName,
        final String statName) throws IOException {
      var statValues = readStatValues(archives, typeName, statName);
      assertNotNull("statValues is null!", statValues);
      assertTrue("statValues is empty!", statValues.length > 0);

      var value = 0;
      for (final var statValue : statValues) {
        statValue.setFilter(StatValue.FILTER_NONE);
        value += (int) statValue.getSnapshotsMaximum();
      }
      return value;
    }

    private static List<File> findFilesWithSuffix(final File dir, final String regex,
        final String suffix) {
      Pattern p = null;
      if (regex != null) {
        p = Pattern.compile(regex);
      }
      final var pattern = p;

      return findFiles(dir, (final File file) -> {
        var value = true;
        if (regex != null) {
          final var matcher = pattern.matcher(file.getName());
          value = matcher.matches();
        }
        if (suffix != null) {
          value = value && file.getName().endsWith(suffix);
        }
        return value;
      }, true);
    }

    private static List<File> findFiles(final File dir, final FileFilter filter,
        final boolean recursive) {
      var tmpfiles = dir.listFiles(filter);
      List<File> matches;
      if (tmpfiles == null) {
        matches = new ArrayList<>();
      } else {
        matches = new ArrayList<>(Arrays.asList(tmpfiles));
      }
      if (recursive) {
        var files = dir.listFiles();
        if (files != null) {
          for (var file : files) {
            if (file.isDirectory()) {
              matches.addAll(findFiles(file, filter, recursive));
            }
          }
        }
      }
      return matches;
    }

    private static StatValue[] readStatValues(final List<File> archives, final String typeName,
        final String statName) throws IOException {
      final var statSpec = new StatSpec() {
        @Override
        public boolean archiveMatches(File value) {
          return true;
        }

        @Override
        public boolean typeMatches(String value) {
          return typeName.equals(value);
        }

        @Override
        public boolean statMatches(String value) {
          return statName.equals(value);
        }

        @Override
        public boolean instanceMatches(String textId, long numericId) {
          return true;
        }

        @Override
        public int getCombineType() {
          return StatSpec.FILE;
        }
      };

      var archiveFiles = archives.toArray(new File[archives.size()]);
      var filters = new StatSpec[] {statSpec};
      var reader = new StatArchiveReader(archiveFiles, filters, true);
      var values = reader.matchSpec(statSpec);
      return values;
    }
  }
}
