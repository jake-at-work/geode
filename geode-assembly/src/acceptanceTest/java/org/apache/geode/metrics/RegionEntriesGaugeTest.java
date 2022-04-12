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

package org.apache.geode.metrics;

import static java.lang.Integer.parseInt;
import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;
import static org.apache.geode.cache.RegionShortcut.LOCAL;
import static org.apache.geode.cache.RegionShortcut.PARTITION;
import static org.apache.geode.cache.RegionShortcut.PARTITION_REDUNDANT;
import static org.apache.geode.cache.RegionShortcut.REPLICATE;
import static org.apache.geode.cache.client.ClientRegionShortcut.PROXY;
import static org.apache.geode.internal.AvailablePortHelper.getRandomAvailableTCPPorts;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.compiler.ClassBuilder.writeJarFromClasses;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.nio.file.Path;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.Pool;
import org.apache.geode.cache.client.PoolManager;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.rules.ServiceJarRule;
import org.apache.geode.test.junit.categories.MetricsTest;
import org.apache.geode.test.junit.rules.gfsh.GfshRule;

@Category(MetricsTest.class)
public class RegionEntriesGaugeTest {

  @Rule
  public GfshRule gfshRule = new GfshRule();

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule
  public ServiceJarRule serviceJarRule = new ServiceJarRule();

  private ClientCache clientCache;
  private String connectToLocatorCommand;
  private String locatorString;
  private Pool server1Pool;
  private Path serviceJarPath;

  @Before
  public void startMembers() throws Exception {
    serviceJarPath = serviceJarRule.createJarFor("metrics-publishing-service.jar",
        MetricsPublishingService.class, SimpleMetricsPublishingService.class);
    var availablePorts = getRandomAvailableTCPPorts(4);
    var locatorPort = availablePorts[0];
    var locatorJmxPort = availablePorts[1];
    var serverPort1 = availablePorts[2];
    var serverPort2 = availablePorts[3];

    locatorString = "localhost[" + locatorPort + "]";

    var folderForLocator = temporaryFolder.newFolder("locator");
    var folderForServer1 = temporaryFolder.newFolder("server1");
    var folderForServer2 = temporaryFolder.newFolder("server2");

    var startLocatorCommand = String.join(" ",
        "start locator",
        "--name=" + "locator",
        "--dir=" + folderForLocator.getAbsolutePath(),
        "--port=" + locatorPort,
        "--http-service-port=0",
        "--J=-Dgemfire.jmx-manager-port=" + locatorJmxPort);

    var startServer1Command = startServerCommand("server1", serverPort1, folderForServer1);
    var startServer2Command = startServerCommand("server2", serverPort2, folderForServer2);

    gfshRule.execute(startLocatorCommand, startServer1Command, startServer2Command);

    connectToLocatorCommand = "connect --locator=" + locatorString;

    var functionJarPath =
        temporaryFolder.getRoot().toPath().resolve("function.jar").toAbsolutePath();
    writeJarFromClasses(functionJarPath.toFile(), GetMemberRegionEntriesGaugeFunction.class);

    var deployCommand = "deploy --jar=" + functionJarPath.toAbsolutePath();
    var listFunctionsCommand = "list functions";

    gfshRule.execute(connectToLocatorCommand, deployCommand, listFunctionsCommand);

    clientCache = new ClientCacheFactory().addPoolLocator("localhost", locatorPort).create();

    server1Pool = PoolManager.createFactory()
        .addServer("localhost", serverPort1)
        .create("server1pool");
  }

  @After
  public void stopMembers() {
    if (clientCache != null) {
      clientCache.close();
    }
    if (server1Pool != null) {
      server1Pool.destroy();
    }

    var shutdownCommand = String.join(" ",
        "shutdown",
        "--include-locators=true");
    gfshRule.execute(connectToLocatorCommand, shutdownCommand);
  }

  @Test
  public void regionEntriesGaugeShowsCountOfLocalRegionValuesInServer() {
    var regionName = "localRegion";
    var region = createRegionInGroup(LOCAL.name(), regionName, "server1");

    var keys = asList("a", "b", "c", "d", "e", "f", "g", "h");
    for (var key : keys) {
      region.put(key, key);
    }
    region.destroy(keys.get(0));
    var expectedNumberOfEntries = keys.size() - 1;

    var getGaugeValueCommand = memberRegionEntryGaugeValueCommand(regionName);

    await()
        .untilAsserted(() -> {
          var execution = gfshRule.execute(connectToLocatorCommand, getGaugeValueCommand);
          var server1EntryCount = linesOf(execution.getOutputText())
              .filter(s -> s.startsWith("server1"))
              .mapToInt(RegionEntriesGaugeTest::extractEntryCount)
              .findFirst();

          assertThat(server1EntryCount)
              .as("Number of entries reported by server1")
              .hasValue(expectedNumberOfEntries);

          var server2Response = linesOf(execution.getOutputText())
              .filter(s -> s.startsWith("server2"))
              .findFirst()
              .orElse("No response from server2");

          assertThat(server2Response)
              .as("server2 response from entry count function")
              .endsWith("[Meter not found.]");
        });
  }

  @Test
  public void regionEntriesGaugeShowsCountOfReplicateRegionValuesInServer() {
    var otherRegion = createRegion(REPLICATE.name(), "otherRegionName");
    otherRegion.put("other-region-key", "other-region-value");

    var regionName = "replicateRegion";
    var regionOfInterest = createRegion(REPLICATE.name(), regionName);

    var keys = asList("a", "b", "c", "d", "e", "f", "g", "h");
    for (var key : keys) {
      regionOfInterest.put(key, key);
    }
    regionOfInterest.destroy(keys.get(0));
    var expectedNumberOfEntries = keys.size() - 1;

    var getGaugeValueCommand = memberRegionEntryGaugeValueCommand(regionName);

    await().untilAsserted(() -> {
      var execution = gfshRule.execute(connectToLocatorCommand, getGaugeValueCommand);
      var entryCounts = linesOf(execution.getOutputText())
          .filter(s -> s.startsWith("server"))
          .map(RegionEntriesGaugeTest::extractEntryCount)
          .collect(toList());

      assertThat(entryCounts)
          .as("Number of entries reported by each server")
          .allMatch(i -> i == expectedNumberOfEntries, "equals " + expectedNumberOfEntries);
    });
  }

  @Test
  public void regionEntriesGaugeShowsCountOfPartitionedRegionValuesInEachServer() {
    var regionName = "partitionedRegion";

    var region = createRegion(PARTITION.name(), regionName);

    var keys = asList("a", "b", "c", "d", "e", "f", "g", "h");
    for (var key : keys) {
      region.put(key, key);
    }

    region.destroy(keys.get(0));

    var expectedNumberOfEntries = keys.size() - 1;

    var getGaugeValueCommand = memberRegionEntryGaugeValueCommand(regionName);

    await().untilAsserted(() -> {
      var execution = gfshRule.execute(connectToLocatorCommand, getGaugeValueCommand);
      var sum = linesOf(execution.getOutputText())
          .filter(s -> s.startsWith("server"))
          .mapToInt(RegionEntriesGaugeTest::extractEntryCount)
          .sum();

      assertThat(sum)
          .as("total number of entries on all servers")
          .isEqualTo(expectedNumberOfEntries);
    });
  }

  @Test
  public void regionEntriesGaugeShowsCountOfPartitionedRedundantRegionValuesInEachServer()
      throws IOException {
    var numberOfRedundantCopies = 1;
    var regionName = "partitionedRegion";

    var server3Port = AvailablePortHelper.getRandomAvailableTCPPort();

    var folderForServer3 = temporaryFolder.newFolder("server3");

    var startServer3Command = startServerCommand("server3", server3Port, folderForServer3);
    gfshRule.execute(connectToLocatorCommand, startServer3Command);

    var region =
        createPartitionedRegionWithRedundancy(regionName, numberOfRedundantCopies);

    var keys = asList("a", "b", "c", "d", "e", "f", "g", "h");
    for (var key : keys) {
      region.put(key, key);
    }

    region.destroy(keys.get(0));

    var getGaugeValueCommand = memberRegionEntryGaugeValueCommand(regionName);

    var numberOfEntries = keys.size() - 1;
    var totalNumberOfCopies = numberOfRedundantCopies + 1;
    var expectedNumberOfEntries = numberOfEntries * totalNumberOfCopies;

    await()
        .untilAsserted(() -> {
          var execution =
              gfshRule.execute(connectToLocatorCommand, getGaugeValueCommand);
          var totalEntryCount = linesOf(execution.getOutputText())
              .filter(s -> s.startsWith("server"))
              .mapToInt(RegionEntriesGaugeTest::extractEntryCount)
              .sum();

          assertThat(totalEntryCount)
              .as("total number of entries on all servers")
              .isEqualTo(expectedNumberOfEntries);
        });
  }

  private static int extractEntryCount(String serverEntryCountLine) {
    var entryCountExtractor = ".*\\[(\\d+).*].*";
    var entryCountPattern = Pattern.compile(entryCountExtractor);
    var matcher = entryCountPattern.matcher(serverEntryCountLine);
    assertThat(matcher.matches())
        .as(serverEntryCountLine)
        .withFailMessage("does not match " + entryCountExtractor)
        .isTrue();
    var matchedGroup = matcher.group(1);
    return parseInt(matchedGroup);
  }

  private static Stream<String> linesOf(String text) {
    return new BufferedReader(new StringReader(text)).lines();
  }

  private Region<String, String> createPartitionedRegionWithRedundancy(String regionName,
      int numberOfRedundantCopies) {

    var createRegionCommand = String.join(" ",
        "create region",
        "--name=" + regionName,
        "--redundant-copies=" + numberOfRedundantCopies,
        "--type=" + PARTITION_REDUNDANT.name());

    gfshRule.execute(connectToLocatorCommand, createRegionCommand);

    return clientCache.<String, String>createClientRegionFactory(PROXY).create(regionName);
  }

  private Region<String, String> createRegion(String regionType, String regionName) {

    var createRegionCommand = String.join(" ",
        "create region",
        "--name=" + regionName,
        "--type=" + regionType);

    gfshRule.execute(connectToLocatorCommand, createRegionCommand);

    return clientCache.<String, String>createClientRegionFactory(PROXY).create(regionName);
  }

  private Region<String, String> createRegionInGroup(String regionType, String regionName,
      String groupName) {
    var createRegionCommand = String.join(" ",
        "create region",
        "--name=" + regionName,
        "--type=" + regionType,
        "--groups=" + groupName);

    gfshRule.execute(connectToLocatorCommand, createRegionCommand);

    return clientCache.<String, String>createClientRegionFactory(PROXY)
        .setPoolName(server1Pool.getName())
        .create(regionName);
  }

  private String startServerCommand(String serverName, int serverPort, File folderForServer) {
    return String.join(" ",
        "start server",
        "--name=" + serverName,
        "--groups=" + serverName,
        "--dir=" + folderForServer.getAbsolutePath(),
        "--server-port=" + serverPort,
        "--locators=" + locatorString,
        "--classpath=" + serviceJarPath);
  }

  private static String memberRegionEntryGaugeValueCommand(String regionName) {
    return String.join(" ",
        "execute function",
        "--id=" + GetMemberRegionEntriesGaugeFunction.ID,
        "--arguments=" + regionName);
  }

  public static class GetMemberRegionEntriesGaugeFunction implements Function<String[]> {

    private static final String ID = "GetMemberRegionEntriesGaugeFunction";

    @Override
    public void execute(FunctionContext<String[]> context) {
      var regionName = context.getArguments()[0];

      var memberRegionEntriesGauge = SimpleMetricsPublishingService.getRegistry()
          .find("geode.cache.entries")
          .tag("region", regionName)
          .gauge();

      Object result = memberRegionEntriesGauge == null
          ? "Meter not found."
          : memberRegionEntriesGauge.value();

      context.getResultSender().lastResult(result);
    }

    @Override
    public String getId() {
      return ID;
    }
  }
}
