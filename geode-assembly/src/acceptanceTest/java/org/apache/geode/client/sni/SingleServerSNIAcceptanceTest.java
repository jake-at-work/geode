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
package org.apache.geode.client.sni;

import static org.apache.geode.cache.Region.SEPARATOR;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_ENABLED_COMPONENTS;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_ENDPOINT_IDENTIFICATION_ENABLED;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_KEYSTORE_TYPE;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_REQUIRE_AUTHENTICATION;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_TRUSTSTORE;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_TRUSTSTORE_PASSWORD;
import static org.apache.geode.test.util.ResourceUtils.createTempFileFromResource;
import static org.assertj.core.api.Assertions.assertThat;

import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.client.proxy.ProxySocketFactories;
import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.internal.cache.tier.sockets.BaseCommand;
import org.apache.geode.rules.DockerComposeRule;

/**
 * This test runs against a 1-server, 1-locator Geode cluster. The server and locator run inside
 * a (single) Docker container and are not route-able from the host (where this JUnit test is
 * running). Another Docker container is running the HAProxy image and it's set up as an SNI
 * gateway. The test connects to the gateway via SNI and the gateway (in one Docker container)
 * forwards traffic to Geode members (running in the other Docker container).
 *
 * This test connects to the server and verifies it can write and read data in the region.
 */

public class SingleServerSNIAcceptanceTest {

  private static final URL DOCKER_COMPOSE_PATH =
      SingleServerSNIAcceptanceTest.class.getResource("docker-compose.yml");

  @ClassRule
  public static DockerComposeRule docker = new DockerComposeRule.Builder()
      .file(DOCKER_COMPOSE_PATH.getPath())
      .service("haproxy", 15443)
      .build();

  private static ClientCache cache;
  private static Region<String, String> region;
  private static Map<String, String> bulkData;

  @BeforeClass
  public static void beforeClass() {
    // start up server/locator processes and initialize the server cache
    docker.execForService("geode", "gfsh", "run", "--file=/geode/scripts/geode-starter.gfsh");

    final var trustStorePath =
        createTempFileFromResource(SingleServerSNIAcceptanceTest.class,
            "geode-config/truststore.jks")
                .getAbsolutePath();

    // set up client cache properties so it can connect to the server
    var clientCacheProperties = new Properties();
    clientCacheProperties.setProperty(SSL_ENABLED_COMPONENTS, "all");
    clientCacheProperties.setProperty(SSL_KEYSTORE_TYPE, "jks");
    clientCacheProperties.setProperty(SSL_REQUIRE_AUTHENTICATION, "false");

    clientCacheProperties.setProperty(SSL_TRUSTSTORE, trustStorePath);
    clientCacheProperties.setProperty(SSL_TRUSTSTORE_PASSWORD, "geode");
    clientCacheProperties.setProperty(SSL_ENDPOINT_IDENTIFICATION_ENABLED, "true");
    cache = getClientCache(clientCacheProperties);

    // the gfsh startup script created a server-side region named "jellyfish"
    region = cache.<String, String>createClientRegionFactory(ClientRegionShortcut.PROXY)
        .create("jellyfish");
    bulkData = getBulkDataMap();
    region.putAll(bulkData);
  }

  @AfterClass
  public static void afterClass() throws Exception {
    var logs = docker.execForService("geode", "cat", "server-dolores/server-dolores.log");
    System.out.println("server logs------------------------------------------");
    System.out.println(logs);

    if (cache != null) {
      cache.close();
      cache = null;
    }
    bulkData = null;
    region = null;
  }

  /**
   * A basic connectivity test that performs a few simple operations
   */
  @Test
  public void connectToSNIProxyDocker() {
    region.put("hello", "world");
    assertThat(region.containsKey("hello")).isFalse(); // proxy regions don't store locally
    assertThat(region.get("hello")).isEqualTo("world");
    region.destroy("hello");
    assertThat(region.get("hello")).isNull();
  }

  /**
   * A test of Region query that returns a "big" result
   */
  @Test
  public void query() throws Exception {
    final SelectResults<String> results = region.query("SELECT * from " + SEPARATOR + "jellyfish");
    assertThat(results).hasSize(bulkData.size());
    for (var result : results) {
      assertThat(bulkData.containsValue(result)).isTrue();
    }
  }

  /**
   * A test of Region bulk getAll
   */
  @Test
  public void getAll() {
    final var results = region.getAll(bulkData.keySet());
    assertThat(results).hasSize(bulkData.size());
    for (var entry : results.entrySet()) {
      assertThat(region.containsKey(entry.getKey())).isFalse();
      assertThat(bulkData.containsKey(entry.getKey())).isTrue();
      assertThat(entry.getValue()).isEqualTo(bulkData.get(entry.getKey()));
    }
  }

  /**
   * A test of Region bulk removeAll
   */
  @Test
  public void removeAll() {
    assertThat(region.sizeOnServer()).isEqualTo(bulkData.size());
    region.removeAll(bulkData.keySet());
    assertThat(region.sizeOnServer()).isZero();
    region.putAll(bulkData);
  }

  /**
   * A test of the Region API's methods that directly access the server cache
   */
  @Test
  public void verifyServerAPIs() {
    assertThat(region.sizeOnServer()).isEqualTo(bulkData.size());
    var keysOnServer = region.keySetOnServer();
    for (var entry : bulkData.keySet()) {
      assertThat(region.containsKeyOnServer(entry)).isTrue();
      assertThat(keysOnServer).contains(entry);
    }
  }


  protected static Map<String, String> getBulkDataMap() {
    // create a putAll map with enough keys to force a lot of "chunking" of the results
    var numberOfKeys = BaseCommand.MAXIMUM_CHUNK_SIZE * 10; // 1,000 keys
    Map<String, String> pairs = new HashMap<>();
    for (var i = 1; i < numberOfKeys; i++) {
      pairs.put("Object_" + i, "Value_" + i);
    }
    return pairs;
  }

  protected static ClientCache getClientCache(Properties properties) {
    int proxyPort = docker.getExternalPortForService("haproxy", 15443);
    return new ClientCacheFactory(properties)
        .addPoolLocator("locator-maeve", 10334)
        .setPoolSocketFactory(ProxySocketFactories.sni("localhost",
            proxyPort))
        .create();
  }

}
