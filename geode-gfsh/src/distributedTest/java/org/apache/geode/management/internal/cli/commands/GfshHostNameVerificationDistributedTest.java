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
package org.apache.geode.management.internal.cli.commands;

import static org.apache.geode.security.SecurableCommunicationChannels.ALL;
import static org.apache.geode.security.SecurableCommunicationChannels.JMX;
import static org.apache.geode.security.SecurableCommunicationChannels.LOCATOR;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.util.Properties;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.ssl.CertStores;
import org.apache.geode.cache.ssl.CertificateBuilder;
import org.apache.geode.cache.ssl.CertificateMaterial;
import org.apache.geode.internal.inet.LocalHostUtil;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.GfshTest;
import org.apache.geode.test.junit.rules.GfshCommandRule;

@Category({GfshTest.class})
public class GfshHostNameVerificationDistributedTest {
  private static MemberVM locator;
  private CertificateMaterial ca;

  @Rule
  public ClusterStartupRule cluster = new ClusterStartupRule();

  @Rule
  public GfshCommandRule gfsh = new GfshCommandRule();

  private CertStores locatorStore;
  private CertStores gfshStore;

  @Before
  public void setupCluster() throws Exception {
    ca = new CertificateBuilder()
        .commonName("Test CA")
        .isCA()
        .generate();

    var hostname = LocalHostUtil.getCanonicalLocalHostName();
    final var builder = new CertificateBuilder()
        .commonName("locator")
        .issuedBy(ca)
        .sanDnsName(InetAddress.getLoopbackAddress().getHostName())
        .sanDnsName(hostname)
        .sanIpAddress(InetAddress.getByName("0.0.0.0"));
    // the rules used by this test use "localhost" as a hostname, which
    // causes it to use a non-loopback IP literal address instead of the
    // host's name on CI Windows runs
    builder.sanIpAddress(LocalHostUtil.getLocalHost());
    var locatorCertificate = builder.generate();

    var gfshCertificate = new CertificateBuilder()
        .commonName("gfsh")
        .issuedBy(ca)
        .generate();

    locatorStore = CertStores.locatorStore();
    locatorStore.withCertificate("locator", locatorCertificate);
    locatorStore.trust("ca", ca);

    gfshStore = CertStores.clientStore();
    gfshStore.withCertificate("gfsh", gfshCertificate);
    gfshStore.trust("ca", ca);
  }

  private File gfshSecurityProperties(Properties clientSSLProps) throws IOException {
    var sslConfigFile = File.createTempFile("gfsh-ssl", "properties");
    var out = new FileOutputStream(sslConfigFile);
    clientSSLProps.store(out, null);
    return sslConfigFile;
  }

  @Test
  public void gfshConnectsToLocator() throws Exception {
    var locatorSSLProps = locatorStore.propertiesWith(LOCATOR);

    var gfshSSLProps = gfshStore.propertiesWith(LOCATOR);

    // create a cluster
    locator = cluster.startLocatorVM(0, locatorSSLProps);

    // connect gfsh
    var sslConfigFile = gfshSecurityProperties(gfshSSLProps);
    gfsh.connectAndVerify(locator.getPort(), GfshCommandRule.PortType.locator,
        "security-properties-file", sslConfigFile.getAbsolutePath());

    gfsh.executeAndAssertThat("list members").statusIsSuccess();
  }

  @Test
  public void expectConnectionFailureWhenNoHostNameInLocatorKey() throws Exception {
    var locatorCertificate = new CertificateBuilder()
        .commonName("locator")
        .issuedBy(ca)
        .generate();

    validateGfshConnectionFailure(locatorCertificate, "No name matching");
  }

  @Test
  public void expectConnectionFailureWhenWrongHostNameInLocatorKey() throws Exception {
    var locatorCertificate = new CertificateBuilder()
        .commonName("locator")
        .issuedBy(ca)
        .sanDnsName("example.com")
        .generate();

    validateGfshConnectionFailure(locatorCertificate, "No subject alternative DNS name matching");
  }

  private void validateGfshConnectionFailure(final CertificateMaterial locatorCertificate,
      final String failure)
      throws Exception {
    var gfshCertificate = new CertificateBuilder()
        .commonName("gfsh")
        .issuedBy(ca)
        .generate();

    var lstore = CertStores.locatorStore();
    lstore.withCertificate("locator", locatorCertificate);
    lstore.trust("ca", ca);

    var gstore = CertStores.clientStore();
    gstore.withCertificate("gfsh", gfshCertificate);
    gstore.trust("ca", ca);

    var locatorSSLProps = lstore.propertiesWith(ALL, false, false);

    var gfshSSLProps = gstore.propertiesWith(ALL, false, true);

    // create a cluster
    locator = cluster.startLocatorVM(0, locatorSSLProps);

    // connect gfsh
    var sslConfigFile = gfshSecurityProperties(gfshSSLProps);

    IgnoredException.addIgnoredException("javax.net.ssl.SSLHandshakeException");
    IgnoredException.addIgnoredException("java.net.SocketException");

    var connectCommand =
        "connect --locator=" + locator.getVM().getHost().getHostName() + "[" + locator.getPort()
            + "] --security-properties-file=" + sslConfigFile.getAbsolutePath();
    gfsh.executeAndAssertThat(connectCommand).statusIsError()
        .containsOutput(failure);
    IgnoredException.removeAllExpectedExceptions();
  }

  @Test
  public void gfshConnectsToLocatorOnJMX() throws Exception {
    var locatorSSLProps = locatorStore.propertiesWith(JMX);
    var gfshSSLProps = gfshStore.propertiesWith(JMX);
    validateGfshConnectOnJMX(locatorSSLProps, gfshSSLProps);
  }

  @Test
  public void gfshConnectsToLocatorOnJMXWhenALL() throws Exception {
    var locatorSSLProps = locatorStore.propertiesWith(ALL);
    var gfshSSLProps = gfshStore.propertiesWith(ALL);
    validateGfshConnectOnJMX(locatorSSLProps, gfshSSLProps);
  }

  private void validateGfshConnectOnJMX(Properties locatorSSLProps, Properties gfshSSLProps)
      throws IOException {
    // create a cluster
    locator = cluster.startLocatorVM(0, locatorSSLProps);

    // connect gfsh on jmx
    var sslConfigFile = gfshSecurityProperties(gfshSSLProps);
    final var jmxPort = locator.getJmxPort();
    final var sslConfigFilePath = sslConfigFile.getAbsolutePath();

    var vm = cluster.getVM(1);
    vm.invoke(() -> {
      var rule = new GfshCommandRule();
      rule.connectAndVerify(jmxPort, GfshCommandRule.PortType.jmxManager,
          "security-properties-file", sslConfigFilePath);

      rule.executeAndAssertThat("list members").statusIsSuccess();
      rule.close();
    });
    vm.bounce(); // to clear the system properties set by JmxOperationInvoker
  }
}
