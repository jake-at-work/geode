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

package org.apache.geode.cache.client.internal;

import static java.lang.String.valueOf;
import static org.apache.geode.distributed.ConfigurationProperties.LOG_FILE;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_KEYSTORE;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_KEYSTORE_PASSWORD;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_KEYSTORE_TYPE;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_TRUSTSTORE;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_TRUSTSTORE_PASSWORD;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_TRUSTSTORE_TYPE;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_USE_DEFAULT_CONTEXT;
import static org.apache.geode.distributed.ServerLauncher.Command.START;
import static org.apache.geode.security.SecurableCommunicationChannels.ALL;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.util.Properties;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.client.ServerOperationException;
import org.apache.geode.cache.client.internal.provider.ExampleX509ExtendedTrustManager;
import org.apache.geode.cache.ssl.CertStores;
import org.apache.geode.cache.ssl.CertificateBuilder;
import org.apache.geode.cache.ssl.CertificateMaterial;
import org.apache.geode.distributed.ServerLauncher;
import org.apache.geode.test.dunit.rules.ClientVM;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;

public class ExampleX509ExtendedSSLContextServerTest {
  private CertificateMaterial ca;

  private static ClientVM clientVM;

  @Rule
  public ClusterStartupRule cluster = new ClusterStartupRule(1);

  @Before
  public void before() {
    ca = new CertificateBuilder()
        .commonName("Test CA")
        .isCA()
        .generate();
  }

  @Test
  public void serverRejectsClientNotTrusted() throws Exception {
    final CertificateMaterial serverCertificate = new CertificateBuilder()
        .commonName("server")
        .issuedBy(ca)
        .sanDnsName(InetAddress.getLocalHost().getHostName())
        .sanDnsName(InetAddress.getLocalHost().getCanonicalHostName())
        .sanIpAddress(InetAddress.getLocalHost())
        .generate();

    final CertStores serverStore = CertStores.serverStore();
    serverStore.withCertificate("server", serverCertificate);
    serverStore.trust("ca", ca);

    Properties serverSSLProps = serverStore.propertiesWith(ALL);

    final ServerLauncher serverLauncher = startServer(serverSSLProps);

    startClient(InetAddress.getLocalHost().getHostName(), serverLauncher.getServerPort());

    serverLauncher.stop();
  }

  private ServerLauncher startServer(final Properties properties)
      throws KeyStoreException, IOException, NoSuchAlgorithmException, CertificateException,
      UnrecoverableKeyException, KeyManagementException {
    final KeyManager[] keyManagers =
        getKeyManagers(properties.getProperty(SSL_KEYSTORE_TYPE), properties
            .getProperty(SSL_KEYSTORE), properties.getProperty(SSL_KEYSTORE_PASSWORD));

    final TrustManager[] trustManagers =
        getTrustManagers(properties.getProperty(SSL_TRUSTSTORE_TYPE), properties
            .getProperty(SSL_TRUSTSTORE), properties.getProperty(SSL_TRUSTSTORE_PASSWORD));

    final SSLContext sslContext = SSLContext.getInstance("TLSv1.2");
    sslContext.init(keyManagers, ExampleX509ExtendedTrustManager.wrap(trustManagers), null);

    // set default context
    SSLContext.setDefault(sslContext);

    properties.setProperty(SSL_USE_DEFAULT_CONTEXT, valueOf(true));
    properties.setProperty(LOG_FILE, "");

    final ServerLauncher serverLauncher = new ServerLauncher.Builder()
        .setCommand(START)
        .setMemberName("server")
        .set(properties)
        .build();

    serverLauncher.start();
    return serverLauncher;
  }

  private void startClient(final String serverHostname,
      final int serverPort)
      throws Exception {
    CertificateMaterial clientCertificate = new CertificateBuilder()
        .commonName("client")
        .issuedBy(ca)
        .generate();

    CertStores clientStore = CertStores.clientStore();
    clientStore.withCertificate("client", clientCertificate);
    clientStore.trust("ca", ca);

    Properties clientSSLProps = clientStore.propertiesWith(ALL);
    clientVM = cluster.startClientVM(0, clientSSLProps, clientCacheFactory -> {
      clientCacheFactory.addPoolServer(serverHostname, serverPort);
    });


    clientVM.invoke(() -> {
      final ClientCache cache = ClusterStartupRule.getClientCache();
      assertThat(cache).isNotNull();
      cache.createClientRegionFactory(ClientRegionShortcut.PROXY).create("foo");
      assertThatThrownBy(() -> {
        cache.getRegion("foo").put(1, 1);
      }).isInstanceOf(
          ServerOperationException.class);
      cache.close();
    });
  }

  private TrustManager[] getTrustManagers(final String type, final String file,
      final String password)
      throws KeyStoreException, IOException, NoSuchAlgorithmException, CertificateException {
    final KeyStore trustStore = getKeyStore(type, file, password);
    final TrustManagerFactory trustManagerFactory =
        TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
    trustManagerFactory.init(trustStore);
    return trustManagerFactory.getTrustManagers();
  }

  private KeyManager[] getKeyManagers(final String type, final String file, final String password)
      throws KeyStoreException, IOException, NoSuchAlgorithmException, CertificateException,
      UnrecoverableKeyException {
    final KeyStore keyStore = getKeyStore(type, file, password);
    final KeyManagerFactory keyManagerFactory =
        KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
    keyManagerFactory.init(keyStore, password.toCharArray());
    return keyManagerFactory.getKeyManagers();
  }

  private KeyStore getKeyStore(final String type, final String file, final String password)
      throws KeyStoreException, IOException, NoSuchAlgorithmException, CertificateException {
    final KeyStore keyStore = KeyStore.getInstance(type);
    try (final InputStream inputStream = new FileInputStream(file)) {
      keyStore.load(inputStream, password.toCharArray());
    }
    return keyStore;
  }

}
