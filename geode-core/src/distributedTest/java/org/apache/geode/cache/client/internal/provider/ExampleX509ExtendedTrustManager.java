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

package org.apache.geode.cache.client.internal.provider;

import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.logging.Logger;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLSession;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509ExtendedTrustManager;

public class ExampleX509ExtendedTrustManager extends DelegatingX509ExtendedTrustManager {
  private final Logger logger = Logger.getLogger(getClass().getName());

  public ExampleX509ExtendedTrustManager(final X509ExtendedTrustManager delegate) {
    super(delegate);
  }

  @Override
  public void checkClientTrusted(final X509Certificate[] chain,
      final String authType, final Socket socket) throws CertificateException {
    super.checkClientTrusted(chain, authType, socket);

    verify(((SSLSocket) socket).getHandshakeSession());

    // verify(((InetSocketAddress) socket.getRemoteSocketAddress()).getHostName(), chain);
  }

  @Override
  public void checkClientTrusted(final X509Certificate[] chain,
      final String authType, final SSLEngine engine) throws CertificateException {
    super.checkClientTrusted(chain, authType, engine);

    verify(engine.getHandshakeSession());

    // verify(engine.getPeerHost(), chain);
  }

  private void verify(final SSLSession sslSession) throws CertificateException {
    final String peerHost = sslSession.getPeerHost();
    String hostname = null;
    try {
      hostname = InetAddress.getByName(peerHost).getHostName();
    } catch (UnknownHostException ignored) {
      hostname = peerHost;
    }

    if (null != hostname && !hostname.contains("jabarrett")) {
      logger.warning("Not trusted, hostname=" + hostname);
      throw new CertificateException("Certificate not trusted");
    }
  }

  // private void verify(final String hostname, final X509Certificate[] chain)
  // throws CertificateException {
  // if (null != hostname && !hostname.contains("trusted")) {
  // logger.warning("Not trusted, hostname=" + hostname);
  // throw new CertificateException("Certificate not trusted");
  // }
  // // if (stream(chain).noneMatch(c -> c.getSubjectDN().getName().contains("trusted"))) {
  // // logger.warning("Not trusted, hostname=" + hostname);
  // // throw new CertificateException("Certificate not trusted.");
  // // }
  // }

  public static TrustManager[] wrap(final TrustManager[] trustManagers) {
    final TrustManager[] clone = trustManagers.clone();
    for (int i = 0; i < clone.length; ++i) {
      if (clone[i] instanceof X509ExtendedTrustManager) {
        clone[i] = new ExampleX509ExtendedTrustManager((X509ExtendedTrustManager) clone[i]);
      }
    }

    return clone;
  }
}
