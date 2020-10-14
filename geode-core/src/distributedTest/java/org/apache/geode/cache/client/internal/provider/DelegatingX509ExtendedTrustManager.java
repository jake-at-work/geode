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

import java.net.Socket;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.X509ExtendedTrustManager;

public abstract class DelegatingX509ExtendedTrustManager extends X509ExtendedTrustManager {

  private final X509ExtendedTrustManager delegate;

  public DelegatingX509ExtendedTrustManager(final X509ExtendedTrustManager delegate) {
    this.delegate = delegate;
  }

  @Override
  public void checkClientTrusted(final X509Certificate[] chain,
      final String authType, final Socket socket) throws CertificateException {
    delegate.checkClientTrusted(chain, authType, socket);
  }

  @Override
  public void checkServerTrusted(final X509Certificate[] chain,
      final String authType, final Socket socket) throws CertificateException {
    delegate.checkServerTrusted(chain, authType, socket);
  }

  @Override
  public void checkClientTrusted(final X509Certificate[] chain,
      final String authType, final SSLEngine engine) throws CertificateException {
    delegate.checkClientTrusted(chain, authType, engine);
  }

  @Override
  public void checkServerTrusted(final X509Certificate[] chain,
      final String authType, final SSLEngine engine) throws CertificateException {
    delegate.checkServerTrusted(chain, authType, engine);
  }

  @Override
  public void checkClientTrusted(final X509Certificate[] chain,
      final String authType) throws CertificateException {
    delegate.checkClientTrusted(chain, authType);
  }

  @Override
  public void checkServerTrusted(final X509Certificate[] chain,
      final String authType) throws CertificateException {
    delegate.checkServerTrusted(chain, authType);
  }

  @Override
  public X509Certificate[] getAcceptedIssuers() {
    return delegate.getAcceptedIssuers();
  }
}
