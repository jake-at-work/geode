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

import java.io.File;

import org.apache.geode.cache.ssl.CertStores;
import org.apache.geode.cache.ssl.CertificateBuilder;

/**
 * This program generates the trust and key stores used by SNI acceptance tests.
 * The stores have a 100 year expiration date, but if you need to generate new ones
 * use this program, modified as necessary to correct problems, to generate new
 * stores.
 */
public class GenerateSNIKeyAndTrustStores {

  public static void main(String... args) throws Exception {
    new GenerateSNIKeyAndTrustStores().generateStores();
  }

  public void generateStores() throws Exception {
    var ca = new CertificateBuilder(365 * 100, "SHA256withRSA")
        .commonName("Test CA")
        .isCA()
        .generate();

    final var resourceFilename = "geode-config/gemfire.properties";
    final var resource = SingleServerSNIAcceptanceTest.class.getResource(resourceFilename);
    var path = resource.getPath();
    path = path.substring(0, path.length() - "gemfire.properties".length());

    var trustStoreCreated = false;

    for (var certName : new String[] {"locator-maeve", "server-clementine", "server-dolores"}) {
      var certificate = new CertificateBuilder(365 * 100, "SHA256withRSA")
          .commonName(certName)
          .issuedBy(ca)
          .sanDnsName(certName)
          .sanDnsName("geode")
          .generate();

      var store = new CertStores(certName);
      store.withCertificate("locator-maeve", certificate);
      store.trust("ca", ca);

      var keyStoreFile = new File(path + certName + "-keystore.jks");
      keyStoreFile.createNewFile();
      store.createKeyStore(keyStoreFile.getAbsolutePath(), "geode");
      System.out.println("created " + keyStoreFile.getAbsolutePath());

      if (!trustStoreCreated) {
        var trustStoreFile = new File(path + "truststore.jks");
        trustStoreFile.createNewFile();
        store.createTrustStore(trustStoreFile.getPath(), "geode");
        System.out.println("created " + trustStoreFile.getAbsolutePath());
        trustStoreCreated = true;
      }
    }
  }

}
