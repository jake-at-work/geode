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
package org.apache.geode.distributed.internal.membership.gms.messenger;

import static org.apache.geode.distributed.internal.membership.gms.util.MemberIdentifierUtil.createMemberID;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.distributed.internal.membership.api.MemberIdentifier;
import org.apache.geode.distributed.internal.membership.api.MembershipConfig;
import org.apache.geode.distributed.internal.membership.gms.GMSMembershipView;
import org.apache.geode.distributed.internal.membership.gms.Services;
import org.apache.geode.test.junit.categories.MembershipTest;
import org.apache.geode.test.junit.rules.ConcurrencyRule;

@Category({MembershipTest.class})
public class GMSEncryptJUnitTest {

  private static final int THREAD_COUNT = 20;
  private static final String DEFAULT_ALGO = "AES:128";

  Services services;

  MemberIdentifier[] mockMembers;

  GMSMembershipView netView;

  @Rule
  public ConcurrencyRule concurrencyRule = new ConcurrencyRule();

  private void initMocks() throws Exception {
    initMocks(DEFAULT_ALGO);
  }

  private void initMocks(String algo) throws Exception {
    var membershipConfig = new MembershipConfig() {
      @Override
      public String getSecurityUDPDHAlgo() {
        return algo;
      }
    };

    services = mock(Services.class);
    when(services.getConfig()).thenReturn(membershipConfig);

    mockMembers = new MemberIdentifier[4];
    for (var i = 0; i < mockMembers.length; i++) {
      mockMembers[i] = createMemberID(8888 + i);
    }
    var viewId = 1;
    List<MemberIdentifier> mbrs = new LinkedList<>();
    mbrs.add(mockMembers[0]);
    mbrs.add(mockMembers[1]);
    mbrs.add(mockMembers[2]);

    // prepare the view
    netView = new GMSMembershipView(mockMembers[0], viewId, mbrs);

  }

  String[] algos = new String[] {"AES:128", "Blowfish"};

  @Test
  public void testOneMemberCanDecryptAnothersMessage() throws Exception {
    for (var algo : algos) {
      initMocks(algo);

      var sender = new GMSEncrypt(services, algo);
      var receiver = new GMSEncrypt(services, algo);

      // establish the public keys for the sender and receiver
      netView.setPublicKey(mockMembers[1], sender.getPublicKeyBytes());
      netView.setPublicKey(mockMembers[2], receiver.getPublicKeyBytes());

      sender.overrideInstallViewForTest(netView);
      receiver.overrideInstallViewForTest(netView);

      // sender encrypts a message, so use receiver's public key
      var ch = "Hello world";
      var challenge = ch.getBytes();
      var encryptedChallenge = sender.encryptData(challenge, mockMembers[2]);

      // receiver decrypts the message using the sender's public key
      var decryptBytes = receiver.decryptData(encryptedChallenge, mockMembers[1]);

      // now send a response
      var response = "Hello yourself!";
      var responseBytes = response.getBytes();
      var encryptedResponse = receiver.encryptData(responseBytes, mockMembers[1]);

      // receiver decodes the response
      var decryptedResponse = sender.decryptData(encryptedResponse, mockMembers[2]);

      Assert.assertFalse(Arrays.equals(challenge, encryptedChallenge));

      Assert.assertTrue(Arrays.equals(challenge, decryptBytes));

      Assert.assertFalse(Arrays.equals(responseBytes, encryptedResponse));

      Assert.assertTrue(Arrays.equals(responseBytes, decryptedResponse));

    }
  }

  @Test
  public void testOneMemberCanDecryptAnothersMessageMultithreaded() throws Exception {
    initMocks();
    final var runs = 100000;
    final var sender = new GMSEncrypt(services, DEFAULT_ALGO);
    final var receiver = new GMSEncrypt(services, DEFAULT_ALGO);

    // establish the public keys for the sender and receiver
    netView.setPublicKey(mockMembers[1], sender.getPublicKeyBytes());
    netView.setPublicKey(mockMembers[2], receiver.getPublicKeyBytes());

    sender.overrideInstallViewForTest(netView);
    receiver.overrideInstallViewForTest(netView);

    for (var j = 0; j < THREAD_COUNT; j++) {
      var callable = (Callable<Object>) () -> {
        var ch = "Hello world";
        var challenge = ch.getBytes();
        var encryptedChallenge = sender.encryptData(challenge, mockMembers[2]);

        // receiver decrypts the message using the sender's public key
        var decryptBytes = receiver.decryptData(encryptedChallenge, mockMembers[1]);

        // now send a response
        var response = "Hello yourself!";
        var responseBytes = response.getBytes();
        var encryptedResponse = receiver.encryptData(responseBytes, mockMembers[1]);

        // receiver decodes the response
        var decryptedResponse = sender.decryptData(encryptedResponse, mockMembers[2]);

        Assert.assertFalse(Arrays.equals(challenge, encryptedChallenge));

        Assert.assertTrue(Arrays.equals(challenge, decryptBytes));

        Assert.assertFalse(Arrays.equals(responseBytes, encryptedResponse));

        Assert.assertTrue(Arrays.equals(responseBytes, decryptedResponse));

        return null;
      };

      concurrencyRule.add(callable).repeatForIterations(runs);
    }

    concurrencyRule.executeInParallel();
  }

  @Test
  public void testPublicKeyPrivateKeyFromSameMember() throws Exception {
    initMocks();

    var sender = new GMSEncrypt(services, DEFAULT_ALGO);
    var receiver = new GMSEncrypt(services, DEFAULT_ALGO);

    // establish the public keys for the sender and receiver
    netView.setPublicKey(mockMembers[1], sender.getPublicKeyBytes());
    netView.setPublicKey(mockMembers[2], receiver.getPublicKeyBytes());

    sender.overrideInstallViewForTest(netView);
    receiver.overrideInstallViewForTest(netView);

    // sender encrypts a message, so use receiver's public key
    var ch = "Hello world";
    var challenge = ch.getBytes();
    var encryptedChallenge = sender.encryptData(challenge, mockMembers[2]);

    // receiver decrypts the message using the sender's public key
    var decryptBytes = receiver.decryptData(encryptedChallenge, mockMembers[1]);

    // now send a response
    var response = "Hello yourself!";
    var responseBytes = response.getBytes();
    var encryptedResponse = receiver.encryptData(responseBytes, mockMembers[1]);

    // receiver decodes the response
    var decryptedResponse = sender.decryptData(encryptedResponse, mockMembers[2]);

    Assert.assertFalse(Arrays.equals(challenge, encryptedChallenge));

    Assert.assertTrue(Arrays.equals(challenge, decryptBytes));

    Assert.assertFalse(Arrays.equals(responseBytes, encryptedResponse));

    Assert.assertTrue(Arrays.equals(responseBytes, decryptedResponse));

  }

  @Test
  public void testForClusterSecretKey() throws Exception {
    initMocks();

    var sender = new GMSEncrypt(services, DEFAULT_ALGO);
    sender.initClusterSecretKey();
    // establish the public keys for the sender and receiver
    netView.setPublicKey(mockMembers[1], sender.getPublicKeyBytes());

    sender.overrideInstallViewForTest(netView);

    // sender encrypts a message, so use receiver's public key
    var ch = "Hello world";
    var challenge = ch.getBytes();
    var encryptedChallenge = sender.encryptData(challenge);

    // receiver decrypts the message using the sender's public key
    var decryptBytes = sender.decryptData(encryptedChallenge);

    Assert.assertFalse(Arrays.equals(challenge, encryptedChallenge));

    Assert.assertTrue(Arrays.equals(challenge, decryptBytes));
  }

  @Test
  public void testForClusterSecretKeyFromOtherMember() throws Exception {
    for (var algo : algos) {
      initMocks(algo);

      final var sender = new GMSEncrypt(services, algo);
      sender.initClusterSecretKey();
      final var receiver = new GMSEncrypt(services, algo);

      // establish the public keys for the sender and receiver
      netView.setPublicKey(mockMembers[1], sender.getPublicKeyBytes());
      netView.setPublicKey(mockMembers[2], receiver.getPublicKeyBytes());

      sender.overrideInstallViewForTest(netView);

      var secretBytes = sender.getClusterSecretKey();
      receiver.setClusterKey(secretBytes);

      receiver.overrideInstallViewForTest(netView);

      // sender encrypts a message, so use receiver's public key
      var ch = "Hello world";
      var challenge = ch.getBytes();
      var encryptedChallenge = sender.encryptData(challenge);

      // receiver decrypts the message using the sender's public key
      var decryptBytes = receiver.decryptData(encryptedChallenge);

      // now send a response
      var response = "Hello yourself!";
      var responseBytes = response.getBytes();
      var encryptedResponse = receiver.encryptData(responseBytes);

      // receiver decodes the response
      var decryptedResponse = sender.decryptData(encryptedResponse);

      Assert.assertFalse(Arrays.equals(challenge, encryptedChallenge));

      Assert.assertTrue(Arrays.equals(challenge, decryptBytes));

      Assert.assertFalse(Arrays.equals(responseBytes, encryptedResponse));

      Assert.assertTrue(Arrays.equals(responseBytes, decryptedResponse));
    }
  }

  @Test
  public void testForClusterSecretKeyFromOtherMemberMultipleThreads() throws Exception {
    initMocks();

    final var sender = new GMSEncrypt(services, DEFAULT_ALGO);
    Thread.sleep(100);
    sender.initClusterSecretKey();
    final var receiver = new GMSEncrypt(services, DEFAULT_ALGO);

    // establish the public keys for the sender and receiver
    netView.setPublicKey(mockMembers[1], sender.getPublicKeyBytes());
    netView.setPublicKey(mockMembers[2], receiver.getPublicKeyBytes());

    sender.overrideInstallViewForTest(netView);

    var secretBytes = sender.getClusterSecretKey();
    receiver.setClusterKey(secretBytes);

    receiver.overrideInstallViewForTest(netView);

    final var runs = 100000;

    for (var j = 0; j < THREAD_COUNT; j++) {
      var callable = (Callable<Void>) () -> {
        // sender encrypts a message, so use receiver's public key

        var ch = "Hello world";
        var challenge = ch.getBytes();
        var encryptedChallenge = sender.encryptData(challenge);

        // receiver decrypts the message using the sender's public key
        var decryptBytes = receiver.decryptData(encryptedChallenge);

        // now send a response
        var response = "Hello yourself!";
        var responseBytes = response.getBytes();
        var encryptedResponse = receiver.encryptData(responseBytes);

        // receiver decodes the response
        var decryptedResponse = sender.decryptData(encryptedResponse);

        Assert.assertFalse(Arrays.equals(challenge, encryptedChallenge));

        Assert.assertTrue(Arrays.equals(challenge, decryptBytes));

        Assert.assertFalse(Arrays.equals(responseBytes, encryptedResponse));

        Assert.assertTrue(Arrays.equals(responseBytes, decryptedResponse));

        return null;
      };
      concurrencyRule.add(callable).repeatForIterations(runs);

    }
    concurrencyRule.executeInParallel();
  }
}
