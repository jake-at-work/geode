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
package org.apache.geode.distributed.internal;

import static org.apache.geode.distributed.internal.DistributionConfig.DS_CONFIG_NAME;
import static org.apache.geode.distributed.internal.DistributionConfig.DS_QUORUM_CHECKER_NAME;
import static org.apache.geode.distributed.internal.DistributionConfig.DS_RECONNECTING_NAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import java.util.Properties;

import org.junit.Test;

import org.apache.geode.distributed.internal.membership.api.QuorumChecker;

/**
 * Unit tests for {@link ConnectionConfigImpl}.
 */
public class ConnectionConfigTest {

  @Test
  public void distributionConfig_doesNotContainDsQuorumCheckerProperty() {
    var quorumChecker = mock(QuorumChecker.class);
    var properties = new Properties();
    properties.put(DS_QUORUM_CHECKER_NAME, quorumChecker);

    var config = new ConnectionConfigImpl(properties);

    var result = (DistributionConfigImpl) config.distributionConfig();
    assertThat(result.getProps()).doesNotContainKey(DS_QUORUM_CHECKER_NAME);
  }

  @Test
  public void distributionConfig_doesNotContainDsReconnectingProperty() {
    var properties = new Properties();
    properties.put(DS_RECONNECTING_NAME, Boolean.TRUE);

    var config = new ConnectionConfigImpl(properties);

    var result = (DistributionConfigImpl) config.distributionConfig();
    assertThat(result.getProps()).doesNotContainKey(DS_RECONNECTING_NAME);
  }

  @Test
  public void distributionConfig_doesNotContainDsConfigProperty() {
    var properties = new Properties();
    properties.put(DS_CONFIG_NAME, mock(DistributionConfig.class));

    var config = new ConnectionConfigImpl(properties);

    var result = (DistributionConfigImpl) config.distributionConfig();
    assertThat(result.getProps()).doesNotContainKey(DS_CONFIG_NAME);
  }

  @Test
  public void isReconnecting_isTrue_ifReconnectingPropertyIsTrue() {
    var properties = new Properties();

    properties.put(DS_RECONNECTING_NAME, Boolean.TRUE);

    var config = new ConnectionConfigImpl(properties);

    assertThat(config.isReconnecting()).isTrue();
  }

  @Test
  public void isReconnecting_isFalse_ifReconnectingPropertyIsFalse() {
    var properties = new Properties();

    properties.put(DS_RECONNECTING_NAME, Boolean.FALSE);

    var config = new ConnectionConfigImpl(properties);

    assertThat(config.isReconnecting()).isFalse();
  }

  @Test
  public void isReconnecting_isFalse_ifReconnectingPropertyDoesNotExist() {
    var properties = new Properties();

    properties.remove(DS_RECONNECTING_NAME);

    var config = new ConnectionConfigImpl(properties);

    assertThat(config.isReconnecting()).isFalse();
  }

  @Test
  public void isReconnecting_isFalse_ifReconnectingPropertyIsNotBoolean() {
    var properties = new Properties();

    properties.put(DS_RECONNECTING_NAME, "a string, not a boolean");

    var config = new ConnectionConfigImpl(properties);

    assertThat(config.isReconnecting()).isFalse();
  }

  @Test
  public void quorumChecker_returnsQuorumCheckerProperty_ifPropertyIsAQuorumChecker() {
    var quorumCheckerFromProperties = mock(QuorumChecker.class);
    var properties = new Properties();
    properties.put(DS_QUORUM_CHECKER_NAME, quorumCheckerFromProperties);

    var config = new ConnectionConfigImpl(properties);

    assertThat(config.quorumChecker())
        .isSameAs(quorumCheckerFromProperties);
  }

  @Test
  public void quorumChecker_returnsNull_ifQuorumCheckerPropertyDoesNotExist() {
    var properties = new Properties();
    properties.remove(DS_QUORUM_CHECKER_NAME);

    var config = new ConnectionConfigImpl(properties);

    assertThat(config.quorumChecker()).isNull();
  }

  @Test
  public void quorumChecker_returnsNull_ifQuorumCheckerPropertyIsNotAQuorumChecker() {
    var properties = new Properties();

    properties.put(DS_QUORUM_CHECKER_NAME, "a string, not a quorum checker");

    var config = new ConnectionConfigImpl(properties);

    assertThat(config.quorumChecker()).isNull();
  }

  @Test
  public void distributionConfig_returnsConfigProperty_ifPropertyIsADistributionConfigImpl() {
    var distributionConfigFromProperties =
        new DistributionConfigImpl(new Properties());
    var properties = new Properties();
    properties.put(DS_CONFIG_NAME, distributionConfigFromProperties);

    var config = new ConnectionConfigImpl(properties);

    assertThat(config.distributionConfig())
        .isSameAs(distributionConfigFromProperties);
  }

  @Test
  public void distributionConfig_returnsDistributionConfigImpl_ifConfigPropertyDoesNotExist() {
    var properties = new Properties();
    properties.remove(DS_CONFIG_NAME);

    var config = new ConnectionConfigImpl(properties);

    assertThat(config.distributionConfig())
        .isInstanceOf(DistributionConfigImpl.class);
  }

  @Test
  public void distributionConfig_returnsDistributionConfigImpl_ifConfigPropertyIsNotADistributionConfigImpl() {
    var distributionConfigFromProperties = "a string, not a distribution config";
    var properties = new Properties();
    properties.put(DS_CONFIG_NAME, distributionConfigFromProperties);

    var config = new ConnectionConfigImpl(properties);

    assertThat(config.distributionConfig())
        .isInstanceOf(DistributionConfigImpl.class)
        .isNotSameAs(distributionConfigFromProperties);
  }
}
