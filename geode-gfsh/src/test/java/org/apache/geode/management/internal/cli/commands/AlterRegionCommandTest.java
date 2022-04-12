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

import static org.apache.geode.cache.Region.SEPARATOR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.cache.ExpirationAction;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.configuration.CacheConfig;
import org.apache.geode.cache.configuration.DeclarableType;
import org.apache.geode.cache.configuration.EnumActionDestroyOverflow;
import org.apache.geode.cache.configuration.RegionAttributesType;
import org.apache.geode.cache.configuration.RegionConfig;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.InternalConfigurationPersistenceService;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.management.configuration.ClassName;
import org.apache.geode.management.internal.functions.CliFunctionResult;
import org.apache.geode.test.junit.rules.GfshParserRule;


public class AlterRegionCommandTest {

  @Rule
  public GfshParserRule parser = new GfshParserRule();

  private AlterRegionCommand command;
  private CacheConfig cacheConfig;
  private RegionConfig existingRegionConfig;

  @Before
  public void before() {
    command = spy(AlterRegionCommand.class);
    var cache = mock(InternalCache.class);
    command.setCache(cache);
    when(cache.getSecurityService()).thenReturn(mock(SecurityService.class));
    var ccService =
        mock(InternalConfigurationPersistenceService.class);
    doReturn(ccService).when(command).getConfigurationPersistenceService();
    var members =
        Stream.of(mock(DistributedMember.class)).collect(Collectors.toSet());
    doReturn(members).when(command).findMembers(any(), any());
    var result =
        new CliFunctionResult("member", CliFunctionResult.StatusState.OK, "regionA altered");
    doReturn(Collections.singletonList(result)).when(command).executeAndGetFunctionResult(any(),
        any(), any());

    cacheConfig = new CacheConfig();
    existingRegionConfig = new RegionConfig();
    existingRegionConfig.setName(SEPARATOR + "regionA");
    existingRegionConfig.setType(RegionShortcut.REPLICATE.name());
    cacheConfig.getRegions().add(existingRegionConfig);
    when(ccService.getCacheConfig("cluster")).thenReturn(cacheConfig);
  }

  @Test
  public void alterWithCacheWriter() {
    var deltaConfig =
        getDeltaRegionConfig("alter region --name=regionA --cache-writer=CommandWriter");
    var deltaAttributes = deltaConfig.getRegionAttributes();
    assertThat(deltaAttributes.getCacheLoader()).isNull();
    assertThat(deltaAttributes.getCacheListeners()).isNotNull().isEmpty();
    assertThat(deltaAttributes.getCacheWriter().getClassName()).isEqualTo("CommandWriter");

    var existingAttributes = new RegionAttributesType();
    existingAttributes.setCacheLoader(new DeclarableType("CacheLoader"));
    existingAttributes.setCacheWriter(new DeclarableType("CacheWriter"));
    existingAttributes.getCacheListeners().add(new DeclarableType("CacheListener"));

    existingRegionConfig.setRegionAttributes(existingAttributes);
    command.updateConfigForGroup("cluster", cacheConfig, deltaConfig);

    // CacheListeners and CacheLoader are unchanged
    assertThat(existingAttributes.getCacheListeners()).hasSize(1);
    assertThat(existingAttributes.getCacheLoader().getClassName()).isEqualTo("CacheLoader");
    assertThat(existingAttributes.getCacheListeners().get(0).getClassName())
        .isEqualTo("CacheListener");

    // CacheWriter is changed
    assertThat(existingAttributes.getCacheWriter().getClassName()).isEqualTo("CommandWriter");
  }

  @Test
  public void alterWithNoCacheWriter() {
    var deltaConfig =
        getDeltaRegionConfig("alter region --name=regionA --cache-writer=' '");
    var deltaAttributes = deltaConfig.getRegionAttributes();
    assertThat(deltaAttributes.getCacheWriter()).isEqualTo(DeclarableType.EMPTY);

    var existingAttributes = new RegionAttributesType();
    existingAttributes.setCacheWriter(new DeclarableType("CacheWriter"));

    existingRegionConfig.setRegionAttributes(existingAttributes);
    command.updateConfigForGroup("cluster", cacheConfig, deltaConfig);

    // CacheWriter is changed
    assertThat(existingAttributes.getCacheWriter()).isNull();
  }

  @Test
  public void alterWithInvalidCacheWriter() {
    var command = "alter region --name=" + SEPARATOR + "Person --cache-writer='1abc'";
    var result = parser.parse(command);
    assertThat(result).isNull();
  }

  @Test
  public void emptyCustomExpiryAndNoCloning() {
    var command =
        "alter region --name=" + SEPARATOR + "Person --entry-idle-time-custom-expiry=''";
    var result = parser.parse(command);
    var paramValue = (ClassName) result.getParamValue("entry-idle-time-custom-expiry");
    assertThat(paramValue).isEqualTo(ClassName.EMPTY);
    assertThat(paramValue.getClassName()).isEqualTo("");

    // when enable-cloning is not specified, the value should be null
    var enableCloning = result.getParamValue("enable-cloning");
    assertThat(enableCloning).isNull();
  }

  @Test
  public void regionNameIsConverted() {
    var command = "alter region --name=Person";
    var result = parser.parse(command);
    assertThat(result.getParamValue("name")).isEqualTo(SEPARATOR + "Person");
  }

  @Test
  public void groupNotExist() {
    doReturn(Collections.emptySet()).when(command).findMembers(any(), any());
    parser.executeAndAssertThat(command,
        "alter region --group=group0 --name=regionA --cache-loader=abc")
        .statusIsError()
        .hasInfoSection().hasLines().contains("No Members Found");
  }

  @Test
  public void regionNotExistOnGroup() {
    parser.executeAndAssertThat(command,
        "alter region --name=regionB --cache-loader=abc")
        .statusIsError()
        .hasInfoSection().hasLines()
        .contains(SEPARATOR + "regionB does not exist in group cluster");

    parser.executeAndAssertThat(command,
        "alter region --group=group1 --name=regionA --cache-loader=abc")
        .statusIsError()
        .hasInfoSection().hasLines().contains(SEPARATOR + "regionA does not exist in group group1");
  }

  @Test
  public void ccServiceNotAvailable() {
    doReturn(null).when(command).getConfigurationPersistenceService();
    parser.executeAndAssertThat(command,
        "alter region --name=regionB --cache-loader=abc")
        .statusIsSuccess()
        .hasInfoSection().hasOutput().contains(
            "Cluster configuration service is not running. Configuration change is not persisted");
  }

  @Test
  public void alterWithCloningEnabled() {
    var regionAttributes =
        getDeltaRegionConfig("alter region --name=regionA --enable-cloning=false")
            .getRegionAttributes();
    assertThat(regionAttributes.isCloningEnabled()).isFalse();
    assertThat(regionAttributes.getAsyncEventQueueIds()).isNull();
    assertThat(regionAttributes.getDataPolicy()).isNull();
    assertThat(regionAttributes.getGatewaySenderIds()).isNull();
    assertThat(regionAttributes.getCacheLoader()).isNull();
    assertThat(regionAttributes.getCacheWriter()).isNull();
    assertThat(regionAttributes.getCacheListeners()).isNotNull().isEmpty();
    assertThat(regionAttributes.getEvictionAttributes()).isNull();
    assertThat(regionAttributes.getEntryIdleTime()).isNull();
    assertThat(regionAttributes.getEntryTimeToLive()).isNull();
    assertThat(regionAttributes.getRegionIdleTime()).isNull();
    assertThat(regionAttributes.getRegionTimeToLive()).isNull();
  }

  @Test
  public void alterWithEntryIdleTimeOut() {
    // check that the deltaConfig is created as expected
    var deltaConfig =
        getDeltaRegionConfig("alter region --name=regionA --entry-idle-time-expiration=7");
    var entryIdleTime =
        deltaConfig.getRegionAttributes().getEntryIdleTime();
    assertThat(entryIdleTime).isNotNull();
    assertThat(entryIdleTime.getTimeout()).isEqualTo("7");
    assertThat(entryIdleTime.getCustomExpiry()).isNull();
    assertThat(entryIdleTime.getAction()).isNull();

    // check that the combined the configuration is created as expected
    var existingAttributes = new RegionAttributesType();
    var expirationAttributesType =
        new RegionAttributesType.ExpirationAttributesType(10,
            ExpirationAction.DESTROY.toXmlString(), null, null);
    existingAttributes.setEntryIdleTime(expirationAttributesType);
    existingRegionConfig.setRegionAttributes(existingAttributes);

    command.updateConfigForGroup("cluster", cacheConfig, deltaConfig);
    var combinedExpirationAttributes =
        existingRegionConfig.getRegionAttributes().getEntryIdleTime();
    assertThat(combinedExpirationAttributes.getTimeout()).isEqualTo("7");
    assertThat(combinedExpirationAttributes.getAction()).isEqualTo("destroy");
    assertThat(combinedExpirationAttributes.getCustomExpiry()).isNull();
  }

  @Test
  public void alterWithEntryIdleTimeOutAction() {
    var deltaConfig =
        getDeltaRegionConfig(
            "alter region --name=regionA --entry-idle-time-expiration-action=destroy");
    var entryIdleTime =
        deltaConfig.getRegionAttributes().getEntryIdleTime();
    assertThat(entryIdleTime).isNotNull();
    assertThat(entryIdleTime.getTimeout()).isNull();
    assertThat(entryIdleTime.getCustomExpiry()).isNull();
    assertThat(entryIdleTime.getAction()).isEqualTo("destroy");

    // check that the combined the configuration is created as expected
    var existingAttributes = new RegionAttributesType();
    var expirationAttributesType =
        new RegionAttributesType.ExpirationAttributesType(10,
            ExpirationAction.INVALIDATE.toXmlString(), null,
            null);
    existingAttributes.setEntryIdleTime(expirationAttributesType);
    existingRegionConfig.setRegionAttributes(existingAttributes);

    command.updateConfigForGroup("cluster", cacheConfig, deltaConfig);
    var combinedExpirationAttributes =
        existingRegionConfig.getRegionAttributes().getEntryIdleTime();
    assertThat(combinedExpirationAttributes.getTimeout()).isEqualTo("10");
    assertThat(combinedExpirationAttributes.getAction()).isEqualTo("destroy");
    assertThat(combinedExpirationAttributes.getCustomExpiry()).isNull();
  }

  @Test
  public void alterWithEntryIdleTimeOutCustomExpiry() {
    var deltaConfig =
        getDeltaRegionConfig("alter region --name=regionA --entry-idle-time-custom-expiry=abc");
    var entryIdleTime =
        deltaConfig.getRegionAttributes().getEntryIdleTime();
    assertThat(entryIdleTime).isNotNull();
    assertThat(entryIdleTime.getTimeout()).isNull();
    assertThat(entryIdleTime.getCustomExpiry().getClassName()).isEqualTo("abc");
    assertThat(entryIdleTime.getAction()).isNull();

    // check that the combined the configuration is created as expected
    var existingAttributes = new RegionAttributesType();
    var expirationAttributesType =
        new RegionAttributesType.ExpirationAttributesType(10,
            ExpirationAction.INVALIDATE.toXmlString(), null,
            null);
    existingAttributes.setEntryIdleTime(expirationAttributesType);
    existingRegionConfig.setRegionAttributes(existingAttributes);

    command.updateConfigForGroup("cluster", cacheConfig, deltaConfig);
    var combinedExpirationAttributes =
        existingRegionConfig.getRegionAttributes().getEntryIdleTime();
    assertThat(combinedExpirationAttributes.getTimeout()).isEqualTo("10");
    assertThat(combinedExpirationAttributes.getAction()).isEqualTo("invalidate");
    assertThat(combinedExpirationAttributes.getCustomExpiry().getClassName()).isEqualTo("abc");
  }

  @Test
  public void alterWithEmptyEntryIdleTimeOutCustomExpiry() {
    var deltaConfig =
        getDeltaRegionConfig("alter region --name=regionA --entry-idle-time-custom-expiry=''");
    var entryIdleTime =
        deltaConfig.getRegionAttributes().getEntryIdleTime();
    assertThat(entryIdleTime).isNotNull();
    assertThat(entryIdleTime.getTimeout()).isNull();
    assertThat(entryIdleTime.getCustomExpiry()).isEqualTo(DeclarableType.EMPTY);
    assertThat(entryIdleTime.getAction()).isNull();

    // check that the combined the configuration is created as expected
    var existingAttributes = new RegionAttributesType();
    var expirationAttributesType =
        new RegionAttributesType.ExpirationAttributesType(10,
            ExpirationAction.INVALIDATE.toXmlString(), null,
            null);
    existingAttributes.setEntryIdleTime(expirationAttributesType);
    existingRegionConfig.setRegionAttributes(existingAttributes);

    command.updateConfigForGroup("cluster", cacheConfig, deltaConfig);
    var combinedExpirationAttributes =
        existingRegionConfig.getRegionAttributes().getEntryIdleTime();
    assertThat(combinedExpirationAttributes.getTimeout()).isEqualTo("10");
    assertThat(combinedExpirationAttributes.getAction()).isEqualTo("invalidate");
    assertThat(combinedExpirationAttributes.getCustomExpiry()).isNull();
  }

  @Test
  public void alterWithCacheListener() {
    var deltaConfig =
        getDeltaRegionConfig("alter region --name=regionA --cache-listener=abc,def");
    var cacheListeners = deltaConfig.getRegionAttributes().getCacheListeners();
    assertThat(cacheListeners).hasSize(2);
    assertThat(cacheListeners.get(0).getClassName()).isEqualTo("abc");
    assertThat(cacheListeners.get(1).getClassName()).isEqualTo("def");

    // check that the combined the configuration is created as expected
    var existingAttributes = new RegionAttributesType();
    existingAttributes.getCacheListeners().add(new DeclarableType("ghi"));
    existingRegionConfig.setRegionAttributes(existingAttributes);
    command.updateConfigForGroup("cluster", cacheConfig, deltaConfig);

    var updatedCacheListeners =
        existingRegionConfig.getRegionAttributes().getCacheListeners();
    assertThat(updatedCacheListeners).hasSize(2);
    assertThat(updatedCacheListeners.get(0).getClassName()).isEqualTo("abc");
    assertThat(updatedCacheListeners.get(1).getClassName()).isEqualTo("def");

    assertThat(existingRegionConfig.getRegionAttributes().getEntryIdleTime()).isNull();
  }

  @Test
  public void alterWithNoCacheListener() {
    var deltaConfig =
        getDeltaRegionConfig("alter region --name=regionA --cache-listener=''");
    var cacheListeners = deltaConfig.getRegionAttributes().getCacheListeners();
    assertThat(cacheListeners).hasSize(1);
    assertThat(cacheListeners.get(0)).isEqualTo(DeclarableType.EMPTY);

    var existingAttributes = new RegionAttributesType();
    existingAttributes.getCacheListeners().add(new DeclarableType("ghi"));
    existingRegionConfig.setRegionAttributes(existingAttributes);
    command.updateConfigForGroup("cluster", cacheConfig, deltaConfig);

    var updatedCacheListeners =
        existingRegionConfig.getRegionAttributes().getCacheListeners();
    assertThat(updatedCacheListeners).hasSize(0);
  }

  @Test
  public void alterWithCacheLoader() {
    var deltaConfig =
        getDeltaRegionConfig("alter region --name=regionA --cache-loader=abc");
    var deltaAttributes = deltaConfig.getRegionAttributes();
    assertThat(deltaAttributes.getCacheWriter()).isNull();
    assertThat(deltaAttributes.getCacheLoader().getClassName()).isEqualTo("abc");
    assertThat(deltaAttributes.getCacheListeners()).isNotNull().isEmpty();

    var existingAttributes = new RegionAttributesType();
    existingAttributes.getCacheListeners().add(new DeclarableType("def"));
    existingAttributes.setCacheLoader(new DeclarableType("def"));
    existingAttributes.setCacheWriter(new DeclarableType("def"));

    existingRegionConfig.setRegionAttributes(existingAttributes);
    command.updateConfigForGroup("cluster", cacheConfig, deltaConfig);

    // after update, the cache listeners remains the same
    assertThat(existingAttributes.getCacheListeners()).hasSize(1);
    assertThat(existingAttributes.getCacheListeners().get(0).getClassName()).isEqualTo("def");

    // after update the cache writer remains the same
    assertThat(existingAttributes.getCacheWriter().getClassName()).isEqualTo("def");

    // after update the cache loader is changed
    assertThat(existingAttributes.getCacheLoader().getClassName()).isEqualTo("abc");
  }

  @Test
  public void alterWithNoCacheLoader() {
    var deltaConfig =
        getDeltaRegionConfig("alter region --name=regionA --cache-loader=''");
    var deltaAttributes = deltaConfig.getRegionAttributes();
    assertThat(deltaAttributes.getCacheLoader()).isEqualTo(DeclarableType.EMPTY);

    var existingAttributes = new RegionAttributesType();
    existingAttributes.setCacheLoader(new DeclarableType("def"));

    existingRegionConfig.setRegionAttributes(existingAttributes);
    command.updateConfigForGroup("cluster", cacheConfig, deltaConfig);

    // after the update the cache loader is null
    assertThat(existingAttributes.getCacheLoader()).isNull();
  }

  @Test
  public void alterWithAsyncEventQueueIds() {
    var deltaConfig =
        getDeltaRegionConfig("alter region --name=regionA --async-event-queue-id=abc,def");
    assertThat(deltaConfig.getRegionAttributes().getAsyncEventQueueIds()).isEqualTo("abc,def");
    assertThat(deltaConfig.getRegionAttributes().getGatewaySenderIds()).isNull();

    var existingAttributes = new RegionAttributesType();
    existingRegionConfig.setRegionAttributes(existingAttributes);
    existingAttributes.setAsyncEventQueueIds("xyz");
    existingAttributes.setGatewaySenderIds("xyz");

    command.updateConfigForGroup("cluster", cacheConfig, deltaConfig);
    assertThat(existingAttributes.getGatewaySenderIds()).isEqualTo("xyz");
    assertThat(existingAttributes.getAsyncEventQueueIds()).isEqualTo("abc,def");
  }

  @Test
  public void alterWithNoAsyncEventQueueIds() {
    var deltaConfig =
        getDeltaRegionConfig("alter region --name=regionA --async-event-queue-id=''");
    assertThat(deltaConfig.getRegionAttributes().getAsyncEventQueueIds()).isEqualTo("");
    assertThat(deltaConfig.getRegionAttributes().getGatewaySenderIds()).isNull();

    var existingAttributes = new RegionAttributesType();
    existingRegionConfig.setRegionAttributes(existingAttributes);
    existingAttributes.setAsyncEventQueueIds("xyz");
    existingAttributes.setGatewaySenderIds("xyz");

    command.updateConfigForGroup("cluster", cacheConfig, deltaConfig);
    assertThat(existingAttributes.getGatewaySenderIds()).isEqualTo("xyz");
    assertThat(existingAttributes.getAsyncEventQueueIds()).isEqualTo("");
    assertThat(existingAttributes.getAsyncEventQueueIdsAsSet()).isNotNull().isEmpty();
  }

  @Test
  public void alterWithEvictionMaxWithExistingLruHeapPercentage() {
    var deltaConfig =
        getDeltaRegionConfig("alter region --name=regionA --eviction-max=20");

    // we are saving the eviction-max as a lruEntryCount's maximum value
    var lruEntryCount =
        deltaConfig.getRegionAttributes().getEvictionAttributes().getLruEntryCount();
    assertThat(lruEntryCount.getMaximum()).isEqualTo("20");

    // when there is no eviction attributes at all
    var existingAttributes = new RegionAttributesType();
    existingRegionConfig.setRegionAttributes(existingAttributes);
    command.updateConfigForGroup("cluster", cacheConfig, deltaConfig);
    assertThat(existingAttributes.getEvictionAttributes()).isNull();

    // when there is lruHeapPercentage eviction
    var evictionAttributes =
        new RegionAttributesType.EvictionAttributes();
    evictionAttributes
        .setLruHeapPercentage(new RegionAttributesType.EvictionAttributes.LruHeapPercentage());
    command.updateConfigForGroup("cluster", cacheConfig, deltaConfig);
    assertThat(evictionAttributes.getLruEntryCount()).isNull();
    assertThat(evictionAttributes.getLruMemorySize()).isNull();
  }

  @Test
  public void alterWithEvictionMaxWithExistingLruEntryCount() {
    var deltaConfig =
        getDeltaRegionConfig("alter region --name=regionA --eviction-max=20");

    // we are saving the eviction-max as a lruEntryCount's maximum value
    var lruEntryCount =
        deltaConfig.getRegionAttributes().getEvictionAttributes().getLruEntryCount();
    assertThat(lruEntryCount.getMaximum()).isEqualTo("20");

    // when there is no eviction attributes at all
    var existingAttributes = new RegionAttributesType();
    existingRegionConfig.setRegionAttributes(existingAttributes);
    command.updateConfigForGroup("cluster", cacheConfig, deltaConfig);
    assertThat(existingAttributes.getEvictionAttributes()).isNull();

    // when there is lruHeapPercentage eviction
    var evictionAttributes =
        new RegionAttributesType.EvictionAttributes();
    existingAttributes.setEvictionAttributes(evictionAttributes);
    var existingEntryCount =
        new RegionAttributesType.EvictionAttributes.LruEntryCount();
    existingEntryCount.setMaximum("100");
    existingEntryCount.setAction(EnumActionDestroyOverflow.LOCAL_DESTROY);
    evictionAttributes.setLruEntryCount(existingEntryCount);

    command.updateConfigForGroup("cluster", cacheConfig, deltaConfig);
    assertThat(evictionAttributes.getLruEntryCount().getMaximum()).isEqualTo("20");
    assertThat(evictionAttributes.getLruEntryCount().getAction())
        .isEqualTo(EnumActionDestroyOverflow.LOCAL_DESTROY);
  }

  @Test
  public void alterWithEvictionMaxWithExistingLruMemory() {
    var deltaConfig =
        getDeltaRegionConfig("alter region --name=regionA --eviction-max=20");

    // we are saving the eviction-max as a lruEntryCount's maximum value
    var lruEntryCount =
        deltaConfig.getRegionAttributes().getEvictionAttributes().getLruEntryCount();
    assertThat(lruEntryCount.getMaximum()).isEqualTo("20");

    // when there is no eviction attributes at all
    var existingAttributes = new RegionAttributesType();
    existingRegionConfig.setRegionAttributes(existingAttributes);
    command.updateConfigForGroup("cluster", cacheConfig, deltaConfig);
    assertThat(existingAttributes.getEvictionAttributes()).isNull();

    // when there is lruHeapPercentage eviction
    var evictionAttributes =
        new RegionAttributesType.EvictionAttributes();
    existingAttributes.setEvictionAttributes(evictionAttributes);
    var existingMemorySize =
        new RegionAttributesType.EvictionAttributes.LruMemorySize();
    existingMemorySize.setMaximum("100");
    existingMemorySize.setAction(EnumActionDestroyOverflow.LOCAL_DESTROY);
    evictionAttributes.setLruMemorySize(existingMemorySize);

    command.updateConfigForGroup("cluster", cacheConfig, deltaConfig);
    assertThat(evictionAttributes.getLruMemorySize().getMaximum()).isEqualTo("20");
    assertThat(evictionAttributes.getLruMemorySize().getAction())
        .isEqualTo(EnumActionDestroyOverflow.LOCAL_DESTROY);
  }

  private RegionConfig getDeltaRegionConfig(String commandString) {
    return (RegionConfig) parser.executeAndAssertThat(command, commandString).getResultModel()
        .getConfigObject();
  }
}
