/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.geode.management.internal.configuration.realizers;

import static org.apache.geode.cache.Region.SEPARATOR;

import java.util.Arrays;

import org.apache.commons.lang3.NotImplementedException;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheListener;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.ExpirationAction;
import org.apache.geode.cache.ExpirationAttributes;
import org.apache.geode.cache.RegionDestroyedException;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.configuration.RegionAttributesType;
import org.apache.geode.cache.configuration.RegionConfig;
import org.apache.geode.internal.cache.EvictionAttributesImpl;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.PartitionAttributesImpl;
import org.apache.geode.management.api.RealizationResult;
import org.apache.geode.management.configuration.Region;
import org.apache.geode.management.internal.configuration.converters.RegionConverter;
import org.apache.geode.management.internal.configuration.domain.DeclarableTypeInstantiator;
import org.apache.geode.management.internal.i18n.CliStrings;
import org.apache.geode.management.internal.util.ManagementUtils;
import org.apache.geode.management.internal.util.RegionPath;
import org.apache.geode.management.runtime.RuntimeRegionInfo;

public class RegionConfigRealizer
    implements ConfigurationRealizer<Region, RuntimeRegionInfo> {

  private final RegionConverter converter = new RegionConverter();

  /**
   * this is used to create only root regions.
   *
   * @param regionConfig the name in the regionConfig can not contain sub-regions.
   */
  @Override
  public RealizationResult create(Region regionConfig, InternalCache cache) {
    var xmlConfig = converter.fromConfigObject(regionConfig);
    return create(xmlConfig, regionConfig.getName(), cache);
  }

  /**
   * this method supports creating root region and sub-regions.
   * We need this because CreateRegionCommand should still
   * support creating sub regions.
   *
   * @param regionConfig the name in regionConfig is ignored.
   * @param regionPath this is the full path of the region
   */
  public RealizationResult create(RegionConfig regionConfig, String regionPath,
      InternalCache cache) {
    var factory = getRegionFactory(cache, regionConfig.getRegionAttributes());
    var regionPathData = new RegionPath(regionPath);
    var regionName = regionPathData.getName();
    var parentRegionPath = regionPathData.getParent();
    if (parentRegionPath == null) {
      factory.create(regionName);
      return new RealizationResult().setMessage("Region successfully created.");
    }

    org.apache.geode.cache.Region parentRegion = cache.getRegion(parentRegionPath);
    factory.createSubregion(parentRegion, regionName);
    return new RealizationResult().setMessage("Region successfully created.");
  }

  private RegionFactory getRegionFactory(Cache cache, RegionAttributesType regionAttributes) {
    RegionFactory factory = cache.createRegionFactory();

    factory.setDataPolicy(DataPolicy.valueOf(regionAttributes.getDataPolicy().name()));

    if (regionAttributes.getScope() != null) {
      factory.setScope(Scope.fromString(regionAttributes.getScope().name()));
    }

    if (regionAttributes.getCacheLoader() != null) {
      ((RegionFactory<Object, Object>) factory)
          .setCacheLoader(DeclarableTypeInstantiator.newInstance(regionAttributes.getCacheLoader(),
              cache));
    }

    if (regionAttributes.getCacheWriter() != null) {
      ((RegionFactory<Object, Object>) factory)
          .setCacheWriter(DeclarableTypeInstantiator.newInstance(regionAttributes.getCacheWriter(),
              cache));
    }

    if (regionAttributes.getCacheListeners() != null) {
      var configListeners = regionAttributes.getCacheListeners();
      var listeners = new CacheListener[configListeners.size()];
      for (var i = 0; i < configListeners.size(); i++) {
        listeners[i] = DeclarableTypeInstantiator.newInstance(configListeners.get(i), cache);
      }
      ((RegionFactory<Object, Object>) factory).initCacheListeners(listeners);
    }

    final var keyConstraint = regionAttributes.getKeyConstraint();
    final var valueConstraint = regionAttributes.getValueConstraint();
    if (keyConstraint != null && !keyConstraint.isEmpty()) {
      var keyConstraintClass =
          ManagementUtils.forName(keyConstraint, CliStrings.CREATE_REGION__KEYCONSTRAINT);
      ((RegionFactory<Object, Object>) factory).setKeyConstraint(keyConstraintClass);
    }

    if (valueConstraint != null && !valueConstraint.isEmpty()) {
      var valueConstraintClass =
          ManagementUtils.forName(valueConstraint, CliStrings.CREATE_REGION__VALUECONSTRAINT);
      ((RegionFactory<Object, Object>) factory).setValueConstraint(valueConstraintClass);
    }

    if (regionAttributes.getCompressor() != null) {
      ((RegionFactory<Object, Object>) factory)
          .setCompressor(DeclarableTypeInstantiator.newInstance(regionAttributes.getCompressor()));
    }

    if (regionAttributes.getPartitionAttributes() != null) {
      factory.setPartitionAttributes(
          convertToRegionFactoryPartitionAttributes(regionAttributes.getPartitionAttributes(),
              cache));
    }

    if (regionAttributes.getEntryIdleTime() != null) {
      var eitl = regionAttributes.getEntryIdleTime();
      ((RegionFactory<Object, Object>) factory).setEntryIdleTimeout(
          new ExpirationAttributes(Integer.parseInt(eitl.getTimeout()),
              ExpirationAction.fromXmlString(eitl.getAction())));


      if (eitl.getCustomExpiry() != null) {
        ((RegionFactory<Object, Object>) factory).setCustomEntryIdleTimeout(
            DeclarableTypeInstantiator.newInstance(eitl.getCustomExpiry(),
                cache));
      }
    }

    if (regionAttributes.getEntryTimeToLive() != null) {
      var ettl = regionAttributes.getEntryTimeToLive();
      ((RegionFactory<Object, Object>) factory).setEntryTimeToLive(
          new ExpirationAttributes(Integer.parseInt(ettl.getTimeout()),
              ExpirationAction.fromXmlString(ettl.getAction())));

      if (ettl.getCustomExpiry() != null) {
        ((RegionFactory<Object, Object>) factory)
            .setCustomEntryTimeToLive(DeclarableTypeInstantiator.newInstance(ettl.getCustomExpiry(),
                cache));
      }
    }

    if (regionAttributes.getRegionIdleTime() != null) {
      var ritl = regionAttributes.getRegionIdleTime();
      ((RegionFactory<Object, Object>) factory).setRegionIdleTimeout(
          new ExpirationAttributes(Integer.parseInt(ritl.getTimeout()),
              ExpirationAction.fromXmlString(ritl.getAction())));
    }

    if (regionAttributes.getRegionTimeToLive() != null) {
      var rttl = regionAttributes.getRegionTimeToLive();
      ((RegionFactory<Object, Object>) factory).setRegionTimeToLive(
          new ExpirationAttributes(Integer.parseInt(rttl.getTimeout()),
              ExpirationAction.fromXmlString(rttl.getAction())));
    }

    if (regionAttributes.getEvictionAttributes() != null) {
      try {
        factory.setEvictionAttributes(
            EvictionAttributesImpl.fromConfig(regionAttributes.getEvictionAttributes()));
      } catch (Exception e) {
        throw new IllegalArgumentException(
            CliStrings.CREATE_REGION__MSG__OBJECT_SIZER_MUST_BE_OBJECTSIZER_AND_DECLARABLE);
      }
    }

    if (regionAttributes.getDiskStoreName() != null) {
      factory.setDiskStoreName(regionAttributes.getDiskStoreName());
    }

    if (regionAttributes.isDiskSynchronous() != null) {
      factory.setDiskSynchronous(regionAttributes.isDiskSynchronous());
    }

    if (regionAttributes.isOffHeap() != null) {
      factory.setOffHeap(regionAttributes.isOffHeap());
    }

    if (regionAttributes.isStatisticsEnabled() != null) {
      factory.setStatisticsEnabled(regionAttributes.isStatisticsEnabled());
    }

    if (regionAttributes.isEnableAsyncConflation() != null) {
      factory.setEnableAsyncConflation(regionAttributes.isEnableAsyncConflation());
    }

    if (regionAttributes.isEnableSubscriptionConflation() != null) {
      factory.setEnableSubscriptionConflation(regionAttributes.isEnableSubscriptionConflation());
    }

    if (regionAttributes.getGatewaySenderIds() != null) {
      Arrays.stream(regionAttributes.getGatewaySenderIds().split(","))
          .forEach(factory::addGatewaySenderId);
    }

    if (regionAttributes.getAsyncEventQueueIds() != null) {
      Arrays.stream(regionAttributes.getAsyncEventQueueIds().split(","))
          .forEach(factory::addAsyncEventQueueId);
    }

    factory.setConcurrencyChecksEnabled(regionAttributes.isConcurrencyChecksEnabled());

    if (regionAttributes.getConcurrencyLevel() != null) {
      factory.setConcurrencyLevel(Integer.parseInt(regionAttributes.getConcurrencyLevel()));
    }

    if (regionAttributes.isCloningEnabled() != null) {
      factory.setCloningEnabled(regionAttributes.isCloningEnabled());
    }

    if (regionAttributes.isMulticastEnabled() != null) {
      factory.setMulticastEnabled(regionAttributes.isMulticastEnabled());
    }
    return factory;
  }

  PartitionAttributesImpl convertToRegionFactoryPartitionAttributes(
      RegionAttributesType.PartitionAttributes configAttributes, Cache cache) {
    var partitionAttributes = new PartitionAttributesImpl();
    if (configAttributes == null) {
      return null;
    }

    if (configAttributes.getRedundantCopies() != null) {
      partitionAttributes
          .setRedundantCopies(Integer.parseInt(configAttributes.getRedundantCopies()));
    }

    if (configAttributes.getTotalMaxMemory() != null) {
      partitionAttributes.setTotalMaxMemory(Integer.parseInt(configAttributes.getTotalMaxMemory()));
    }

    if (configAttributes.getTotalNumBuckets() != null) {
      partitionAttributes
          .setTotalNumBuckets(Integer.parseInt(configAttributes.getTotalNumBuckets()));
    }

    if (configAttributes.getLocalMaxMemory() != null) {
      partitionAttributes.setLocalMaxMemory(Integer.parseInt(configAttributes.getLocalMaxMemory()));
    }

    if (configAttributes.getColocatedWith() != null) {
      partitionAttributes.setColocatedWith(configAttributes.getColocatedWith());
    }

    if (configAttributes.getPartitionResolver() != null) {
      partitionAttributes.setPartitionResolver(
          DeclarableTypeInstantiator.newInstance(configAttributes.getPartitionResolver(), cache));
    }

    if (configAttributes.getRecoveryDelay() != null) {
      partitionAttributes.setRecoveryDelay(Long.parseLong(configAttributes.getRecoveryDelay()));
    }

    if (configAttributes.getStartupRecoveryDelay() != null) {
      partitionAttributes
          .setStartupRecoveryDelay(Long.parseLong(configAttributes.getStartupRecoveryDelay()));
    }

    if (configAttributes.getPartitionListeners() != null) {
      var configListeners = configAttributes.getPartitionListeners();
      for (var configListener : configListeners) {
        partitionAttributes.addPartitionListener(
            DeclarableTypeInstantiator.newInstance(configListener, cache));
      }
    }

    return partitionAttributes;
  }


  @Override
  public RuntimeRegionInfo get(Region config, InternalCache cache) {
    var region =
        cache.getRegion(SEPARATOR + config.getName());
    if (region == null) {
      return null;
    }

    var info = new RuntimeRegionInfo();
    info.setEntryCount(region.size());
    return info;
  }

  /**
   * the default implementation will have the extra work of getting the runtime info which is
   * unnecessary. It will also have some concurrency issue if the region is being destroyed.
   */
  @Override
  public boolean exists(Region config, InternalCache cache) {
    var region =
        cache.getRegion(SEPARATOR + config.getName());
    if (region == null) {
      return false;
    }

    return !region.isDestroyed();
  }

  @Override
  public RealizationResult update(Region config, InternalCache cache) {
    throw new NotImplementedException("Not implemented");
  }

  @Override
  public RealizationResult delete(Region config, InternalCache cache) {
    org.apache.geode.cache.Region region = cache.getRegion(config.getName());
    if (region == null) {
      // Since we are trying to delete this region, we can return early
      return new RealizationResult().setMessage("Region does not exist.");
    }

    try {
      region.destroyRegion();
    } catch (RegionDestroyedException dex) {
      // Probably happened as a distributed op but it still reflects our current desired action
      // which is why it can be ignored here.
      return new RealizationResult().setMessage("Region does not exist.");
    }
    return new RealizationResult().setMessage("Region successfully deleted.");
  }

}
