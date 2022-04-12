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
package org.apache.geode.internal.cache;

import static org.apache.geode.distributed.internal.InternalDistributedSystem.ALLOW_MULTIPLE_SYSTEMS_PROPERTY;
import static org.apache.geode.internal.util.InternalCacheBuilderTestUtil.CacheState.CLOSED;
import static org.apache.geode.internal.util.InternalCacheBuilderTestUtil.CacheState.OPEN;
import static org.apache.geode.internal.util.InternalCacheBuilderTestUtil.THROWING_CACHE_CONSTRUCTOR;
import static org.apache.geode.internal.util.InternalCacheBuilderTestUtil.THROWING_CACHE_SUPPLIER;
import static org.apache.geode.internal.util.InternalCacheBuilderTestUtil.THROWING_SYSTEM_CONSTRUCTOR;
import static org.apache.geode.internal.util.InternalCacheBuilderTestUtil.THROWING_SYSTEM_SUPPLIER;
import static org.apache.geode.internal.util.InternalCacheBuilderTestUtil.cache;
import static org.apache.geode.internal.util.InternalCacheBuilderTestUtil.constructedCache;
import static org.apache.geode.internal.util.InternalCacheBuilderTestUtil.constructedSystem;
import static org.apache.geode.internal.util.InternalCacheBuilderTestUtil.constructorOf;
import static org.apache.geode.internal.util.InternalCacheBuilderTestUtil.systemWith;
import static org.apache.geode.internal.util.InternalCacheBuilderTestUtil.systemWithNoCache;
import static org.apache.geode.internal.util.InternalCacheBuilderTestUtil.throwingCacheConfig;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

import java.util.Properties;

import org.assertj.core.api.ThrowableAssert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;
import org.mockito.Mock;

import org.apache.geode.cache.CacheExistsException;
import org.apache.geode.metrics.internal.MetricsService;

/**
 * Unit tests for {@link InternalCacheBuilder} when
 * {@code InternalDistributedSystem.ALLOW_MULTIPLE_SYSTEMS} is set to true.
 */
public class InternalCacheBuilderAllowsMultipleSystemsTest {

  @Rule
  public RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();

  @Mock
  private MetricsService.Builder metricsSessionBuilder;


  @Before
  public void setUp() {
    initMocks(this);

    System.setProperty(ALLOW_MULTIPLE_SYSTEMS_PROPERTY, "true");
  }

  @Test
  public void create_throwsNullPointerException_ifConfigPropertiesIsNull() {
    var internalCacheBuilder = new InternalCacheBuilder(
        null, new CacheConfig(), metricsSessionBuilder, THROWING_SYSTEM_SUPPLIER,
        constructorOf(constructedSystem()),
        THROWING_CACHE_SUPPLIER, constructorOf(constructedCache()));

    assertThatThrownBy(internalCacheBuilder::create)
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  public void create_throwsNullPointerException_andCacheConfigIsNull() {
    var internalCacheBuilder = new InternalCacheBuilder(
        new Properties(), null, metricsSessionBuilder, THROWING_SYSTEM_SUPPLIER,
        constructorOf(constructedSystem()),
        THROWING_CACHE_SUPPLIER, constructorOf(constructedCache()));

    assertThatThrownBy(internalCacheBuilder::create)
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  public void create_constructsSystem_withGivenProperties_ifNoSystemExists() {
    var constructedCache = constructedCache();

    var systemConstructor = constructorOf(constructedSystem());
    var configProperties = new Properties();

    var internalCacheBuilder = new InternalCacheBuilder(
        configProperties, new CacheConfig(), metricsSessionBuilder, THROWING_SYSTEM_SUPPLIER,
        systemConstructor, THROWING_CACHE_SUPPLIER, constructorOf(constructedCache));

    internalCacheBuilder.create();

    verify(systemConstructor).construct(same(configProperties), any(), any());
  }

  @Test
  public void create_returnsConstructedCache_ifNoSystemExists() {
    var constructedCache = constructedCache();

    var internalCacheBuilder = new InternalCacheBuilder(
        new Properties(), new CacheConfig(), metricsSessionBuilder, THROWING_SYSTEM_SUPPLIER,
        constructorOf(constructedSystem()), THROWING_CACHE_SUPPLIER,
        constructorOf(constructedCache));

    var result = internalCacheBuilder.create();

    assertThat(result).isSameAs(constructedCache);
  }

  @Test
  public void create_setsConstructedCache_onConstructedSystem_ifNoSystemExists() {
    var constructedSystem = constructedSystem();
    var constructedCache = constructedCache();

    var internalCacheBuilder = new InternalCacheBuilder(
        new Properties(), new CacheConfig(), metricsSessionBuilder, THROWING_SYSTEM_SUPPLIER,
        constructorOf(constructedSystem), THROWING_CACHE_SUPPLIER, constructorOf(constructedCache));

    internalCacheBuilder.create();

    verify(constructedSystem).setCache(same(constructedCache));
  }

  @Test
  public void create_setsConstructedSystem_onConstructedCache_ifNoSystemExists() {
    var constructedSystem = constructedSystem();

    var cacheConstructor = constructorOf(constructedCache());

    var internalCacheBuilder = new InternalCacheBuilder(
        new Properties(), new CacheConfig(), metricsSessionBuilder, THROWING_SYSTEM_SUPPLIER,
        constructorOf(constructedSystem), THROWING_CACHE_SUPPLIER, cacheConstructor);

    internalCacheBuilder.create();

    verify(cacheConstructor)
        .construct(anyBoolean(), any(), same(constructedSystem), any(), anyBoolean(), any());
  }


  @Test
  public void createWithSystem_throwsNullPointerException_ifSystemIsNull() {
    var internalCacheBuilder = new InternalCacheBuilder(
        new Properties(), new CacheConfig(), metricsSessionBuilder, THROWING_SYSTEM_SUPPLIER,
        THROWING_SYSTEM_CONSTRUCTOR,
        THROWING_CACHE_SUPPLIER, THROWING_CACHE_CONSTRUCTOR);

    var thrown = catchThrowable(() -> internalCacheBuilder.create(null));

    assertThat(thrown).isInstanceOf(NullPointerException.class);
  }

  @Test
  public void createWithSystem_returnsConstructedCache_ifSystemCacheDoesNotExist() {
    var constructedCache = constructedCache();

    var internalCacheBuilder = new InternalCacheBuilder(
        new Properties(), new CacheConfig(), metricsSessionBuilder, THROWING_SYSTEM_SUPPLIER,
        THROWING_SYSTEM_CONSTRUCTOR,
        THROWING_CACHE_SUPPLIER, constructorOf(constructedCache));

    var result = internalCacheBuilder
        .create(systemWithNoCache());

    assertThat(result).isSameAs(constructedCache);
  }

  @Test
  public void createWithSystem_setsConstructedCache_onGivenSystem_ifSystemCacheDoesNotExist() {
    var givenSystem = systemWithNoCache();
    var constructedCache = constructedCache();

    var internalCacheBuilder = new InternalCacheBuilder(
        new Properties(), new CacheConfig(), metricsSessionBuilder, THROWING_SYSTEM_SUPPLIER,
        THROWING_SYSTEM_CONSTRUCTOR,
        THROWING_CACHE_SUPPLIER, constructorOf(constructedCache));

    internalCacheBuilder
        .create(givenSystem);

    verify(givenSystem).setCache(same(constructedCache));
  }

  @Test
  public void createWithSystem_setsGivenSystem_onConstructedCache_ifSystemCacheDoesNotExist() {
    var givenSystem = systemWithNoCache();

    var cacheConstructor = constructorOf(constructedCache());

    var internalCacheBuilder = new InternalCacheBuilder(
        new Properties(), new CacheConfig(), metricsSessionBuilder, THROWING_SYSTEM_SUPPLIER,
        THROWING_SYSTEM_CONSTRUCTOR,
        THROWING_CACHE_SUPPLIER, cacheConstructor);

    internalCacheBuilder
        .create(givenSystem);

    verify(cacheConstructor)
        .construct(anyBoolean(), any(), same(givenSystem), any(), anyBoolean(), any());
  }

  @Test
  public void createWithSystem_returnsConstructedCache_ifGivenSystemHasClosedCache() {
    var closedCache = cache(CLOSED);
    var givenSystem = systemWith(closedCache);

    var constructedCache = constructedCache();
    var internalCacheBuilder = new InternalCacheBuilder(
        new Properties(), new CacheConfig(), metricsSessionBuilder, THROWING_SYSTEM_SUPPLIER,
        THROWING_SYSTEM_CONSTRUCTOR,
        THROWING_CACHE_SUPPLIER, constructorOf(constructedCache));

    var result = internalCacheBuilder
        .create(givenSystem);

    assertThat(result).isSameAs(constructedCache);
  }

  @Test
  public void createWithSystem_setsConstructedCache_onGivenSystem_ifSystemCacheIsClosed() {
    var closedCache = cache(CLOSED);
    var givenSystem = systemWith(closedCache);

    var constructedCache = constructedCache();

    var internalCacheBuilder = new InternalCacheBuilder(
        new Properties(), new CacheConfig(), metricsSessionBuilder, THROWING_SYSTEM_SUPPLIER,
        THROWING_SYSTEM_CONSTRUCTOR, THROWING_CACHE_SUPPLIER, constructorOf(constructedCache));

    internalCacheBuilder
        .create(givenSystem);

    verify(givenSystem).setCache(same(constructedCache));
  }

  @Test
  public void createWithSystem_setsGivenSystem_onConstructedCache_ifSystemCacheIsClosed() {
    var closedCache = cache(CLOSED);
    var givenSystem = systemWith(closedCache);

    var cacheConstructor = constructorOf(constructedCache());

    var internalCacheBuilder = new InternalCacheBuilder(
        new Properties(), new CacheConfig(), metricsSessionBuilder, THROWING_SYSTEM_SUPPLIER,
        THROWING_SYSTEM_CONSTRUCTOR,
        THROWING_CACHE_SUPPLIER, cacheConstructor);

    internalCacheBuilder
        .create(givenSystem);

    verify(cacheConstructor)
        .construct(anyBoolean(), any(), same(givenSystem), any(), anyBoolean(), any());
  }

  @Test
  public void createWithSystem_throwsCacheExistsException_ifSystemCacheIsOpen_butExistingNotOk() {
    var openCache = cache(OPEN);
    var givenSystem = systemWith(openCache);

    var internalCacheBuilder = new InternalCacheBuilder(
        new Properties(), new CacheConfig(), metricsSessionBuilder, THROWING_SYSTEM_SUPPLIER,
        THROWING_SYSTEM_CONSTRUCTOR,
        THROWING_CACHE_SUPPLIER, THROWING_CACHE_CONSTRUCTOR);

    var thrown = catchThrowable(() -> internalCacheBuilder
        .setIsExistingOk(false)
        .create(givenSystem));

    assertThat(thrown).isInstanceOf(CacheExistsException.class);
  }

  @Test
  public void createWithSystem_doesNotSetSystemCache_onGivenSystem__ifSystemCacheIsOpen_butExistingNotOk() {
    var openCache = cache(OPEN);
    var givenSystem = systemWith(openCache);

    var internalCacheBuilder = new InternalCacheBuilder(
        new Properties(), new CacheConfig(), metricsSessionBuilder, THROWING_SYSTEM_SUPPLIER,
        THROWING_SYSTEM_CONSTRUCTOR,
        THROWING_CACHE_SUPPLIER, THROWING_CACHE_CONSTRUCTOR);

    ignoreThrowable(() -> internalCacheBuilder
        .setIsExistingOk(false)
        .create(givenSystem));

    verify(givenSystem, never()).setCache(any());
  }

  @Test
  public void createWithSystem_propagatesCacheConfigException_ifSystemCacheIsOpen_andExistingOk_butCacheIsIncompatible() {
    var incompatibleOpenCache = cache("incompatible", OPEN);
    var givenSystem = systemWith(incompatibleOpenCache);

    Throwable thrownByCacheConfig = new IllegalStateException("incompatible");

    var internalCacheBuilder = new InternalCacheBuilder(
        new Properties(), throwingCacheConfig(thrownByCacheConfig), metricsSessionBuilder,
        THROWING_SYSTEM_SUPPLIER,
        THROWING_SYSTEM_CONSTRUCTOR, THROWING_CACHE_SUPPLIER, THROWING_CACHE_CONSTRUCTOR);

    var thrown = catchThrowable(() -> internalCacheBuilder
        .setIsExistingOk(true)
        .create(givenSystem));

    assertThat(thrown).isSameAs(thrownByCacheConfig);
  }

  @Test
  public void createWithSystem_doesNotSetSystemCache_onGivenSystem_ifSystemCacheIsOpen_andExistingOk_butCacheIsNotCompatible() {
    var incompatibleOpenCache = cache("incompatible", OPEN);
    var givenSystem = systemWith(incompatibleOpenCache);
    when(givenSystem.getCache()).thenReturn(incompatibleOpenCache);

    var internalCacheBuilder = new InternalCacheBuilder(
        new Properties(), throwingCacheConfig(new IllegalStateException("incompatible")),
        metricsSessionBuilder,
        THROWING_SYSTEM_SUPPLIER, THROWING_SYSTEM_CONSTRUCTOR, THROWING_CACHE_SUPPLIER,
        THROWING_CACHE_CONSTRUCTOR);

    ignoreThrowable(() -> internalCacheBuilder
        .setIsExistingOk(true)
        .create(givenSystem));

    verify(givenSystem, never()).setCache(any());
  }

  @Test
  public void createWithSystem_returnsSystemCache_ifSystemCacheIsOpen_andExistingOk_andCacheIsCompatible() {
    var compatibleOpenCache = cache("compatible", OPEN);
    var givenSystem = systemWith(compatibleOpenCache);

    var internalCacheBuilder = new InternalCacheBuilder(
        new Properties(), new CacheConfig(), metricsSessionBuilder, THROWING_SYSTEM_SUPPLIER,
        THROWING_SYSTEM_CONSTRUCTOR,
        THROWING_CACHE_SUPPLIER, THROWING_CACHE_CONSTRUCTOR);

    var result = internalCacheBuilder
        .setIsExistingOk(true)
        .create(givenSystem);

    assertThat(result).isSameAs(compatibleOpenCache);
  }

  @Test
  public void createWithSystem_setsSystemCache_onGivenSystem_ifSystemCacheIsOpen_andExistingOk_andCacheIsCompatible() {
    var compatibleOpenCache = cache("compatible", OPEN);
    var givenSystem = systemWith(compatibleOpenCache);
    when(givenSystem.getCache()).thenReturn(compatibleOpenCache);

    var internalCacheBuilder = new InternalCacheBuilder(
        new Properties(), new CacheConfig(), metricsSessionBuilder, THROWING_SYSTEM_SUPPLIER,
        THROWING_SYSTEM_CONSTRUCTOR, THROWING_CACHE_SUPPLIER, THROWING_CACHE_CONSTRUCTOR);

    internalCacheBuilder
        .setIsExistingOk(true)
        .create(givenSystem);

    verify(givenSystem).setCache(same(compatibleOpenCache));
  }

  private static void ignoreThrowable(ThrowableAssert.ThrowingCallable shouldRaiseThrowable) {
    try {
      shouldRaiseThrowable.call();
    } catch (Throwable ignored) {
    }
  }
}
