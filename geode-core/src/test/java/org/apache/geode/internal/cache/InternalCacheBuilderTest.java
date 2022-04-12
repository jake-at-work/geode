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
import static org.apache.geode.internal.util.InternalCacheBuilderTestUtil.singletonSystem;
import static org.apache.geode.internal.util.InternalCacheBuilderTestUtil.supplierOf;
import static org.apache.geode.internal.util.InternalCacheBuilderTestUtil.systemWithNoCache;
import static org.apache.geode.internal.util.InternalCacheBuilderTestUtil.throwingCacheConfig;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.same;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

import java.util.Properties;
import java.util.function.Supplier;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import org.apache.geode.cache.CacheExistsException;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.metrics.internal.MetricsService;

/**
 * Unit tests for {@link InternalCacheBuilder}.
 */
public class InternalCacheBuilderTest {

  @Mock
  private Supplier<InternalDistributedSystem> nullSingletonSystemSupplier;

  @Mock
  private Supplier<InternalCache> nullSingletonCacheSupplier;

  @Mock
  private MetricsService.Builder metricsServiceBuilder;

  @Before
  public void setUp() {
    initMocks(this);

    when(nullSingletonSystemSupplier.get()).thenReturn(null);
    when(nullSingletonCacheSupplier.get()).thenReturn(null);
  }

  @Test
  public void setsMetricsServiceBuilderIsClientFalseByDefault() {
    var theMetricsServiceBuilder = mock(MetricsService.Builder.class);

    new InternalCacheBuilder(new Properties(), new CacheConfig(), theMetricsServiceBuilder,
        nullSingletonSystemSupplier, constructorOf(constructedSystem()), nullSingletonCacheSupplier,
        constructorOf(constructedCache()));

    verify(theMetricsServiceBuilder).setIsClient(false);
  }

  @Test
  public void addMeterSubregistry_addsGivenRegistryToMetricsServiceBuilder() {
    var internalCacheBuilder = new InternalCacheBuilder(
        new Properties(), new CacheConfig(), metricsServiceBuilder, nullSingletonSystemSupplier,
        constructorOf(constructedSystem()), nullSingletonCacheSupplier,
        constructorOf(constructedCache()));

    var addedMeterRegistry = new SimpleMeterRegistry();

    internalCacheBuilder.addMeterSubregistry(addedMeterRegistry);

    verify(metricsServiceBuilder).addPersistentMeterRegistry(same(addedMeterRegistry));
  }

  @Test
  public void addMeterSubregistry_throwsNullPointerExceptionIfTheGivenRegistryIsNull() {
    var internalCacheBuilder = new InternalCacheBuilder(
        new Properties(), new CacheConfig(), metricsServiceBuilder, nullSingletonSystemSupplier,
        constructorOf(constructedSystem()), nullSingletonCacheSupplier,
        constructorOf(constructedCache()));

    assertThatThrownBy(() -> internalCacheBuilder.addMeterSubregistry(null))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  public void setIsClient_setsIsClientInMetricsServiceBuilder() {
    var theMetricsServiceBuilder = mock(MetricsService.Builder.class);

    var internalCacheBuilder = new InternalCacheBuilder(
        new Properties(), new CacheConfig(), theMetricsServiceBuilder, nullSingletonSystemSupplier,
        constructorOf(constructedSystem()), nullSingletonCacheSupplier,
        constructorOf(constructedCache()));

    internalCacheBuilder.setIsClient(true);

    verify(theMetricsServiceBuilder).setIsClient(true);
  }

  @Test
  public void create_throwsNullPointerException_ifConfigPropertiesIsNull() {
    var internalCacheBuilder = new InternalCacheBuilder(
        null, new CacheConfig(), metricsServiceBuilder, nullSingletonSystemSupplier,
        constructorOf(constructedSystem()),
        nullSingletonCacheSupplier, constructorOf(constructedCache()));

    assertThatThrownBy(internalCacheBuilder::create)
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  public void create_throwsNullPointerException_andCacheConfigIsNull() {
    var internalCacheBuilder = new InternalCacheBuilder(
        new Properties(), null, metricsServiceBuilder, nullSingletonSystemSupplier,
        constructorOf(constructedSystem()), nullSingletonCacheSupplier,
        constructorOf(constructedCache()));

    assertThatThrownBy(internalCacheBuilder::create)
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  public void create_constructsSystem_withGivenProperties_ifNoSystemExists_andNoCacheExists() {
    var systemConstructor = constructorOf(constructedSystem());
    var configProperties = new Properties();

    var internalCacheBuilder = new InternalCacheBuilder(
        configProperties, new CacheConfig(), metricsServiceBuilder, nullSingletonSystemSupplier,
        systemConstructor, nullSingletonCacheSupplier, constructorOf(constructedCache()));

    internalCacheBuilder
        .create();

    verify(systemConstructor).construct(same(configProperties), any(), any());
  }

  @Test
  public void create_passesIsClientToSystemConstructor_isNoSystemExists() {
    var cacheConstructor = constructorOf(constructedCache());
    var internalCacheBuilder = new InternalCacheBuilder(
        new Properties(), new CacheConfig(), metricsServiceBuilder, nullSingletonSystemSupplier,
        constructorOf(constructedSystem()), nullSingletonCacheSupplier, cacheConstructor);

    internalCacheBuilder
        .setIsClient(true)
        .create();

    verify(cacheConstructor)
        .construct(eq(true), any(), any(), any(), anyBoolean(), any());
  }

  @Test
  public void create_passesMetricsServiceBuilderToSystemConstructor_ifNoSystemExists() {
    var systemConstructor = constructorOf(constructedSystem());

    var theMetricsServiceBuilder = mock(MetricsService.Builder.class);
    var internalCacheBuilder = new InternalCacheBuilder(
        new Properties(), new CacheConfig(), theMetricsServiceBuilder, nullSingletonSystemSupplier,
        systemConstructor, nullSingletonCacheSupplier, constructorOf(constructedCache()));

    internalCacheBuilder.create();

    verify(systemConstructor).construct(any(), any(), same(theMetricsServiceBuilder));
  }

  @Test
  public void create_returnsConstructedCache_ifNoSystemExists() {
    var constructedCache = constructedCache();

    var internalCacheBuilder = new InternalCacheBuilder(
        new Properties(), new CacheConfig(), metricsServiceBuilder, nullSingletonSystemSupplier,
        constructorOf(constructedSystem()), nullSingletonCacheSupplier,
        constructorOf(constructedCache));

    var result = internalCacheBuilder
        .create();

    assertThat(result).isSameAs(constructedCache);
  }

  @Test
  public void create_setsConstructedCache_onConstructedSystem_ifNoSystemExists() {
    var constructedSystem = constructedSystem();
    var constructedCache = constructedCache();

    var internalCacheBuilder = new InternalCacheBuilder(
        new Properties(), new CacheConfig(), metricsServiceBuilder, nullSingletonSystemSupplier,
        constructorOf(constructedSystem), nullSingletonCacheSupplier,
        constructorOf(constructedCache));

    internalCacheBuilder
        .create();

    verify(constructedSystem).setCache(same(constructedCache));
  }

  @Test
  public void create_setsConstructedSystem_onConstructedCache_ifNoSystemExists_() {
    var constructedSystem = constructedSystem();
    var constructedCache = constructedCache();

    var cacheConstructor = constructorOf(constructedCache);

    var internalCacheBuilder = new InternalCacheBuilder(
        new Properties(), new CacheConfig(), metricsServiceBuilder, nullSingletonSystemSupplier,
        constructorOf(constructedSystem), nullSingletonCacheSupplier, cacheConstructor);

    internalCacheBuilder
        .create();

    verify(cacheConstructor)
        .construct(anyBoolean(), any(), same(constructedSystem), any(), anyBoolean(), any());
  }

  @Test
  public void create_returnsConstructedCache_ifSingletonSystemExists_andNoCacheExists() {
    var constructedCache = constructedCache();

    var internalCacheBuilder = new InternalCacheBuilder(
        new Properties(), new CacheConfig(), metricsServiceBuilder, supplierOf(singletonSystem()),
        THROWING_SYSTEM_CONSTRUCTOR, nullSingletonCacheSupplier, constructorOf(constructedCache));

    var result = internalCacheBuilder
        .create();

    assertThat(result).isSameAs(constructedCache);
  }

  @Test
  public void create_setsSingletonSystem_onConstructedCache_ifSingletonSystemExists_andNoCacheExists() {
    var singletonSystem = singletonSystem();
    var constructedCache = constructedCache();

    var cacheConstructor = constructorOf(constructedCache);

    var internalCacheBuilder = new InternalCacheBuilder(
        new Properties(), new CacheConfig(), metricsServiceBuilder, supplierOf(singletonSystem),
        THROWING_SYSTEM_CONSTRUCTOR, nullSingletonCacheSupplier, cacheConstructor);

    internalCacheBuilder
        .create();

    verify(singletonSystem).setCache(same(constructedCache));
  }

  @Test
  public void create_setsConstructedCache_onSingletonSystem_ifSingletonSystemExists_andNoCacheExists() {
    var singletonSystem = singletonSystem();
    var constructedCache = constructedCache();

    var cacheConstructor = constructorOf(constructedCache);

    var internalCacheBuilder = new InternalCacheBuilder(
        new Properties(), new CacheConfig(), metricsServiceBuilder, supplierOf(singletonSystem),
        THROWING_SYSTEM_CONSTRUCTOR, nullSingletonCacheSupplier, cacheConstructor);

    internalCacheBuilder
        .create();

    verify(cacheConstructor)
        .construct(anyBoolean(), any(), same(singletonSystem), any(), anyBoolean(), any());
  }

  @Test
  public void create_returnsConstructedCache_ifSingletonSystemExists_andSingletonCacheIsClosed() {
    var constructedCache = constructedCache();

    var internalCacheBuilder = new InternalCacheBuilder(
        new Properties(), new CacheConfig(), metricsServiceBuilder, supplierOf(singletonSystem()),
        THROWING_SYSTEM_CONSTRUCTOR, supplierOf(cache("singleton", CLOSED)),
        constructorOf(constructedCache));

    var result = internalCacheBuilder
        .create();

    assertThat(result).isSameAs(constructedCache);
  }

  @Test
  public void create_setsConstructedCache_onSingletonSystem_ifSingletonSystemExists_andSingletonCacheIsClosed() {
    var singletonSystem = singletonSystem();
    var constructedCache = constructedCache();

    var internalCacheBuilder = new InternalCacheBuilder(
        new Properties(), new CacheConfig(), metricsServiceBuilder, supplierOf(singletonSystem),
        THROWING_SYSTEM_CONSTRUCTOR, supplierOf(cache("singleton", CLOSED)),
        constructorOf(constructedCache));

    internalCacheBuilder
        .create();

    verify(singletonSystem).setCache(same(constructedCache));
  }

  @Test
  public void create_setsSingletonSystem_onConstructedCache_ifSingletonSystemExists_andSingletonCacheIsClosed() {
    var singletonSystem = singletonSystem();

    var cacheConstructor = constructorOf(constructedCache());

    var internalCacheBuilder = new InternalCacheBuilder(
        new Properties(), new CacheConfig(), metricsServiceBuilder, supplierOf(singletonSystem),
        THROWING_SYSTEM_CONSTRUCTOR, supplierOf(cache("singleton", CLOSED)), cacheConstructor);

    internalCacheBuilder
        .create();

    verify(cacheConstructor)
        .construct(anyBoolean(), any(), same(singletonSystem), any(), anyBoolean(), any());
  }

  @Test
  public void create_throwsCacheExistsException_ifSingletonSystemExists_andSingletonCacheIsOpen_butExistingIsNotOk() {
    var internalCacheBuilder = new InternalCacheBuilder(
        new Properties(), new CacheConfig(), metricsServiceBuilder, supplierOf(singletonSystem()),
        THROWING_SYSTEM_CONSTRUCTOR, supplierOf(cache("singleton", OPEN)),
        THROWING_CACHE_CONSTRUCTOR);

    var thrown = catchThrowable(() -> internalCacheBuilder
        .setIsExistingOk(false)
        .create());

    assertThat(thrown).isInstanceOf(CacheExistsException.class);
  }

  @Test
  public void create_propagatesCacheConfigException_ifSingletonSystemExists_andSingletonCacheIsOpen_andExistingIsOk_butCacheIsIncompatible() {
    Throwable thrownByCacheConfig = new IllegalStateException("incompatible");

    var internalCacheBuilder = new InternalCacheBuilder(
        new Properties(), throwingCacheConfig(thrownByCacheConfig), metricsServiceBuilder,
        supplierOf(singletonSystem()),
        THROWING_SYSTEM_CONSTRUCTOR, supplierOf(cache("singleton", OPEN)),
        THROWING_CACHE_CONSTRUCTOR);

    var thrown = catchThrowable(() -> internalCacheBuilder
        .setIsExistingOk(true)
        .create());

    assertThat(thrown).isSameAs(thrownByCacheConfig);
  }

  @Test
  public void create_returnsSingletonCache_ifSingletonCacheIsOpen() {
    var singletonCache = cache("singleton", OPEN);

    var internalCacheBuilder = new InternalCacheBuilder(
        new Properties(), new CacheConfig(), metricsServiceBuilder, supplierOf(singletonSystem()),
        THROWING_SYSTEM_CONSTRUCTOR, supplierOf(singletonCache), THROWING_CACHE_CONSTRUCTOR);

    var result = internalCacheBuilder
        .create();

    assertThat(result).isSameAs(singletonCache);
  }

  @Test
  public void createWithSystem_throwsNullPointerException_ifSystemIsNull() {
    var internalCacheBuilder = new InternalCacheBuilder(
        new Properties(), new CacheConfig(), metricsServiceBuilder, THROWING_SYSTEM_SUPPLIER,
        THROWING_SYSTEM_CONSTRUCTOR,
        THROWING_CACHE_SUPPLIER, THROWING_CACHE_CONSTRUCTOR);

    var thrown = catchThrowable(() -> internalCacheBuilder
        .create(null));

    assertThat(thrown).isInstanceOf(NullPointerException.class);
  }

  @Test
  public void createWithSystem_returnsConstructedCache_ifNoCacheExists() {
    var constructedCache = constructedCache();

    var internalCacheBuilder = new InternalCacheBuilder(
        new Properties(), new CacheConfig(), metricsServiceBuilder, THROWING_SYSTEM_SUPPLIER,
        THROWING_SYSTEM_CONSTRUCTOR,
        nullSingletonCacheSupplier, constructorOf(constructedCache));

    var result = internalCacheBuilder
        .create(systemWithNoCache());

    assertThat(result).isSameAs(constructedCache);
  }

  @Test
  public void createWithSystem_setsConstructedCache_onGivenSystem_ifNoCacheExists() {
    var givenSystem = systemWithNoCache();
    var constructedCache = constructedCache();

    var internalCacheBuilder = new InternalCacheBuilder(
        new Properties(), new CacheConfig(), metricsServiceBuilder, THROWING_SYSTEM_SUPPLIER,
        THROWING_SYSTEM_CONSTRUCTOR,
        nullSingletonCacheSupplier, constructorOf(constructedCache));

    internalCacheBuilder
        .create(givenSystem);

    verify(givenSystem).setCache(same(constructedCache));
  }

  @Test
  public void createWithSystem_setsGivenSystem_onConstructedCache_ifNoCacheExists() {
    var givenSystem = systemWithNoCache();

    var cacheConstructor = constructorOf(constructedCache());

    var internalCacheBuilder = new InternalCacheBuilder(
        new Properties(), new CacheConfig(), metricsServiceBuilder, THROWING_SYSTEM_SUPPLIER,
        THROWING_SYSTEM_CONSTRUCTOR,
        nullSingletonCacheSupplier, cacheConstructor);

    internalCacheBuilder
        .create(givenSystem);

    verify(cacheConstructor)
        .construct(anyBoolean(), any(), same(givenSystem), any(), anyBoolean(), any());
  }

  @Test
  public void createWithSystem_returnsConstructedCache_ifSingletonCacheIsClosed() {
    var constructedCache = constructedCache();

    var internalCacheBuilder = new InternalCacheBuilder(
        new Properties(), new CacheConfig(), metricsServiceBuilder, THROWING_SYSTEM_SUPPLIER,
        THROWING_SYSTEM_CONSTRUCTOR,
        supplierOf(cache("singleton", CLOSED)), constructorOf(constructedCache));

    var result = internalCacheBuilder
        .create(systemWithNoCache());

    assertThat(result).isSameAs(constructedCache);
  }

  @Test
  public void createWithSystem_setsConstructedCache_onGivenSystem_ifSingletonCacheIsClosed() {
    var givenSystem = systemWithNoCache();
    var constructedCache = constructedCache();

    var internalCacheBuilder = new InternalCacheBuilder(
        new Properties(), new CacheConfig(), metricsServiceBuilder, THROWING_SYSTEM_SUPPLIER,
        THROWING_SYSTEM_CONSTRUCTOR,
        supplierOf(cache("singleton", CLOSED)), constructorOf(constructedCache));

    internalCacheBuilder
        .create(givenSystem);

    verify(givenSystem).setCache(same(constructedCache));
  }

  @Test
  public void createWithSystem_setsGivenSystem_onConstructedCache_ifSingletonCacheIsClosed() {
    var givenSystem = systemWithNoCache();

    var cacheConstructor = constructorOf(constructedCache());

    var internalCacheBuilder = new InternalCacheBuilder(
        new Properties(), new CacheConfig(), metricsServiceBuilder, THROWING_SYSTEM_SUPPLIER,
        THROWING_SYSTEM_CONSTRUCTOR,
        supplierOf(cache("singleton", CLOSED)), cacheConstructor);

    internalCacheBuilder
        .create(givenSystem);

    verify(cacheConstructor)
        .construct(anyBoolean(), any(), same(givenSystem), any(), anyBoolean(), any());
  }

  @Test
  public void createWithSystem_throwsCacheExistsException_ifSingletonCacheIsOpen_butExistingIsNotOk() {
    var internalCacheBuilder = new InternalCacheBuilder(
        new Properties(), new CacheConfig(), metricsServiceBuilder, THROWING_SYSTEM_SUPPLIER,
        THROWING_SYSTEM_CONSTRUCTOR,
        supplierOf(cache("singleton", OPEN)), THROWING_CACHE_CONSTRUCTOR);

    var thrown = catchThrowable(() -> internalCacheBuilder
        .setIsExistingOk(false)
        .create(systemWithNoCache()));

    assertThat(thrown).isInstanceOf(CacheExistsException.class);
  }

  @Test
  public void createWithSystem_doesNotSetSingletonCache_onGivenSystem_ifSingletonCacheIsOpen_butExistingIsNotOk() {
    var givenSystem = systemWithNoCache();

    var internalCacheBuilder = new InternalCacheBuilder(
        new Properties(), new CacheConfig(), metricsServiceBuilder, THROWING_SYSTEM_SUPPLIER,
        THROWING_SYSTEM_CONSTRUCTOR,
        supplierOf(cache("singleton", OPEN)), THROWING_CACHE_CONSTRUCTOR);

    ignoreThrowable(() -> internalCacheBuilder
        .setIsExistingOk(false)
        .create(givenSystem));

    verify(givenSystem, never()).setCache(any());
  }

  @Test
  public void createWithSystem_propagatesCacheConfigException_ifSingletonCacheIsOpen_andExistingIsOk_butCacheIsIncompatible() {
    Throwable thrownByCacheConfig = new IllegalStateException("incompatible");

    var internalCacheBuilder = new InternalCacheBuilder(
        new Properties(), throwingCacheConfig(thrownByCacheConfig), metricsServiceBuilder,
        THROWING_SYSTEM_SUPPLIER,
        THROWING_SYSTEM_CONSTRUCTOR, supplierOf(cache("singleton", OPEN)),
        THROWING_CACHE_CONSTRUCTOR);

    var thrown = catchThrowable(() -> internalCacheBuilder
        .setIsExistingOk(true)
        .create(systemWithNoCache()));

    assertThat(thrown).isSameAs(thrownByCacheConfig);
  }

  @Test
  public void createWithSystem_doesNotSetSingletonCache_onGivenSystem_ifSingletonCacheIsOpen_andExistingIsOk_butCacheIsNotCompatible() {
    var givenSystem = systemWithNoCache();

    var internalCacheBuilder = new InternalCacheBuilder(
        new Properties(), throwingCacheConfig(new IllegalStateException("incompatible")),
        metricsServiceBuilder, THROWING_SYSTEM_SUPPLIER, THROWING_SYSTEM_CONSTRUCTOR,
        supplierOf(cache("singleton", OPEN)),
        THROWING_CACHE_CONSTRUCTOR);

    ignoreThrowable(() -> internalCacheBuilder
        .setIsExistingOk(true)
        .create(givenSystem));

    verifyNoMoreInteractions(givenSystem);
  }

  @Test
  public void createWithSystem_returnsSingletonCache_ifSingletonCacheIsOpen_andExistingIsOk_andCacheIsCompatible() {
    var singletonCache = cache("singleton", OPEN);

    var internalCacheBuilder = new InternalCacheBuilder(
        new Properties(), new CacheConfig(), metricsServiceBuilder, THROWING_SYSTEM_SUPPLIER,
        THROWING_SYSTEM_CONSTRUCTOR,
        supplierOf(singletonCache), THROWING_CACHE_CONSTRUCTOR);

    var result = internalCacheBuilder
        .setIsExistingOk(true)
        .create(systemWithNoCache());

    assertThat(result).isSameAs(singletonCache);
  }

  @Test
  public void createWithSystem_setsSingletonCache_onGivenSystem_ifSingletonCacheIsOpen_andExistingIsOk_andCacheIsCompatible() {
    var givenSystem = systemWithNoCache();
    var singletonCache = cache("singleton", OPEN);

    var internalCacheBuilder = new InternalCacheBuilder(
        new Properties(), new CacheConfig(), metricsServiceBuilder, THROWING_SYSTEM_SUPPLIER,
        THROWING_SYSTEM_CONSTRUCTOR,
        supplierOf(singletonCache), THROWING_CACHE_CONSTRUCTOR);

    internalCacheBuilder
        .setIsExistingOk(true)
        .create(givenSystem);

    verify(givenSystem).setCache(same(singletonCache));
  }

  private static void ignoreThrowable(ThrowingCallable shouldRaiseThrowable) {
    try {
      shouldRaiseThrowable.call();
    } catch (Throwable ignored) {
    }
  }
}
