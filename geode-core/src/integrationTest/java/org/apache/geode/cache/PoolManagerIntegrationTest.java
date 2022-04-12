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
package org.apache.geode.cache;

import static org.apache.geode.cache.client.ClientRegionShortcut.PROXY;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.PoolManager;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.internal.cache.PoolFactoryImpl;
import org.apache.geode.internal.cache.PoolManagerImpl;
import org.apache.geode.test.junit.categories.ClientServerTest;

/**
 * Tests PoolManager
 *
 * @since GemFire 5.7
 */
@Category(ClientServerTest.class)
public class PoolManagerIntegrationTest {
  private DistributedSystem ds;

  @Before
  public void setUp() {
    var props = new Properties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "");
    ds = DistributedSystem.connect(props);
    assertThat(PoolManager.getAll().size()).isEqualTo(0);
  }

  @After
  public void tearDown() {
    PoolManager.close();
    ds.disconnect();
  }

  @Test
  public void testCreateFactory() {
    assertThat(PoolManager.createFactory()).isNotNull();
    assertThat(PoolManager.getAll().size()).isEqualTo(0);
  }

  @Test
  public void testGetMap() {
    assertThat(PoolManager.getAll().size()).isEqualTo(0);
    {
      var cpf = PoolManager.createFactory();
      ((PoolFactoryImpl) cpf).setStartDisabled(true);
      cpf.addLocator("localhost", 12345).create("mypool");
    }

    assertThat(PoolManager.getAll().size()).isEqualTo(1);
    {
      var cpf = PoolManager.createFactory();
      ((PoolFactoryImpl) cpf).setStartDisabled(true);
      cpf.addLocator("localhost", 12345).create("mypool2");
    }

    assertThat(PoolManager.getAll().size()).isEqualTo(2);
    assertThat(PoolManager.getAll().get("mypool")).isNotNull();
    assertThat(PoolManager.getAll().get("mypool2")).isNotNull();
    assertThat((PoolManager.getAll().get("mypool")).getName()).isEqualTo("mypool");
    assertThat((PoolManager.getAll().get("mypool2")).getName()).isEqualTo("mypool2");
  }

  @Test
  public void testFind() {
    {
      var cpf = PoolManager.createFactory();
      ((PoolFactoryImpl) cpf).setStartDisabled(true);
      cpf.addLocator("localhost", 12345).create("mypool");
    }
    assertThat(PoolManager.find("mypool")).isNotNull();
    assertThat((PoolManager.find("mypool")).getName()).isEqualTo("mypool");
    assertThat(PoolManager.find("bogus")).isNull();
  }

  @Test
  public void testRegionFind() {
    var cpf = PoolManager.createFactory();
    ((PoolFactoryImpl) cpf).setStartDisabled(true);
    var pool = cpf.addLocator("localhost", 12345).create("mypool");
    var cache = CacheFactory.create(ds);
    var fact = new AttributesFactory<Object, Object>();
    fact.setPoolName(pool.getName());
    Region region = cache.createRegion("myRegion", fact.create());
    assertThat(PoolManager.find(region)).isEqualTo(pool);
  }

  @Test
  public void testClose() {
    PoolManager.close();
    assertThat(PoolManager.getAll().size()).isEqualTo(0);
    {
      var cpf = PoolManager.createFactory();
      ((PoolFactoryImpl) cpf).setStartDisabled(true);
      cpf.addLocator("localhost", 12345).create("mypool");
    }

    assertThat(PoolManager.getAll().size()).isEqualTo(1);
    PoolManager.close();
    assertThat(PoolManager.getAll().size()).isEqualTo(0);
    {
      var cpf = PoolManager.createFactory();
      ((PoolFactoryImpl) cpf).setStartDisabled(true);
      cpf.addLocator("localhost", 12345).create("mypool");
    }

    assertThat(PoolManager.getAll().size()).isEqualTo(1);
    PoolManager.find("mypool").destroy();
    assertThat(PoolManager.find("mypool")).isNull();
    assertThat(PoolManager.getAll().size()).isEqualTo(0);
    PoolManager.close();
    assertThat(PoolManager.getAll().size()).isEqualTo(0);
  }

  @Test
  public void unregisterShouldThrowExceptionWhenThePoolHasRegionsStillAssociated() {
    PoolManager.createFactory().addLocator("localhost", 12345).create("poolOne");
    var clientCache = new ClientCacheFactory().create();
    assertThat(
        clientCache.createClientRegionFactory(PROXY).setPoolName("poolOne").create("regionOne"))
            .isNotNull();
    assertThatThrownBy(() -> PoolManagerImpl.getPMI().unregister(PoolManager.find("poolOne")))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Pool could not be destroyed because it is still in use by 1 regions");
  }

  @Test
  public void unregisterShouldCompleteSuccessfullyWhenThePoolDoesNotHaveRegionsAssociated() {
    PoolManager.createFactory().addLocator("localhost", 12345).create("poolOne");
    var clientCache = new ClientCacheFactory().create();
    assertThat(
        clientCache.createClientRegionFactory(PROXY).setPoolName("poolOne").create("regionOne"))
            .isNotNull();

    var poolOne = PoolManager.find("poolOne");
    clientCache.getRegion("regionOne").localDestroyRegion();
    assertThatCode(() -> PoolManagerImpl.getPMI().unregister(poolOne)).doesNotThrowAnyException();
    assertThat(PoolManager.find("poolOne")).isNull();
  }
}
