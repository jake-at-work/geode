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
package org.apache.geode.cache30;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.Socket;
import java.util.List;
import java.util.Properties;

import org.junit.Test;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Declarable;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.asyncqueue.AsyncEvent;
import org.apache.geode.cache.asyncqueue.AsyncEventListener;
import org.apache.geode.cache.asyncqueue.AsyncEventQueue;
import org.apache.geode.cache.asyncqueue.AsyncEventQueueFactory;
import org.apache.geode.cache.client.Pool;
import org.apache.geode.cache.client.PoolFactory;
import org.apache.geode.cache.client.PoolManager;
import org.apache.geode.cache.client.SocketFactory;
import org.apache.geode.internal.cache.xmlcache.CacheCreation;
import org.apache.geode.internal.cache.xmlcache.CacheXml;
import org.apache.geode.internal.cache.xmlcache.Declarable2;
import org.apache.geode.internal.cache.xmlcache.RegionAttributesCreation;


public class CacheXmlGeode10DUnitTest extends CacheXml81DUnitTest {

  @Override
  protected String getGemFireVersion() {
    return CacheXml.VERSION_1_0;
  }

  @SuppressWarnings("rawtypes")
  @Test
  public void testAsyncEventQueueIsForwardExpirationDestroyAttribute() throws Exception {
    final String regionName = testName.getMethodName();

    // Create AsyncEventQueue with Listener
    final CacheCreation cache = new CacheCreation();
    AsyncEventQueueFactory factory = cache.createAsyncEventQueueFactory();

    AsyncEventListener listener = new MyAsyncEventListenerGeode10();

    // Test for default forwardExpirationDestroy attribute value (which is false)
    String aeqId1 = "aeqWithDefaultFED";
    factory.create(aeqId1, listener);
    AsyncEventQueue aeq1 = cache.getAsyncEventQueue(aeqId1);
    assertFalse(aeq1.isForwardExpirationDestroy());

    // Test by setting forwardExpirationDestroy attribute value.
    String aeqId2 = "aeqWithFEDsetToTrue";
    factory.setForwardExpirationDestroy(true);
    factory.create(aeqId2, listener);

    AsyncEventQueue aeq2 = cache.getAsyncEventQueue(aeqId2);
    assertTrue(aeq2.isForwardExpirationDestroy());

    // Create region and set the AsyncEventQueue
    final RegionAttributesCreation attrs = new RegionAttributesCreation(cache);
    attrs.addAsyncEventQueueId(aeqId2);

    final Region regionBefore = cache.createRegion(regionName, attrs);
    assertNotNull(regionBefore);
    assertTrue(regionBefore.getAttributes().getAsyncEventQueueIds().size() == 1);

    testXml(cache);

    final Cache c = getCache();
    assertNotNull(c);

    aeq1 = c.getAsyncEventQueue(aeqId1);
    assertFalse(aeq1.isForwardExpirationDestroy());

    aeq2 = c.getAsyncEventQueue(aeqId2);
    assertTrue(aeq2.isForwardExpirationDestroy());

    final Region regionAfter = c.getRegion(regionName);
    assertNotNull(regionAfter);
    assertTrue(regionAfter.getAttributes().getAsyncEventQueueIds().size() == 1);

    regionAfter.localDestroyRegion();

    // Clear AsyncEventQueues.
    c.close();
  }

  @Test
  public void testPoolSocketFactory() throws IOException {
    getSystem();
    CacheCreation cache = new CacheCreation();
    PoolFactory f = cache.createPoolFactory();
    f.setSocketFactory(new TestSocketFactory());
    f.addServer("localhost", 443);
    f.create("mypool");

    testXml(cache);
    Pool cp = PoolManager.find("mypool");
    assertThat(cp.getSocketFactory()).isInstanceOf(TestSocketFactory.class);
  }

  public static class MyAsyncEventListenerGeode10 implements AsyncEventListener, Declarable {

    @Override
    public boolean processEvents(List<AsyncEvent> events) {
      return true;
    }

    @Override
    public void close() {}

    @Override
    public void init(Properties properties) {}
  }

  public static class TestSocketFactory implements SocketFactory, Declarable2 {
    @Override
    public Socket createSocket() throws IOException {
      return new Socket();
    }

    @Override
    public Properties getConfig() {
      return new Properties();
    }

    @Override
    public void initialize(Cache cache, Properties properties) {

    }
  }
}
