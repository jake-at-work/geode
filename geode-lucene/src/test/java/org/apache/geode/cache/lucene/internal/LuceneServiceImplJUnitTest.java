/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
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
package org.apache.geode.cache.lucene.internal;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.lucene.analysis.Analyzer;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;

import org.apache.geode.Statistics;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.EvictionAlgorithm;
import org.apache.geode.cache.EvictionAttributes;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.asyncqueue.internal.AsyncEventQueueFactoryImpl;
import org.apache.geode.cache.lucene.LuceneSerializer;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.OperationExecutors;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.PartitionedRegionDataStore;
import org.apache.geode.test.junit.categories.LuceneTest;

@Category({LuceneTest.class})
public class LuceneServiceImplJUnitTest {
  @Rule
  public ExpectedException thrown = ExpectedException.none();

  PartitionedRegion region;
  GemFireCacheImpl cache;
  LuceneServiceImpl service = new LuceneServiceImpl();

  @Before
  public void createMocks() throws NoSuchFieldException, SecurityException,
      IllegalArgumentException, IllegalAccessException {
    region = mock(PartitionedRegion.class);
    cache = mock(GemFireCacheImpl.class);
    var f = LuceneServiceImpl.class.getDeclaredField("cache");
    f.setAccessible(true);
    f.set(service, cache);
  }

  @Test
  public void shouldPassSerializer() {
    service = Mockito.spy(service);
    var factory = service.createIndexFactory();
    var serializer = mock(LuceneSerializer.class);
    factory.setLuceneSerializer(serializer);
    factory.setFields("field1", "field2");
    factory.create("index", "region");
    Mockito.verify(service).createIndex(eq("index"), eq("region"), any(), eq(serializer),
        eq(false));
  }

  @Test
  public void shouldThrowIllegalArgumentExceptionIfFieldsAreMissing() {
    thrown.expect(IllegalArgumentException.class);
    service.createIndexFactory().create("index", "region");
  }

  @Test
  public void shouldThrowIllegalArgumentExceptionIfFieldsMapIsMissing() {
    thrown.expect(IllegalArgumentException.class);
    service.createIndex("index", "region", Collections.emptyMap(), null, false);
  }

  @Test
  public void shouldReturnFalseIfRegionNotFoundInWaitUntilFlush() throws InterruptedException {
    var result =
        service.waitUntilFlushed("dummyIndex", "dummyRegion", 60000, TimeUnit.MILLISECONDS);
    assertFalse(result);
  }

  @Test
  public void userRegionShouldNotBeSetBeforeIndexInitialized() throws Exception {
    var testService = new TestLuceneServiceImpl();
    var f = LuceneServiceImpl.class.getDeclaredField("cache");
    f.setAccessible(true);
    f.set(testService, cache);
    var aeqFactory = mock(AsyncEventQueueFactoryImpl.class);
    when(cache.createAsyncEventQueueFactory()).thenReturn(aeqFactory);

    var ds = mock(DistributedSystem.class);
    var luceneIndexStats = mock(Statistics.class);
    when(cache.getDistributedSystem()).thenReturn(ds);
    when(ds.createAtomicStatistics(any(), anyString()))
        .thenReturn(luceneIndexStats);
    when(cache.getRegion(anyString())).thenReturn(region);
    var manager = mock(DistributionManager.class);
    when(cache.getDistributionManager()).thenReturn(manager);
    var executors = mock(OperationExecutors.class);
    when(executors.getWaitingThreadPool()).thenReturn(Executors.newSingleThreadExecutor());
    when(manager.getExecutors()).thenReturn(executors);

    var ratts = mock(RegionAttributes.class);
    when(region.getAttributes()).thenReturn(ratts);
    when(ratts.getDataPolicy()).thenReturn(DataPolicy.PARTITION);
    var evictionAttrs = mock(EvictionAttributes.class);
    when(ratts.getEvictionAttributes()).thenReturn(evictionAttrs);
    when(evictionAttrs.getAlgorithm()).thenReturn(EvictionAlgorithm.NONE);

    Map<String, Analyzer> fieldMap = new HashMap<>();
    fieldMap.put("field1", null);
    fieldMap.put("field2", null);
    testService.createIndex("index", "region", fieldMap, null, true);
  }

  @Test
  public void createLuceneIndexOnExistingRegionShouldNotThrowNPEIfBucketMovedDuringReindexing() {
    var index = mock(LuceneIndexImpl.class);
    var dataStore = mock(PartitionedRegionDataStore.class);
    when(region.getDataStore()).thenReturn(dataStore);
    Integer[] bucketIds = {1, 2, 3, 4, 5};
    Set<Integer> primaryBucketIds = new HashSet(Arrays.asList(bucketIds));
    when(dataStore.getAllLocalPrimaryBucketIds()).thenReturn(primaryBucketIds);
    when(dataStore.getLocalBucketById(3)).thenReturn(null);
    var result = service.createLuceneIndexOnDataRegion(region, index);
    assertTrue(result);
  }

  private class TestLuceneServiceImpl extends LuceneServiceImpl {

    @Override
    public void afterDataRegionCreated(InternalLuceneIndex index) {
      var userRegion =
          (PartitionedRegion) index.getCache().getRegion(index.getRegionPath());
      verify(userRegion, never()).addAsyncEventQueueId(anyString(), anyBoolean());
    }

    @Override
    protected void validateLuceneIndexProfile(PartitionedRegion region) {

    }

    @Override
    protected void validateAllMembersAreTheSameVersion(PartitionedRegion region) {

    }
  }
}
