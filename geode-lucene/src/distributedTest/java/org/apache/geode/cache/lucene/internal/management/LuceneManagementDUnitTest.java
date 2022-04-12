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
package org.apache.geode.cache.lucene.internal.management;

import static org.apache.geode.cache.lucene.test.LuceneTestUtilities.INDEX_NAME;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Set;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.DataSerializable;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.lucene.LuceneQuery;
import org.apache.geode.cache.lucene.LuceneQueryException;
import org.apache.geode.cache.lucene.LuceneServiceProvider;
import org.apache.geode.cache.lucene.management.LuceneServiceMXBean;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.management.ManagementTestBase;
import org.apache.geode.management.MemberMXBean;
import org.apache.geode.management.internal.MBeanJMXAdapter;
import org.apache.geode.management.internal.SystemManagementService;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.junit.categories.LuceneTest;

@Category({LuceneTest.class})
public class LuceneManagementDUnitTest extends ManagementTestBase {

  @Test
  public void testMBeanAndProxiesCreated() throws Exception {
    initManagement(true);

    // Verify MBean is created in each managed node
    for (var vm : getManagedNodeList()) {
      vm.invoke(this::verifyMBean);
    }

    // Verify MBean proxies are created in the managing node
    getManagingNode().invoke(() -> verifyMBeanProxies(getCache()));
  }

  @Test
  public void testMBeanIndexMetricsCreatedOneRegion() throws Exception {
    initManagement(true);
    var regionName = getName();
    var numIndexes = 5;
    for (var vm : getManagedNodeList()) {
      // Create indexes and region
      createIndexesAndRegion(regionName, numIndexes, vm);

      // Verify index metrics
      vm.invoke(() -> verifyAllMBeanIndexMetrics(regionName, numIndexes, numIndexes));
    }

    // Verify MBean proxies are created in the managing node
    getManagingNode().invoke(
        () -> verifyAllMBeanProxyIndexMetrics(regionName, numIndexes, numIndexes, getCache()));
  }

  @Test
  public void testMBeanIndexMetricsCreatedTwoRegions() throws Exception {
    initManagement(true);
    var baseRegionName = getName();
    var numIndexes = 5;
    var numRegions = 2;
    for (var vm : getManagedNodeList()) {
      // Create indexes and regions the managed VM
      for (var i = 0; i < numRegions; i++) {
        var regionName = baseRegionName + i;
        createIndexesAndRegion(regionName, numIndexes, vm);
      }

      // Verify index metrics in the managed VM
      for (var i = 0; i < numRegions; i++) {
        var regionName = baseRegionName + i;
        vm.invoke(
            () -> verifyAllMBeanIndexMetrics(regionName, numIndexes, numIndexes * numRegions));
      }
    }

    // Verify index metrics in the managing node
    for (var i = 0; i < numRegions; i++) {
      var regionName = baseRegionName + i;
      getManagingNode().invoke(() -> verifyAllMBeanProxyIndexMetrics(regionName, numIndexes,
          numIndexes * numRegions, getCache()));
    }
  }

  @Test
  public void testMBeanIndexMetricsValues() throws Exception {
    initManagement(true);
    var regionName = getName();
    var numIndexes = 1;
    for (var vm : getManagedNodeList()) {
      // Create indexes and region
      createIndexesAndRegion(regionName, numIndexes, vm);
    }

    // Put objects with field0
    var numPuts = 10;
    getManagedNodeList().get(0).invoke(() -> putEntries(regionName, numPuts));

    // Query objects with field0
    var indexName = INDEX_NAME + "_" + 0;
    getManagedNodeList().get(0).invoke(() -> queryEntries(regionName, indexName));

    // Wait for the managed members to be updated a few times in the manager node
    getManagingNode().invoke(() -> waitForMemberProxiesToRefresh(2, getCache()));

    // Verify index metrics
    var numManagedNodes = getManagedNodeList().size();
    getManagingNode().invoke(() -> verifyMBeanIndexMetricsValues(regionName, indexName, numPuts,
        numManagedNodes/* 1 query per managed node */, 1/* 1 result */, getCache()));
  }

  private static void waitForMemberProxiesToRefresh(int refreshCount, final InternalCache cache) {
    Set<? extends DistributedMember> members =
        cache.getDistributionManager().getOtherNormalDistributionManagerIds();
    // Currently, the LuceneServiceMBean is not updated in the manager since it has no getters,
    // so use the MemberMBean instead.
    for (DistributedMember member : members) {
      var memberMBeanName = getManagementService().getMemberMBeanName(member);
      // Wait for the MemberMBean proxy to be created
      waitForProxy(memberMBeanName, MemberMXBean.class);
      // Wait for the MemberMBean proxy to be updated refreshCount times.
      waitForRefresh(refreshCount, memberMBeanName);
    }
  }

  private void verifyMBean() {
    getMBean();
  }

  private LuceneServiceMXBean getMBean() {
    var objectName = MBeanJMXAdapter
        .getCacheServiceMBeanName(getSystem().getDistributedMember(), "LuceneService");
    assertNotNull(getManagementService().getMBeanInstance(objectName, LuceneServiceMXBean.class));
    return getManagementService().getMBeanInstance(objectName, LuceneServiceMXBean.class);
  }

  private static void verifyMBeanProxies(final InternalCache cache) {
    Set<? extends DistributedMember> members =
        cache.getDistributionManager().getOtherNormalDistributionManagerIds();
    for (DistributedMember member : members) {
      getMBeanProxy(member);
    }
  }

  private static LuceneServiceMXBean getMBeanProxy(DistributedMember member) {
    var service = (SystemManagementService) getManagementService();
    var objectName = MBeanJMXAdapter.getCacheServiceMBeanName(member, "LuceneService");
    await()
        .untilAsserted(
            () -> assertNotNull(service.getMBeanProxy(objectName, LuceneServiceMXBean.class)));
    return service.getMBeanProxy(objectName, LuceneServiceMXBean.class);
  }

  private void createIndexesAndRegion(String regionName, int numIndexes, VM vm) {
    // Create indexes
    vm.invoke(() -> createIndexes(regionName, numIndexes));

    // Create region
    createPartitionRegion(vm, regionName);
  }

  private void createIndexes(String regionName, int numIndexes) {
    var luceneService = LuceneServiceProvider.get(getCache());
    for (var i = 0; i < numIndexes; i++) {
      luceneService.createIndexFactory().setFields("field" + i).create(INDEX_NAME + "_" + i,
          regionName);
    }
  }

  private void verifyAllMBeanIndexMetrics(String regionName, int numRegionIndexes,
      int numTotalIndexes) {
    var mbean = getMBean();
    verifyMBeanIndexMetrics(mbean, regionName, numRegionIndexes, numTotalIndexes);
  }

  private static void verifyAllMBeanProxyIndexMetrics(String regionName, int numRegionIndexes,
      int numTotalIndexes, final InternalCache cache) {
    Set<? extends DistributedMember> members =
        cache.getDistributionManager().getOtherNormalDistributionManagerIds();
    for (DistributedMember member : members) {
      var mbean = getMBeanProxy(member);
      verifyMBeanIndexMetrics(mbean, regionName, numRegionIndexes, numTotalIndexes);
    }
  }

  private static void verifyMBeanIndexMetrics(LuceneServiceMXBean mbean, String regionName,
      int numRegionIndexes, int numTotalIndexes) {
    assertEquals(numTotalIndexes, mbean.listIndexMetrics().length);
    assertEquals(numRegionIndexes, mbean.listIndexMetrics(regionName).length);
    for (var i = 0; i < numRegionIndexes; i++) {
      assertNotNull(mbean.listIndexMetrics(regionName, INDEX_NAME + "_" + i));
    }
  }

  private void putEntries(String regionName, int numEntries) {
    for (var i = 0; i < numEntries; i++) {
      Region region = getCache().getRegion(regionName);
      var key = String.valueOf(i);
      Object value = new TestObject(key);
      region.put(key, value);
    }
  }

  private void queryEntries(String regionName, String indexName) throws LuceneQueryException {
    var service = LuceneServiceProvider.get(getCache());
    LuceneQuery query =
        service.createLuceneQueryFactory().create(indexName, regionName, "field0:0", null);
    query.findValues();
  }

  private void verifyMBeanIndexMetricsValues(String regionName, String indexName, int expectedPuts,
      int expectedQueries, int expectedHits, final InternalCache cache) {
    // Get index metrics from all members
    Set<? extends DistributedMember> members =
        cache.getDistributionManager().getOtherNormalDistributionManagerIds();
    int totalCommits = 0, totalUpdates = 0, totalDocuments = 0, totalQueries = 0, totalHits = 0;
    for (DistributedMember member : members) {
      var mbean = getMBeanProxy(member);
      var metrics = mbean.listIndexMetrics(regionName, indexName);
      assertNotNull(metrics);
      totalCommits += metrics.getCommits();
      totalUpdates += metrics.getUpdates();
      totalDocuments += metrics.getDocuments();
      totalQueries += metrics.getQueryExecutions();
      totalHits += metrics.getQueryExecutionTotalHits();
    }

    // Verify index metrics counts
    assertThat("totalCommits", totalCommits, greaterThanOrEqualTo(expectedPuts));
    assertThat("totalUpdates", totalUpdates, greaterThanOrEqualTo(expectedPuts));
    assertEquals(true, expectedPuts <= totalDocuments);
    assertEquals(expectedQueries, totalQueries);
    assertEquals(expectedHits, totalHits);
  }

  protected static class TestObject implements DataSerializable {
    private static final long serialVersionUID = 1L;
    private String field0;

    public TestObject(String value) {
      field0 = value;
    }

    public TestObject() {}

    public String toString() {
      return getClass().getSimpleName() + "[" + "field0="
          + field0 + "]";
    }

    @Override
    public void toData(DataOutput out) throws IOException {
      out.writeUTF(field0);
    }

    @Override
    public void fromData(DataInput in) throws IOException, ClassNotFoundException {
      field0 = in.readUTF();
    }
  }
}
