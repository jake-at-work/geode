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
package org.apache.geode.internal.cache.wan.serial;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.CancelException;
import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.internal.cache.ForceReattemptException;
import org.apache.geode.internal.cache.execute.data.CustId;
import org.apache.geode.internal.cache.execute.data.Order;
import org.apache.geode.internal.cache.execute.data.OrderId;
import org.apache.geode.internal.cache.execute.data.Shipment;
import org.apache.geode.internal.cache.execute.data.ShipmentId;
import org.apache.geode.internal.cache.wan.WANTestBase;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.junit.categories.WanTest;

@Category({WanTest.class})
public class SerialWANPropagation_PartitionedRegionDUnitTest extends WANTestBase {

  private static final long serialVersionUID = 1L;

  public SerialWANPropagation_PartitionedRegionDUnitTest() {
    super();
  }

  @Test
  public void testPartitionedSerialPropagation() {

    var lnPort = vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    var nyPort = vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    createCacheInVMs(nyPort, vm2, vm3);
    createReceiverInVMs(vm2, vm3);

    createCacheInVMs(lnPort, vm4, vm5, vm6, vm7);

    vm4.invoke(() -> WANTestBase.createSender("ln", 2, false, 100, 10, false, false, null, true));
    vm5.invoke(() -> WANTestBase.createSender("ln", 2, false, 100, 10, false, false, null, true));

    vm4.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", "ln", 1, 100,
        isOffHeap()));
    vm5.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", "ln", 1, 100,
        isOffHeap()));
    vm6.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", "ln", 1, 100,
        isOffHeap()));
    vm7.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", "ln", 1, 100,
        isOffHeap()));

    vm4.invoke(() -> WANTestBase.startSender("ln"));
    // vm5.invoke(() -> WANTestBase.startSender( "ln" ));

    vm2.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", null, 1, 100,
        isOffHeap()));
    vm3.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", null, 1, 100,
        isOffHeap()));

    vm4.invoke(() -> WANTestBase.doPuts(getTestMethodName() + "_PR", 1000));

    vm2.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_PR", 1000));
  }

  @Test
  public void testPartitionedSerialPropagationWithTXWhenSendersNotConfiguredOnAllServers() {
    int lnPort = vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    int nyPort = vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    createCacheInVMs(nyPort, vm2, vm3);
    createReceiverInVMs(vm2, vm3);

    createCacheInVMs(lnPort, vm4, vm5, vm6, vm7);

    vm4.invoke(() -> WANTestBase.createSender("ln", 2, false, 100, 10, false, false, null, true));
    vm5.invoke(() -> WANTestBase.createSender("ln", 2, false, 100, 10, false, false, null, true));

    vm4.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", "ln", 1, 100,
        isOffHeap()));
    vm5.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", "ln", 1, 100,
        isOffHeap()));
    vm6.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", "ln", 1, 100,
        isOffHeap()));
    vm7.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", "ln", 1, 100,
        isOffHeap()));

    vm4.invoke(() -> WANTestBase.startSender("ln"));

    vm2.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", null, 1, 100,
        isOffHeap()));
    vm3.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", null, 1, 100,
        isOffHeap()));

    vm7.invoke(() -> WANTestBase.doTxPuts(getTestMethodName() + "_PR", 1000));

    vm2.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_PR", 1000));
  }

  @Test
  public void testBothReplicatedAndPartitionedSerialPropagation() {

    var lnPort = vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    var nyPort = vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    createCacheInVMs(nyPort, vm2, vm3);
    createReceiverInVMs(vm2, vm3);

    createCacheInVMs(lnPort, vm4, vm5, vm6, vm7);

    vm4.invoke(() -> WANTestBase.createSender("ln", 2, false, 100, 10, false, false, null, true));
    vm5.invoke(() -> WANTestBase.createSender("ln", 2, false, 100, 10, false, false, null, true));

    vm2.invoke(
        () -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", null, isOffHeap()));
    vm3.invoke(
        () -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", null, isOffHeap()));

    vm2.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", null, 1, 100,
        isOffHeap()));
    vm3.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", null, 1, 100,
        isOffHeap()));

    startSenderInVMs("ln", vm4, vm5);

    vm4.invoke(
        () -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", "ln", isOffHeap()));
    vm5.invoke(
        () -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", "ln", isOffHeap()));
    vm6.invoke(
        () -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", "ln", isOffHeap()));
    vm7.invoke(
        () -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", "ln", isOffHeap()));

    vm4.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", "ln", 1, 100,
        isOffHeap()));
    vm5.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", "ln", 1, 100,
        isOffHeap()));
    vm6.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", "ln", 1, 100,
        isOffHeap()));
    vm7.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", "ln", 1, 100,
        isOffHeap()));

    vm4.invoke(() -> WANTestBase.doPuts(getTestMethodName() + "_RR", 1000));
    vm5.invoke(() -> WANTestBase.doPuts(getTestMethodName() + "_PR", 1000));

    vm2.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_RR", 1000));
    vm3.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_PR", 1000));
  }

  @Test
  public void testSerialReplicatedAndPartitionedPropagation() {

    var lnPort = vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    var nyPort = vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    createCacheInVMs(nyPort, vm2, vm3);
    createReceiverInVMs(vm2, vm3);

    createCacheInVMs(lnPort, vm4, vm5, vm6, vm7);

    vm4.invoke(
        () -> WANTestBase.createSender("lnSerial", 2, false, 100, 10, false, false, null, true));
    vm5.invoke(
        () -> WANTestBase.createSender("lnSerial", 2, false, 100, 10, false, false, null, true));

    vm2.invoke(
        () -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", null, isOffHeap()));
    vm3.invoke(
        () -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", null, isOffHeap()));

    vm2.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", null, 1, 100,
        isOffHeap()));
    vm3.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", null, 1, 100,
        isOffHeap()));

    vm4.invoke(() -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", "lnSerial",
        isOffHeap()));
    vm5.invoke(() -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", "lnSerial",
        isOffHeap()));
    vm6.invoke(() -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", "lnSerial",
        isOffHeap()));
    vm7.invoke(() -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", "lnSerial",
        isOffHeap()));

    vm4.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", "lnSerial", 1,
        100, isOffHeap()));
    vm5.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", "lnSerial", 1,
        100, isOffHeap()));
    vm6.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", "lnSerial", 1,
        100, isOffHeap()));
    vm7.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", "lnSerial", 1,
        100, isOffHeap()));

    vm4.invoke(() -> WANTestBase.startSender("lnSerial"));
    vm5.invoke(() -> WANTestBase.startSender("lnSerial"));

    vm4.invoke(() -> WANTestBase.doPuts(getTestMethodName() + "_RR", 1000));
    vm5.invoke(() -> WANTestBase.doPuts(getTestMethodName() + "_PR", 1000));

    vm2.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_RR", 1000));
    vm3.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_PR", 1000));
  }

  @Test
  public void testSerialReplicatedAndSerialPartitionedPropagation() {

    var lnPort = vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    var nyPort = vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    createCacheInVMs(nyPort, vm2, vm3);
    createReceiverInVMs(vm2, vm3);

    createCacheInVMs(lnPort, vm4, vm5, vm6, vm7);

    vm4.invoke(
        () -> WANTestBase.createSender("lnSerial1", 2, false, 100, 10, false, false, null, true));
    vm5.invoke(
        () -> WANTestBase.createSender("lnSerial1", 2, false, 100, 10, false, false, null, true));

    vm5.invoke(
        () -> WANTestBase.createSender("lnSerial2", 2, false, 100, 10, false, false, null, true));
    vm6.invoke(
        () -> WANTestBase.createSender("lnSerial2", 2, false, 100, 10, false, false, null, true));

    vm2.invoke(
        () -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", null, isOffHeap()));
    vm3.invoke(
        () -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", null, isOffHeap()));

    vm2.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", null, 1, 100,
        isOffHeap()));
    vm3.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", null, 1, 100,
        isOffHeap()));

    vm4.invoke(() -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", "lnSerial1",
        isOffHeap()));
    vm5.invoke(() -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", "lnSerial1",
        isOffHeap()));
    vm6.invoke(() -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", "lnSerial1",
        isOffHeap()));
    vm7.invoke(() -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", "lnSerial1",
        isOffHeap()));

    vm4.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", "lnSerial2",
        1, 100, isOffHeap()));
    vm5.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", "lnSerial2",
        1, 100, isOffHeap()));
    vm6.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", "lnSerial2",
        1, 100, isOffHeap()));
    vm7.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", "lnSerial2",
        1, 100, isOffHeap()));

    startSenderInVMs("lnSerial1", vm4, vm5);

    startSenderInVMs("lnSerial2", vm5, vm6);

    vm4.invoke(() -> WANTestBase.doPuts(getTestMethodName() + "_RR", 1000));
    vm5.invoke(() -> WANTestBase.doPuts(getTestMethodName() + "_PR", 1000));

    vm2.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_RR", 1000));
    vm3.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_PR", 1000));
  }

  @Test
  public void testPartitionedSerialPropagationToTwoWanSites() {

    var lnPort = createFirstLocatorWithDSId(1);
    var nyPort = vm0.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));
    var tkPort = vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(3, lnPort));

    createCacheInVMs(nyPort, vm2);
    vm2.invoke(WANTestBase::createReceiver);
    createCacheInVMs(tkPort, vm3);
    vm3.invoke(WANTestBase::createReceiver);

    createCacheInVMs(lnPort, vm4, vm5, vm6, vm7);

    vm4.invoke(
        () -> WANTestBase.createSender("lnSerial1", 2, false, 100, 10, false, false, null, true));
    vm5.invoke(
        () -> WANTestBase.createSender("lnSerial1", 2, false, 100, 10, false, false, null, true));

    vm4.invoke(
        () -> WANTestBase.createSender("lnSerial2", 3, false, 100, 10, false, false, null, true));
    vm5.invoke(
        () -> WANTestBase.createSender("lnSerial2", 3, false, 100, 10, false, false, null, true));

    vm2.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", null, 1, 100,
        isOffHeap()));
    vm3.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", null, 1, 100,
        isOffHeap()));

    startSenderInVMs("lnSerial1", vm4, vm5);
    startSenderInVMs("lnSerial2", vm4, vm5);

    vm4.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR",
        "lnSerial1,lnSerial2", 1, 100, isOffHeap()));
    vm5.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR",
        "lnSerial1,lnSerial2", 1, 100, isOffHeap()));
    vm6.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR",
        "lnSerial1,lnSerial2", 1, 100, isOffHeap()));
    vm7.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR",
        "lnSerial1,lnSerial2", 1, 100, isOffHeap()));

    vm4.invoke(() -> WANTestBase.doPuts(getTestMethodName() + "_PR", 1000));

    vm2.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_PR", 1000));
    vm3.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_PR", 1000));
  }

  @Test
  public void testPartitionedSerialPropagationHA() throws Exception {
    IgnoredException.addIgnoredException("Broken pipe");
    IgnoredException.addIgnoredException("Connection reset");
    IgnoredException.addIgnoredException("Unexpected IOException");

    var lnPort = vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    var nyPort = vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    createCacheInVMs(nyPort, vm2, vm3);
    createReceiverInVMs(vm2, vm3);

    createCacheInVMs(lnPort, vm4, vm5, vm6, vm7);

    vm4.invoke(() -> WANTestBase.createSender("ln", 2, false, 100, 10, false, false, null, true));
    vm5.invoke(() -> WANTestBase.createSender("ln", 2, false, 100, 10, false, false, null, true));

    vm4.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", "ln", 1, 100,
        isOffHeap()));
    vm5.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", "ln", 1, 100,
        isOffHeap()));
    vm6.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", "ln", 1, 100,
        isOffHeap()));
    vm7.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", "ln", 1, 100,
        isOffHeap()));

    startSenderInVMs("ln", vm4, vm5);

    vm2.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", null, 1, 100,
        isOffHeap()));
    vm3.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", null, 1, 100,
        isOffHeap()));

    // do initial 100 puts to create all the buckets
    vm5.invoke(() -> WANTestBase.doPuts(getTestMethodName() + "_PR", 100));

    IgnoredException.addIgnoredException(CancelException.class.getName());
    IgnoredException.addIgnoredException(CacheClosedException.class.getName());
    IgnoredException.addIgnoredException(ForceReattemptException.class.getName());
    // start async puts
    AsyncInvocation<Void> inv =
        vm5.invokeAsync(() -> WANTestBase.doPuts(getTestMethodName() + "_PR", 1000));
    // close the cache on vm4 in between the puts
    vm4.invoke(() -> WANTestBase.killSender());

    inv.await();
    vm2.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_PR", 1000));
    vm3.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_PR", 1000));
    vm4.invoke(() -> WANTestBase.checkConflatedStats("ln", 0));
    vm5.invoke(() -> WANTestBase.checkConflatedStats("ln", 0));
  }

  @Test
  public void testPartitionedSerialPropagationWithParallelThreads() {

    var lnPort = vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    var nyPort = vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    createCacheInVMs(nyPort, vm2, vm3);
    createCacheInVMs(lnPort, vm4, vm5, vm6, vm7);

    vm4.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", "ln", 1, 100,
        isOffHeap()));
    vm5.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", "ln", 1, 100,
        isOffHeap()));
    vm6.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", "ln", 1, 100,
        isOffHeap()));
    vm7.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", "ln", 1, 100,
        isOffHeap()));
    vm2.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", null, 1, 100,
        isOffHeap()));
    vm3.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", null, 1, 100,
        isOffHeap()));

    createReceiverInVMs(vm2, vm3);

    vm4.invoke(() -> WANTestBase.createSender("ln", 2, false, 100, 10, false, false, null, true));
    vm5.invoke(() -> WANTestBase.createSender("ln", 2, false, 100, 10, false, false, null, true));

    startSenderInVMs("ln", vm4, vm5);



    vm4.invoke(() -> WANTestBase.doMultiThreadedPuts( // TODO: eats exceptions
        getTestMethodName() + "_PR", 1000));

    vm2.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_PR", 1000));
    vm3.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_PR", 1000));
  }

  @Test
  public void testPartitionedSerialPropagationWithGroupTransactionEventsAndMixOfEventsInAndNotInTransactions()
      throws Exception {
    var lnPort = vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    var nyPort = vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    createCacheInVMs(nyPort, vm2, vm3);
    createReceiverInVMs(vm2, vm3);
    createCacheInVMs(lnPort, vm4, vm5, vm6, vm7);

    vm4.invoke(() -> setNumDispatcherThreadsForTheRun(1));
    vm5.invoke(() -> setNumDispatcherThreadsForTheRun(1));
    vm6.invoke(() -> setNumDispatcherThreadsForTheRun(1));
    vm7.invoke(() -> setNumDispatcherThreadsForTheRun(1));

    vm4.invoke(
        () -> WANTestBase.createSender("ln", 2, false, 100, 10, false, false, null, true, true));
    vm5.invoke(
        () -> WANTestBase.createSender("ln", 2, false, 100, 10, false, false, null, true, true));
    vm6.invoke(
        () -> WANTestBase.createSender("ln", 2, false, 100, 10, false, false, null, true, true));
    vm7.invoke(
        () -> WANTestBase.createSender("ln", 2, false, 100, 10, false, false, null, true, true));


    vm4.invoke(
        () -> WANTestBase.createCustomerOrderShipmentPartitionedRegion("ln", 2, 10,
            isOffHeap()));
    vm5.invoke(
        () -> WANTestBase.createCustomerOrderShipmentPartitionedRegion("ln", 2, 10,
            isOffHeap()));
    vm6.invoke(
        () -> WANTestBase.createCustomerOrderShipmentPartitionedRegion("ln", 2, 10,
            isOffHeap()));
    vm7.invoke(
        () -> WANTestBase.createCustomerOrderShipmentPartitionedRegion("ln", 2, 10,
            isOffHeap()));

    startSenderInVMs("ln", vm4, vm5, vm6, vm7);

    vm2.invoke(() -> createCustomerOrderShipmentPartitionedRegion(null, 1, 8, isOffHeap()));
    vm3.invoke(() -> createCustomerOrderShipmentPartitionedRegion(null, 1, 8, isOffHeap()));

    var customers = 4;

    var transactionsPerCustomer = 1000;
    final Map<Object, Object> keyValuesInTransactions = new HashMap<>();
    for (var custId = 0; custId < customers; custId++) {
      for (var i = 0; i < transactionsPerCustomer; i++) {
        var custIdObject = new CustId(custId);
        var orderId = new OrderId(i, custIdObject);
        var shipmentId1 = new ShipmentId(i, orderId);
        var shipmentId2 = new ShipmentId(i + 1, orderId);
        var shipmentId3 = new ShipmentId(i + 2, orderId);
        keyValuesInTransactions.put(orderId, new Order());
        keyValuesInTransactions.put(shipmentId1, new Shipment());
        keyValuesInTransactions.put(shipmentId2, new Shipment());
        keyValuesInTransactions.put(shipmentId3, new Shipment());
      }
    }

    var ordersPerCustomerNotInTransactions = 1000;

    final Map<Object, Object> keyValuesNotInTransactions = new HashMap<>();
    for (var custId = 0; custId < customers; custId++) {
      for (var i = 0; i < ordersPerCustomerNotInTransactions; i++) {
        var custIdObject = new CustId(custId);
        var orderId = new OrderId(i + transactionsPerCustomer * customers, custIdObject);
        keyValuesNotInTransactions.put(orderId, new Order());
      }
    }

    // eventsPerTransaction is 1 (orders) + 3 (shipments)
    var eventsPerTransaction = 4;
    AsyncInvocation<Void> inv1 =
        vm7.invokeAsync(
            () -> WANTestBase.doOrderAndShipmentPutsInsideTransactions(keyValuesInTransactions,
                eventsPerTransaction));

    AsyncInvocation<Void> inv2 =
        vm6.invokeAsync(
            () -> WANTestBase.putGivenKeyValue(orderRegionName, keyValuesNotInTransactions));

    inv1.await();
    inv2.await();

    var entries =
        ordersPerCustomerNotInTransactions * customers + transactionsPerCustomer * customers;

    vm4.invoke(() -> WANTestBase.validateRegionSize(orderRegionName, entries));
    vm5.invoke(() -> WANTestBase.validateRegionSize(orderRegionName, entries));
    vm6.invoke(() -> WANTestBase.validateRegionSize(orderRegionName, entries));
    vm7.invoke(() -> WANTestBase.validateRegionSize(orderRegionName, entries));

    vm2.invoke(() -> WANTestBase.validateRegionSize(orderRegionName, entries));
    vm3.invoke(() -> WANTestBase.validateRegionSize(orderRegionName, entries));

    vm4.invoke(() -> WANTestBase.checkConflatedStats("ln", 0));
    vm5.invoke(() -> WANTestBase.checkConflatedStats("ln", 0));
    vm6.invoke(() -> WANTestBase.checkConflatedStats("ln", 0));
    vm7.invoke(() -> WANTestBase.checkConflatedStats("ln", 0));

    vm7.invoke(() -> WANTestBase.validateParallelSenderQueueAllBucketsDrained("ln"));
    vm6.invoke(() -> WANTestBase.validateParallelSenderQueueAllBucketsDrained("ln"));
    vm5.invoke(() -> WANTestBase.validateParallelSenderQueueAllBucketsDrained("ln"));
    vm4.invoke(() -> WANTestBase.validateParallelSenderQueueAllBucketsDrained("ln"));
  }
}
