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

package org.apache.geode.management;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.geode.cache.Cache;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.management.internal.SystemManagementService;
import org.apache.geode.test.awaitility.GeodeAwaitility;

public class MXBeanAwaitility {

  public static LocatorMXBean awaitLocalLocatorMXBean() {
    var service = getSystemManagementService();

    GeodeAwaitility.await()
        .untilAsserted(() -> assertThat(service.getLocalLocatorMXBean()).isNotNull());

    return service.getLocalLocatorMXBean();
  }

  public static LocatorMXBean awaitLocatorMXBeanProxy(final DistributedMember member) {
    var service = getSystemManagementService();
    var objectName = service.getLocatorMBeanName(member);

    var alias = "Awaiting LocatorMXBean proxy for " + member;
    GeodeAwaitility.await(alias).untilAsserted(
        () -> assertThat(service.getMBeanProxy(objectName, LocatorMXBean.class)).isNotNull());

    return service.getMBeanProxy(objectName, LocatorMXBean.class);
  }

  public static GatewaySenderMXBean awaitGatewaySenderMXBeanProxy(final DistributedMember member,
      final String senderId) {
    var service = getSystemManagementService();
    var objectName = service.getGatewaySenderMBeanName(member, senderId);

    var alias = "Awaiting GatewaySenderMXBean proxy for " + member;
    GeodeAwaitility.await(alias).untilAsserted(
        () -> assertThat(service.getMBeanProxy(objectName, GatewaySenderMXBean.class)).isNotNull());

    return service.getMBeanProxy(objectName, GatewaySenderMXBean.class);
  }

  public static GatewayReceiverMXBean awaitGatewayReceiverMXBeanProxy(
      final DistributedMember member) {
    var service = getSystemManagementService();
    var objectName = service.getGatewayReceiverMBeanName(member);

    var alias = "Awaiting GatewayReceiverMXBean proxy for " + member;
    GeodeAwaitility.await(alias)
        .untilAsserted(
            () -> assertThat(service.getMBeanProxy(objectName, GatewayReceiverMXBean.class))
                .isNotNull());

    return service.getMBeanProxy(objectName, GatewayReceiverMXBean.class);
  }

  public static SystemManagementService getSystemManagementService() {
    Cache cache = GemFireCacheImpl.getInstance();
    return (SystemManagementService) ManagementService.getExistingManagementService(cache);
  }

  public static MemberMXBean awaitMemberMXBeanProxy(final DistributedMember member) {
    return awaitMemberMXBeanProxy(member, getSystemManagementService());
  }

  public static MemberMXBean awaitMemberMXBeanProxy(final DistributedMember member,
      final SystemManagementService managementService) {
    var objectName = managementService.getMemberMBeanName(member);

    var alias = "Awaiting MemberMXBean proxy for " + member;
    GeodeAwaitility.await(alias)
        .untilAsserted(
            () -> assertThat(managementService.getMBeanProxy(objectName, MemberMXBean.class))
                .isNotNull());

    return managementService.getMBeanProxy(objectName, MemberMXBean.class);
  }
}
