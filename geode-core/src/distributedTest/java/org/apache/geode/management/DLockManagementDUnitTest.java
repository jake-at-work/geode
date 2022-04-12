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

import static org.apache.geode.internal.process.ProcessUtils.identifyPid;
import static org.apache.geode.management.internal.MBeanJMXAdapter.getDistributedLockServiceName;
import static org.apache.geode.management.internal.MBeanJMXAdapter.getLockServiceMBeanName;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.Serializable;

import javax.management.ObjectName;

import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.distributed.DistributedLockService;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.locks.DLockService;
import org.apache.geode.management.internal.SystemManagementService;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.VM;


@SuppressWarnings({"serial", "unused"})
public class DLockManagementDUnitTest implements Serializable {

  private static final int MAX_WAIT_MILLIS = 2 * 60 * 1000; // 2 MINUTES

  private static final String LOCK_SERVICE_NAME =
      DLockManagementDUnitTest.class.getSimpleName() + "_testLockService";

  @Member
  private VM[] memberVMs;

  @Manager
  private VM managerVM;

  @Rule
  public ManagementTestRule managementTestRule =
      ManagementTestRule.builder().defineManagersFirst(false).start(true).build();

  @Test
  public void testLockServiceMXBean() throws Throwable {
    createLockServiceGrantor(memberVMs[0]);
    createLockService(memberVMs[1]);
    createLockService(memberVMs[2]);

    for (var memberVM : memberVMs) {
      verifyLockServiceMXBeanInMember(memberVM);
    }
    verifyLockServiceMXBeanInManager(managerVM);

    for (var memberVM : memberVMs) {
      closeLockService(memberVM);
    }
  }

  @Test
  public void testDistributedLockServiceMXBean() throws Throwable {
    createLockServiceGrantor(memberVMs[0]);
    createLockService(memberVMs[1]);
    createLockService(memberVMs[2]);

    verifyDistributedLockServiceMXBean(managerVM, 3);

    var member = managementTestRule.getDistributedMember(memberVMs[2]);
    verifyFetchOperations(managerVM, member);

    createLockService(managerVM);
    verifyDistributedLockServiceMXBean(managerVM, 4);

    for (var memberVM : memberVMs) {
      closeLockService(memberVM);
    }
    verifyProxyCleanupInManager(managerVM);
    verifyDistributedLockServiceMXBean(managerVM, 1);

    closeLockService(managerVM);
    verifyDistributedLockServiceMXBean(managerVM, 0);
  }

  private void verifyProxyCleanupInManager(final VM managerVM) {
    managerVM.invoke("verifyProxyCleanupInManager", () -> {
      var otherMembers = managementTestRule.getOtherNormalMembers();
      var service = managementTestRule.getSystemManagementService();

      for (final var member : otherMembers) {
        var objectName = service.getRegionMBeanName(member, LOCK_SERVICE_NAME);
        GeodeAwaitility.await()
            .untilAsserted(() -> assertThat(lockServiceMXBeanIsGone(service, objectName)).isTrue());
      }
    });
  }

  private boolean lockServiceMXBeanIsGone(final SystemManagementService service,
      final ObjectName objectName) {
    return service.getMBeanProxy(objectName, LockServiceMXBean.class) == null;
  }

  private void createLockServiceGrantor(final VM memberVM) {
    memberVM.invoke("createLockServiceGrantor", () -> {
      assertThat(DistributedLockService.getServiceNamed(LOCK_SERVICE_NAME)).isNull();

      var lockService = (DLockService) DistributedLockService.create(LOCK_SERVICE_NAME,
          managementTestRule.getCache().getDistributedSystem());
      DistributedMember grantor = lockService.getLockGrantorId().getLockGrantorMember();
      assertThat(grantor).isNotNull();

      var lockServiceMXBean = awaitLockServiceMXBean(LOCK_SERVICE_NAME);

      assertThat(lockServiceMXBean).isNotNull();
      assertThat(lockServiceMXBean.isDistributed()).isTrue();
      assertThat(lockServiceMXBean.getName()).isEqualTo(LOCK_SERVICE_NAME);
      assertThat(lockServiceMXBean.isLockGrantor()).isTrue();
      assertThat(lockServiceMXBean.fetchGrantorMember())
          .isEqualTo(managementTestRule.getDistributedMember().getId());
    });
  }

  private void createLockService(final VM anyVM) {
    anyVM.invoke("createLockService", () -> {
      assertThat(DistributedLockService.getServiceNamed(LOCK_SERVICE_NAME)).isNull();

      DistributedLockService.create(LOCK_SERVICE_NAME,
          managementTestRule.getCache().getDistributedSystem());

      var lockServiceMXBean = awaitLockServiceMXBean(LOCK_SERVICE_NAME);

      assertThat(lockServiceMXBean).isNotNull();
      assertThat(lockServiceMXBean.isDistributed()).isTrue();
      assertThat(lockServiceMXBean.isLockGrantor()).isFalse();
    });
  }

  private void closeLockService(final VM anyVM) {
    anyVM.invoke("closeLockService", () -> {
      assertThat(DistributedLockService.getServiceNamed(LOCK_SERVICE_NAME)).isNotNull();
      DistributedLockService.destroy(LOCK_SERVICE_NAME);

      awaitLockServiceMXBeanIsNull(LOCK_SERVICE_NAME);

      var service = managementTestRule.getManagementService();
      var lockServiceMXBean = service.getLocalLockServiceMBean(LOCK_SERVICE_NAME);
      assertThat(lockServiceMXBean).isNull();
    });
  }

  private void verifyLockServiceMXBeanInMember(final VM memberVM) {
    memberVM.invoke("verifyLockServiceMXBeanInManager", () -> {
      var lockService =
          DistributedLockService.getServiceNamed(LOCK_SERVICE_NAME);
      lockService.lock("lockObject_" + identifyPid(), MAX_WAIT_MILLIS, -1);

      var service = managementTestRule.getManagementService();
      var lockServiceMXBean = service.getLocalLockServiceMBean(LOCK_SERVICE_NAME);
      assertThat(lockServiceMXBean).isNotNull();

      var listHeldLock = lockServiceMXBean.listHeldLocks();
      assertThat(listHeldLock).hasSize(1);

      var lockThreadMap = lockServiceMXBean.listThreadsHoldingLock();
      assertThat(lockThreadMap).hasSize(1);
    });
  }

  /**
   * Verify lock data from remote Managing node
   */
  private void verifyLockServiceMXBeanInManager(final VM managerVM) throws Exception {
    managerVM.invoke("verifyLockServiceMXBeanInManager", () -> {
      var otherMembers = managementTestRule.getOtherNormalMembers();

      for (var member : otherMembers) {
        var lockServiceMXBean =
            awaitLockServiceMXBeanProxy(member, LOCK_SERVICE_NAME);
        assertThat(lockServiceMXBean).isNotNull();

        var listHeldLock = lockServiceMXBean.listHeldLocks();
        assertThat(listHeldLock).hasSize(1);

        var lockThreadMap = lockServiceMXBean.listThreadsHoldingLock();
        assertThat(lockThreadMap).hasSize(1);
      }
    });
  }

  private void verifyFetchOperations(final VM memberVM, final DistributedMember member) {
    memberVM.invoke("verifyFetchOperations", () -> {
      var service = managementTestRule.getManagementService();

      var distributedSystemMXBean = awaitDistributedSystemMXBean();
      var distributedLockServiceMXBeanName =
          getDistributedLockServiceName(LOCK_SERVICE_NAME);
      assertThat(distributedSystemMXBean.fetchDistributedLockServiceObjectName(LOCK_SERVICE_NAME))
          .isEqualTo(distributedLockServiceMXBeanName);

      var lockServiceMXBeanName = getLockServiceMBeanName(member.getId(), LOCK_SERVICE_NAME);
      assertThat(
          distributedSystemMXBean.fetchLockServiceObjectName(member.getId(), LOCK_SERVICE_NAME))
              .isEqualTo(lockServiceMXBeanName);
    });
  }

  /**
   * Verify Aggregate MBean
   */
  private void verifyDistributedLockServiceMXBean(final VM managerVM, final int memberCount) {
    managerVM.invoke("verifyDistributedLockServiceMXBean", () -> {
      var service = managementTestRule.getManagementService();

      if (memberCount == 0) {
        GeodeAwaitility.await().untilAsserted(
            () -> assertThat(service.getDistributedLockServiceMXBean(LOCK_SERVICE_NAME)).isNull());
        return;
      }

      var distributedLockServiceMXBean =
          awaitDistributedLockServiceMXBean(LOCK_SERVICE_NAME, memberCount);
      assertThat(distributedLockServiceMXBean).isNotNull();
      assertThat(distributedLockServiceMXBean.getName()).isEqualTo(LOCK_SERVICE_NAME);
    });
  }

  private DistributedSystemMXBean awaitDistributedSystemMXBean() {
    var service = managementTestRule.getManagementService();

    GeodeAwaitility.await()
        .untilAsserted(() -> assertThat(service.getDistributedSystemMXBean()).isNotNull());

    return service.getDistributedSystemMXBean();
  }

  /**
   * Await and return a DistributedRegionMXBean proxy with specified member count.
   */
  private DistributedLockServiceMXBean awaitDistributedLockServiceMXBean(
      final String lockServiceName, final int memberCount) {
    var service = managementTestRule.getManagementService();

    GeodeAwaitility.await().untilAsserted(() -> {
      assertThat(service.getDistributedLockServiceMXBean(lockServiceName)).isNotNull();
      assertThat(service.getDistributedLockServiceMXBean(lockServiceName).getMemberCount())
          .isEqualTo(memberCount);
    });

    return service.getDistributedLockServiceMXBean(lockServiceName);
  }

  /**
   * Await and return a LockServiceMXBean proxy for a specific member and lockServiceName.
   */
  private LockServiceMXBean awaitLockServiceMXBeanProxy(final DistributedMember member,
      final String lockServiceName) {
    var service = managementTestRule.getSystemManagementService();
    var lockServiceMXBeanName = service.getLockServiceMBeanName(member, lockServiceName);

    GeodeAwaitility.await().untilAsserted(
        () -> assertThat(service.getMBeanProxy(lockServiceMXBeanName, LockServiceMXBean.class))
            .isNotNull());

    return service.getMBeanProxy(lockServiceMXBeanName, LockServiceMXBean.class);
  }

  /**
   * Await creation of local LockServiceMXBean for specified lockServiceName.
   */
  private LockServiceMXBean awaitLockServiceMXBean(final String lockServiceName) {
    var service = managementTestRule.getSystemManagementService();

    GeodeAwaitility.await().untilAsserted(
        () -> assertThat(service.getLocalLockServiceMBean(lockServiceName)).isNotNull());

    return service.getLocalLockServiceMBean(lockServiceName);
  }

  /**
   * Await destruction of local LockServiceMXBean for specified lockServiceName.
   */
  private void awaitLockServiceMXBeanIsNull(final String lockServiceName) {
    var service = managementTestRule.getSystemManagementService();

    GeodeAwaitility.await().untilAsserted(
        () -> assertThat(service.getLocalLockServiceMBean(lockServiceName)).isNull());
  }

}
