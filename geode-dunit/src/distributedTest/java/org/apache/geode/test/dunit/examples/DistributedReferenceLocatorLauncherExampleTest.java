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
package org.apache.geode.test.dunit.examples;

import static java.util.Arrays.asList;
import static org.apache.geode.distributed.ConfigurationProperties.HTTP_SERVICE_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.test.dunit.VM.getController;
import static org.apache.geode.test.dunit.VM.getHostName;
import static org.apache.geode.test.dunit.VM.getVM;
import static org.apache.geode.test.dunit.VM.getVMId;
import static org.apache.geode.test.dunit.rules.DistributedRule.getLocatorPort;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.Serializable;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.distributed.LocatorLauncher;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.test.dunit.rules.DistributedReference;
import org.apache.geode.test.junit.rules.serializable.SerializableTemporaryFolder;

@SuppressWarnings("serial")
public class DistributedReferenceLocatorLauncherExampleTest implements Serializable {

  @Rule
  public DistributedReference<LocatorLauncher> locatorLauncher = new DistributedReference<>();

  @Rule
  public SerializableTemporaryFolder temporaryFolder = new SerializableTemporaryFolder();

  @Before
  public void setUp() {
    var hostName = getHostName();
    var randomPorts = AvailablePortHelper.getRandomAvailableTCPPorts(5);

    var locators = new StringBuilder()
        .append(hostName).append('[').append(getLocatorPort()).append(']').append(',')
        .append(hostName).append('[').append(randomPorts[0]).append(']').append(',')
        .append(hostName).append('[').append(randomPorts[1]).append(']').append(',')
        .append(hostName).append('[').append(randomPorts[2]).append(']').append(',')
        .append(hostName).append('[').append(randomPorts[3]).append(']').append(',')
        .append(hostName).append('[').append(randomPorts[4]).append(']');

    var index = 0;
    for (var vm : asList(getVM(0), getVM(1), getVM(2), getVM(3), getController())) {
      var whichPort = index++;
      vm.invoke(() -> {
        var name = "locator-" + getVMId();
        var locatorLauncher = new LocatorLauncher.Builder()
            .setWorkingDirectory(temporaryFolder.newFolder(name).getAbsolutePath())
            .setMemberName(name)
            .setPort(randomPorts[whichPort])
            .set(LOCATORS, locators.toString())
            .set(HTTP_SERVICE_PORT, "0")
            .set(JMX_MANAGER_PORT, "0")
            .build();
        locatorLauncher.start();
        this.locatorLauncher.set(locatorLauncher);
      });
    }
  }

  @Test
  public void eachVmHasItsOwnLocatorLauncher() {
    for (var vm : asList(getVM(0), getVM(1), getVM(2), getVM(3), getController())) {
      vm.invoke(() -> {
        assertThat(locatorLauncher.get()).isNotNull();

        var cache = (InternalCache) locatorLauncher.get().getCache();
        var system = cache.getInternalDistributedSystem();
        assertThat(system.getAllOtherMembers()).hasSize(5);
      });
    }
  }
}
