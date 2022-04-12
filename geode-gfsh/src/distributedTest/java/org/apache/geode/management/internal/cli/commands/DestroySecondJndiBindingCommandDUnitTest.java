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

package org.apache.geode.management.internal.cli.commands;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.w3c.dom.Element;

import org.apache.geode.internal.jndi.JNDIInvoker;
import org.apache.geode.management.internal.configuration.utils.XmlUtils;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.GfshTest;
import org.apache.geode.test.junit.rules.GfshCommandRule;
import org.apache.geode.test.junit.rules.VMProvider;

@Category({GfshTest.class})
public class DestroySecondJndiBindingCommandDUnitTest {

  private MemberVM locator, server1, server2;

  @Rule
  public ClusterStartupRule cluster = new ClusterStartupRule();

  @Rule
  public GfshCommandRule gfsh = new GfshCommandRule();

  @Before
  public void before() throws Exception {
    locator = cluster.startLocatorVM(0);
    server1 = cluster.startServerVM(1, locator.getPort());
    server2 = cluster.startServerVM(2, locator.getPort());

    gfsh.connectAndVerify(locator);

    gfsh.execute(
        "create jndi-binding --name=jndi1 --type=SIMPLE --jdbc-driver-class=org.apache.derby.jdbc.EmbeddedDriver --connection-url=\"jdbc:derby:newDB;create=true\"");

    gfsh.execute(
        "create jndi-binding --name=jndi2 --type=SIMPLE --jdbc-driver-class=org.apache.derby.jdbc.EmbeddedDriver --connection-url=\"jdbc:derby:newDB;create=true\"");
  }

  @Test
  @SuppressWarnings("deprecation")
  public void testDestroySecondJndiBinding() {

    gfsh.executeAndAssertThat("destroy jndi-binding --name=jndi2").statusIsSuccess()
        .tableHasColumnOnlyWithValues("Member", "server-1", "server-2");

    // verify cluster config is updated
    locator.invoke(() -> {
      var ccService =
          ClusterStartupRule.getLocator().getConfigurationPersistenceService();
      var configuration = ccService.getConfiguration("cluster");
      var document = XmlUtils.createDocumentFromXml(configuration.getCacheXmlContent());
      var jndiBindings = document.getElementsByTagName("jndi-binding");

      assertThat(jndiBindings.getLength()).isEqualTo(1);

      var found = false;
      for (var i = 0; i < jndiBindings.getLength(); i++) {
        var eachBinding = (Element) jndiBindings.item(i);
        if (eachBinding.getAttribute("jndi-name").equals("jndi2")) {
          found = true;
          break;
        }
      }
      assertThat(found).isFalse();

      found = false;
      for (var i = 0; i < jndiBindings.getLength(); i++) {
        var eachBinding = (Element) jndiBindings.item(i);
        if (eachBinding.getAttribute("jndi-name").equals("jndi1")) {
          found = true;
          break;
        }
      }
      assertThat(found).isTrue();
    });

    // verify datasource exists
    VMProvider.invokeInEveryMember(
        () -> assertThat(JNDIInvoker.getNoOfAvailableDataSources()).isEqualTo(1), server1, server2);

    // bounce server1 and assert that there is still no datasource received from cluster config
    server1.stop(false);
    server1 = cluster.startServerVM(1, locator.getPort());

    // verify no datasource from cluster config
    server1.invoke(() -> {
      assertThat(JNDIInvoker.getNoOfAvailableDataSources()).isEqualTo(1);
    });
  }
}
