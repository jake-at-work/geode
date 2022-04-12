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
 *
 */
package org.apache.geode.tools.pulse.ui;

import static org.apache.geode.cache.Region.SEPARATOR;
import static org.apache.geode.tools.pulse.tests.ui.PulseTestConstants.CLUSTER_CLIENTS_ID;
import static org.apache.geode.tools.pulse.tests.ui.PulseTestConstants.CLUSTER_FUNCTIONS_ID;
import static org.apache.geode.tools.pulse.tests.ui.PulseTestConstants.CLUSTER_GCPAUSES_ID;
import static org.apache.geode.tools.pulse.tests.ui.PulseTestConstants.CLUSTER_QUERIESPERSEC_ID;
import static org.apache.geode.tools.pulse.tests.ui.PulseTestConstants.CLUSTER_SUBSCRIPTION_ID;
import static org.apache.geode.tools.pulse.tests.ui.PulseTestConstants.CLUSTER_UNIQUECQS_ID;
import static org.apache.geode.tools.pulse.tests.ui.PulseTestConstants.CLUSTER_VIEW_GRID_ID;
import static org.apache.geode.tools.pulse.tests.ui.PulseTestConstants.CLUSTER_VIEW_LABEL;
import static org.apache.geode.tools.pulse.tests.ui.PulseTestConstants.CLUSTER_VIEW_LOCATORS_ID;
import static org.apache.geode.tools.pulse.tests.ui.PulseTestConstants.CLUSTER_VIEW_MEMBERS_ID;
import static org.apache.geode.tools.pulse.tests.ui.PulseTestConstants.CLUSTER_VIEW_REGIONS_ID;
import static org.apache.geode.tools.pulse.tests.ui.PulseTestConstants.DATA_BROWSER_LABEL;
import static org.apache.geode.tools.pulse.tests.ui.PulseTestConstants.DATA_BROWSER_REGION1_CHECKBOX;
import static org.apache.geode.tools.pulse.tests.ui.PulseTestConstants.DATA_BROWSER_REGIONName1;
import static org.apache.geode.tools.pulse.tests.ui.PulseTestConstants.DATA_DROPDOWN_ID;
import static org.apache.geode.tools.pulse.tests.ui.PulseTestConstants.MEMBER_VIEW_JVMPAUSES_ID;
import static org.apache.geode.tools.pulse.tests.ui.PulseTestConstants.MEMBER_VIEW_REGION_ID;
import static org.apache.geode.tools.pulse.tests.ui.PulseTestConstants.MEMBER_VIEW_SOCKETS_ID;
import static org.apache.geode.tools.pulse.tests.ui.PulseTestConstants.REDUNDANCY_GRID_ID;
import static org.apache.geode.tools.pulse.tests.ui.PulseTestConstants.SERVER_GROUP_GRID_ID;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertNotNull;

import java.text.DecimalFormat;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.interactions.Actions;
import org.openqa.selenium.support.ui.ExpectedConditions;
import org.openqa.selenium.support.ui.WebDriverWait;

import org.apache.geode.tools.pulse.internal.data.Cluster;

public abstract class PulseAcceptanceTestBase {
  public abstract WebDriver getWebDriver();

  public abstract String getPulseURL();

  public abstract Cluster getCluster();

  private final DecimalFormat df = new DecimalFormat("0.00");

  @Before
  public void setup() {
    // Make sure we go to the home page first
    searchByXPathAndClick("//a[text()='Cluster View']");
  }

  private void searchByLinkAndClick(String linkText) {
    var element = new WebDriverWait(getWebDriver(), 2)
        .until(ExpectedConditions.elementToBeClickable(By.linkText(linkText)));
    assertNotNull(element);
    element.click();
  }

  private void searchByIdAndClick(String id) {
    var element = getWebDriver().findElement(By.id(id));
    assertNotNull(element);
    element.click();
  }

  private void searchByXPathAndClick(String xpath) {
    var element = getWebDriver().findElement(By.xpath(xpath));
    assertNotNull(element);
    element.click();
  }

  @Test
  public void testClusterDetailsAndWidgets() {
    var displayedLocatorCount =
        getWebDriver().findElement(By.id(CLUSTER_VIEW_LOCATORS_ID)).getText();
    assertThat(String.valueOf(getCluster().getLocatorCount())).isEqualTo(displayedLocatorCount);

    var displayedRegionCount =
        getWebDriver().findElement(By.id(CLUSTER_VIEW_REGIONS_ID)).getText();
    assertThat(String.valueOf(getCluster().getTotalRegionCount())).isEqualTo(displayedRegionCount);

    var displayedClusterMemberCount =
        getWebDriver().findElement(By.id(CLUSTER_VIEW_MEMBERS_ID)).getText();
    assertThat(String.valueOf(getCluster().getMemberCount()))
        .isEqualTo(displayedClusterMemberCount);

    var displyedClusterClients = getWebDriver().findElement(By.id(CLUSTER_CLIENTS_ID)).getText();
    assertThat(String.valueOf(getCluster().getClientConnectionCount()))
        .isEqualTo(displyedClusterClients);

    var displayedFnRunningCount =
        getWebDriver().findElement(By.id(CLUSTER_FUNCTIONS_ID)).getText();
    assertThat(String.valueOf(getCluster().getRunningFunctionCount()))
        .isEqualTo(displayedFnRunningCount);

    var displayedUniqueCQs = getWebDriver().findElement(By.id(CLUSTER_UNIQUECQS_ID)).getText();
    assertThat(String.valueOf(getCluster().getRegisteredCQCount())).isEqualTo(displayedUniqueCQs);

    var displayedSubscriptionCount =
        getWebDriver().findElement(By.id(CLUSTER_SUBSCRIPTION_ID)).getText();
    assertThat(String.valueOf(getCluster().getSubscriptionCount()))
        .isEqualTo(displayedSubscriptionCount);

    // TODO: verify it is the right counter
    var displayedGCPauses = getWebDriver().findElement(By.id(CLUSTER_GCPAUSES_ID)).getText();
    assertThat(String.valueOf(getCluster().getPreviousJVMPauseCount()))
        .isEqualTo(displayedGCPauses);

    var clusterQueriesPerSec =
        getWebDriver().findElement(By.id(CLUSTER_QUERIESPERSEC_ID)).getText();
    assertThat(df.format(getCluster().getQueriesPerSec())).isEqualTo(clusterQueriesPerSec);
  }

  @Test
  @Ignore("flaky")
  public void testClusterGridViewMemberDetails() {
    searchByIdAndClick("default_grid_button");
    var elements =
        getWebDriver().findElements(By.xpath("//table[@id='memberList']/tbody/tr"));

    var actualMembers = getCluster().getMembers();
    // table contains header row so actual members is one less than the tr elements.
    assertThat(actualMembers.length).isEqualTo(elements.size() - 1);

    for (var actualMember : actualMembers) {
      // reset from member view as we are looping
      searchByXPathAndClick("//a[text()='Cluster View']");
      searchByIdAndClick("default_grid_button");
      var displayedMemberId =
          getWebDriver().findElement(By.xpath("//table[@id='memberList']/tbody/tr[contains(@id, '"
              + actualMember.getName() + "')]/td")).getText();
      assertThat(actualMember.getId()).contains(displayedMemberId);

      var displayedMemberName =
          getWebDriver().findElement(By.xpath("//table[@id='memberList']/tbody/tr[contains(@id, '"
              + actualMember.getName() + "')]/td[2]")).getText();
      assertThat(actualMember.getName()).isEqualTo(displayedMemberName);

      var displayedMemberHost =
          getWebDriver().findElement(By.xpath("//table[@id='memberList']/tbody/tr[contains(@id, '"
              + actualMember.getName() + "')]/td[3]")).getText();
      assertThat(actualMember.getHost()).isEqualTo(displayedMemberHost);

      // now click the grid row to go to member view and assert details displayed
      searchByXPathAndClick("//table[@id='memberList']/tbody/tr[contains(@id, '"
          + actualMember.getName() + "')]/td");

      var displayedRegionCount =
          getWebDriver().findElement(By.id(MEMBER_VIEW_REGION_ID)).getText();
      assertThat(String.valueOf(actualMember.getTotalRegionCount()))
          .isEqualTo(displayedRegionCount);

      var displaySocketCount =
          getWebDriver().findElement(By.id(MEMBER_VIEW_SOCKETS_ID)).getText();
      if (actualMember.getTotalFileDescriptorOpen() < 0) {
        assertThat("NA").isEqualTo(displaySocketCount);
      } else {
        assertThat(String.valueOf(actualMember.getTotalFileDescriptorOpen()))
            .isEqualTo(displaySocketCount);
      }

      var displayedJVMPauses =
          getWebDriver().findElement(By.id(MEMBER_VIEW_JVMPAUSES_ID)).getText();
      assertThat(String.valueOf(actualMember.getPreviousJVMPauseCount()))
          .isEqualTo(displayedJVMPauses);
    }
  }

  @Test
  public void testDropDownList() {
    searchByIdAndClick("default_grid_button");
    searchByXPathAndClick("//table[@id='memberList']/tbody/tr[contains(@id, 'locator')]/td");
    searchByIdAndClick("memberName");
    searchByLinkAndClick("locator");
    searchByIdAndClick("memberName");
    // searchByLinkAndClick("server-1");
  }

  @Test
  public void userCanGetToPulseDetails() {
    getWebDriver().get(getPulseURL() + "pulseVersion");
    assertThat(getWebDriver().getPageSource()).contains("sourceRevision");
  }

  @Test
  public void testMemberGridViewData() {
    searchByXPathAndClick("//a[text()='Cluster View']");
    searchByIdAndClick("default_grid_button");
    searchByXPathAndClick("//table[@id='memberList']/tbody/tr[contains(@id, 'server-1')]/td");
    searchByXPathAndClick("//a[@id='btngridIcon']");

    // get the number of rows on the grid
    var displayedRegionsList =
        getWebDriver().findElements(By.xpath("//table[@id='memberRegionsList']/tbody/tr"));
    var actualRegions = getCluster().getMember("server-1").getMemberRegionsList();
    // table contains header row so actual regions is one less than the tr elements.
    assertThat(actualRegions.length).isEqualTo(displayedRegionsList.size() - 1);

    for (var i = 0; i < actualRegions.length; i++) {
      var displayedMemberRegionName = getWebDriver()
          .findElement(By.xpath(
              "//table[@id='memberRegionsList']/tbody/tr[contains(@id, '" + (i + 1) + "')]/td[1]"))
          .getText();
      assertThat(actualRegions[i].getName()).isEqualTo(displayedMemberRegionName);

      var displayedMemberRegionType = getWebDriver()
          .findElement(By.xpath(
              "//table[@id='memberRegionsList']/tbody/tr[contains(@id, '" + (i + 1) + "')]/td[2]"))
          .getText();
      assertThat(actualRegions[i].getRegionType()).isEqualTo(displayedMemberRegionType);

      var displayedMemberRegionEntryCount = getWebDriver()
          .findElement(By.xpath(
              "//table[@id='memberRegionsList']/tbody/tr[contains(@id, '" + (i + 1) + "')]/td[3]"))
          .getText();
      assertThat(String.valueOf(actualRegions[i].getSystemRegionEntryCount()))
          .isEqualTo(displayedMemberRegionEntryCount);
    }
  }

  @Test
  public void testDataBrowserRegionName() {
    if (getCluster().getTotalRegionCount() > 0) {
      var server = getCluster().getMember("server-1");
      // click data browser
      searchByLinkAndClick(DATA_BROWSER_LABEL);

      var regionList =
          getWebDriver().findElements(By.xpath("//ul[@id='treeDemo']/li"));
      assertThat(getCluster().getTotalRegionCount()).isEqualTo(regionList.size());
      var displayedRegionName =
          getWebDriver().findElement(By.id(DATA_BROWSER_REGIONName1)).getText();
      assertThat(server.getMemberRegionsList()[0].getName()).isEqualTo(displayedRegionName);

      searchByIdAndClick(DATA_BROWSER_REGION1_CHECKBOX);
      var memberList =
          getWebDriver().findElements(By.xpath("//ul[@id='membersList']/li"));
      assertThat(getCluster().getClusterRegion(SEPARATOR + "FOO").getMemberCount())
          .isEqualTo(memberList.size());
      var DataBrowserMember1Name1 =
          getWebDriver().findElement(By.xpath("//label[@for='Member0']")).getText();
      assertThat("server-1").isEqualTo(DataBrowserMember1Name1);
      searchByIdAndClick(DATA_BROWSER_REGION1_CHECKBOX);

      // execute a query
      var queryTextArea = getWebDriver().findElement(By.id("dataBrowserQueryText"));
      queryTextArea.sendKeys("select * from " + SEPARATOR + "FOO");
      var executeButton = getWebDriver().findElement(By.id("btnExecuteQuery"));
      executeButton.click();
      var queryResultHeader = getWebDriver()
          .findElement(By.xpath("//div[@id='clusterDetails']/div/div/span[@class='n-title']"))
          .getText();
      assertThat("java.lang.String").isEqualTo(queryResultHeader);
    }
  }

  @Test
  public void testRegionViewTreeMapPopUpData() {
    if (getCluster().getTotalRegionCount() > 0) {
      searchByLinkAndClick(CLUSTER_VIEW_LABEL);
      searchByLinkAndClick(DATA_DROPDOWN_ID);
      searchByIdAndClick("GraphTreeMapClusterData-canvas");
    }
  }

  @Test
  public void testDataViewTreeMapPopUpData() {
    if (getCluster().getTotalRegionCount() > 0) {
      var actualRegion = getCluster().getClusterRegion(SEPARATOR + "FOO");
      searchByLinkAndClick(CLUSTER_VIEW_LABEL);
      searchByLinkAndClick(DATA_DROPDOWN_ID);
      var TreeMapMember =
          getWebDriver().findElement(By.id("GraphTreeMapClusterData-canvas"));
      var builder = new Actions(getWebDriver());
      builder.clickAndHold(TreeMapMember).perform();

      var displayedRegionType = getWebDriver()
          .findElement(By.xpath("//div[@id='_tooltip']/div/div/div[2]/div/div[2]/div")).getText();
      assertThat(actualRegion.getRegionType()).isEqualTo(displayedRegionType);

      var displayedEntryCount = getWebDriver()
          .findElement(By.xpath("//div[@id='_tooltip']/div/div/div[2]/div[2]/div[2]/div"))
          .getText();
      assertThat(String.valueOf(actualRegion.getSystemRegionEntryCount()))
          .isEqualTo(displayedEntryCount);

      var displayedEntrySize = getWebDriver()
          .findElement(By.xpath("//div[@id='_tooltip']/div/div/div[2]/div[3]/div[2]/div"))
          .getText();
      if (actualRegion.getEntrySize() > 0) {
        assertThat(new DecimalFormat("##.####").format(actualRegion.getEntrySize()))
            .isEqualTo(displayedEntrySize);
      } else {
        assertThat("NA").isEqualTo(displayedEntrySize);
      }
      builder.moveToElement(TreeMapMember).release().perform();
    }
  }

  private void testTreeMapPopUpData(String gridIcon) {
    for (var member : getCluster().getMembersHMap().keySet()) {
      searchByLinkAndClick(CLUSTER_VIEW_LABEL);
      if (gridIcon.equals(SERVER_GROUP_GRID_ID)) {
        var serverGroupRadio =
            getWebDriver().findElement(By.xpath("//label[@for='radio-servergroups']"));
        serverGroupRadio.click();
      } else if (gridIcon.equals(REDUNDANCY_GRID_ID)) {
        var redundancyGroupRadio =
            getWebDriver().findElement(By.xpath("//label[@for='radio-redundancyzones']"));
        redundancyGroupRadio.click();
      }
      searchByIdAndClick(gridIcon);
      var TreeMapMember = new WebDriverWait(getWebDriver(), 5).until(ExpectedConditions
          .elementToBeClickable(By.xpath("//*[contains(text(), '" + member + "')]")));
      var builder = new Actions(getWebDriver());
      builder.clickAndHold(TreeMapMember).perform();

      var actualMember = getCluster().getMembersHMap().get(member);
      var displayedThreadCount = getWebDriver()
          .findElement(By.xpath("//div[@id='_tooltip']/div/div/div[2]/div[4]/div[2]/div"))
          .getText();
      assertThat(String.valueOf(actualMember.getNumThreads())).isEqualTo(displayedThreadCount);

      var displayedSocketCount = getWebDriver()
          .findElement(By.xpath("//div[@id='_tooltip']/div/div/div[2]/div[5]/div[2]/div"))
          .getText();
      if (actualMember.getTotalFileDescriptorOpen() > 0) {
        assertThat(String.valueOf(actualMember.getTotalFileDescriptorOpen()))
            .isEqualTo(displayedSocketCount);
      } else {
        assertThat("NA").isEqualTo(displayedSocketCount);
      }
      builder.moveToElement(TreeMapMember).release().perform();
    }
  }

  @Test
  @Ignore("flaky")
  public void testTopologyPopUpData() {
    testTreeMapPopUpData(CLUSTER_VIEW_GRID_ID);
  }
}
