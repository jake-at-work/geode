/*
 *
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
package org.apache.geode.tools.pulse.tests.junit;

import static org.apache.geode.cache.Region.SEPARATOR;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.URI;

import org.apache.http.client.methods.RequestBuilder;
import org.apache.http.util.EntityUtils;
import org.json.JSONObject;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

/**
 * JUnit Tests for ClusterSelectedRegionService in the back-end server for region detail page
 *
 *
 */
@Ignore
public class ClusterSelectedRegionServiceTest extends BaseServiceTest {

  /**
   *
   * @throws java.lang.Exception
   */
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    doLogin();
    System.out.println("\n\nClusterSelectedRegionServiceTest :: Setup done");
  }

  /**
   *
   * @throws java.lang.Exception
   */
  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    doLogout();
    System.out.println("ClusterSelectedRegionServiceTest :: Teardown done");
  }

  /**
   *
   * @throws java.lang.Exception
   */
  @Override
  @Before
  public void setUp() throws Exception {}

  /**
   *
   * @throws java.lang.Exception
   */
  @Override
  @After
  public void tearDown() throws Exception {}

  /**
   * Tests that service returns json object
   *
   */
  @Test
  public void testResponseNotNull() {
    System.out.println(
        "ClusterSelectedRegionServiceTest ::  ------TESTCASE BEGIN : NULL RESPONSE CHECK FOR CLUSTER REGIONS------");
    if (httpclient != null) {
      try {
        var pulseupdate = RequestBuilder.post().setUri(new URI(PULSE_UPDATE_URL))
            .addParameter(PULSE_UPDATE_PARAM, PULSE_UPDATE_1_VALUE).build();
        var response = httpclient.execute(pulseupdate);
        try {
          var entity = response.getEntity();

          System.out.println("ClusterSelectedRegionServiceTest :: HTTP request status : "
              + response.getStatusLine());
          var respReader =
              new BufferedReader(new InputStreamReader(entity.getContent()));
          var sw = new StringWriter();
          var pw = new PrintWriter(sw);
          String sz = null;
          while ((sz = respReader.readLine()) != null) {
            pw.print(sz);
          }
          var jsonResp = sw.getBuffer().toString();
          System.out
              .println("ClusterSelectedRegionServiceTest :: JSON response returned : " + jsonResp);
          EntityUtils.consume(entity);

          var jsonObj = new JSONObject(jsonResp);
          Assert.assertNotNull(
              "ClusterSelectedRegionServiceTest :: Server returned null response for ClusterSelectedRegion",
              jsonObj.getJSONObject("ClusterSelectedRegion"));
        } finally {
          response.close();
        }
      } catch (Exception failed) {
        logException(failed);
        Assert.fail("Exception ! ");
      }
    } else {
      Assert.fail("ClusterSelectedRegionServiceTest :: No Http connection was established.");
    }
    System.out.println(
        "ClusterSelectedRegionServiceTest ::  ------TESTCASE END : NULL RESPONSE CHECK FOR CLUSTER REGIONS------\n");
  }

  /**
   *
   * Tests that response is for same logged in user
   *
   */
  @Test
  public void testResponseUsername() {
    System.out.println(
        "ClusterSelectedRegionServiceTest ::  ------TESTCASE BEGIN : NULL USERNAME IN RESPONSE CHECK FOR CLUSTER REGIONS------");
    if (httpclient != null) {
      try {
        var pulseupdate = RequestBuilder.post().setUri(new URI(PULSE_UPDATE_URL))
            .addParameter(PULSE_UPDATE_PARAM, PULSE_UPDATE_1_VALUE).build();
        var response = httpclient.execute(pulseupdate);
        try {
          var entity = response.getEntity();

          System.out.println("ClusterSelectedRegionServiceTest :: HTTP request status : "
              + response.getStatusLine());

          var respReader =
              new BufferedReader(new InputStreamReader(entity.getContent()));
          var sw = new StringWriter();
          var pw = new PrintWriter(sw);
          String sz = null;
          while ((sz = respReader.readLine()) != null) {
            pw.print(sz);
          }
          var jsonResp = sw.getBuffer().toString();
          System.out
              .println("ClusterSelectedRegionServiceTest :: JSON response returned : " + jsonResp);
          EntityUtils.consume(entity);

          var jsonObj = new JSONObject(jsonResp);
          var clusterSelectedRegionObj = jsonObj.getJSONObject("ClusterSelectedRegion");
          Assert.assertNotNull(
              "ClusterSelectedRegionServiceTest :: Server returned null response for ClusterSelectedRegion",
              clusterSelectedRegionObj);
          var szUser = clusterSelectedRegionObj.getString("userName");
          Assert.assertEquals(
              "ClusterSelectedRegionServiceTest :: Server returned wrong user name. Expected was admin. Server returned = "
                  + szUser,
              szUser, "admin");
        } finally {
          response.close();
        }
      } catch (Exception failed) {
        logException(failed);
        Assert.fail("Exception ! ");
      }
    } else {
      Assert.fail("ClusterSelectedRegionServiceTest :: No Http connection was established.");
    }
    System.out.println(
        "ClusterSelectedRegionServiceTest ::  ------TESTCASE END : NULL USERNAME IN RESPONSE CHECK FOR CLUSTER REGIONS------\n");
  }

  /**
   *
   * Tests that response is for same region
   *
   * Test method for
   * {@link org.apache.geode.tools.pulse.internal.service.ClusterSelectedRegionService#execute(javax.servlet.http.HttpServletRequest)}.
   *
   */
  @Test
  public void testResponseRegionPathMatches() {
    System.out.println(
        "ClusterSelectedRegionServiceTest ::  ------TESTCASE BEGIN : REGION PATH IN RESPONSE CHECK FOR CLUSTER REGIONS------");
    if (httpclient != null) {
      try {
        var pulseupdate = RequestBuilder.post().setUri(new URI(PULSE_UPDATE_URL))
            .addParameter(PULSE_UPDATE_PARAM, PULSE_UPDATE_1_VALUE).build();
        var response = httpclient.execute(pulseupdate);
        try {
          var entity = response.getEntity();

          System.out.println("ClusterSelectedRegionServiceTest :: HTTP request status : "
              + response.getStatusLine());

          var respReader =
              new BufferedReader(new InputStreamReader(entity.getContent()));
          var sw = new StringWriter();
          var pw = new PrintWriter(sw);
          String sz = null;
          while ((sz = respReader.readLine()) != null) {
            pw.print(sz);
          }
          var jsonResp = sw.getBuffer().toString();
          System.out
              .println("ClusterSelectedRegionServiceTest :: JSON response returned : " + jsonResp);
          EntityUtils.consume(entity);

          var jsonObj = new JSONObject(jsonResp);
          var clusterSelectedRegionObj = jsonObj.getJSONObject("ClusterSelectedRegion");
          Assert.assertNotNull(
              "ClusterSelectedRegionServiceTest :: Server returned null response for ClusterSelectedRegion",
              clusterSelectedRegionObj);
          var jsonObjRegion = clusterSelectedRegionObj.getJSONObject("selectedRegion");
          Assert.assertNotNull(
              "ClusterSelectedRegionServiceTest :: Server returned null response for selectedRegion",
              jsonObjRegion);
          Assert.assertTrue(
              "ClusterSelectedRegionServiceTest :: Server did not return 'path' of region",
              jsonObjRegion.has("path"));
          var szPath = jsonObjRegion.getString("path");
          Assert.assertEquals(
              "ClusterSelectedRegionServiceTest :: Server returned wrong region path. Expected region path = "
                  + SEPARATOR + "GlobalVilage_2" + SEPARATOR
                  + "GlobalVilage_9 , actual region path = "
                  + szPath,
              szPath, SEPARATOR + "GlobalVilage_2" + SEPARATOR + "GlobalVilage_9");
        } finally {
          response.close();
        }
      } catch (Exception failed) {
        logException(failed);
        Assert.fail("Exception ! ");
      }
    } else {
      Assert.fail("ClusterSelectedRegionServiceTest :: No Http connection was established.");
    }
    System.out.println(
        "ClusterSelectedRegionServiceTest ::  ------TESTCASE END : REGION PATH IN RESPONSE CHECK FOR CLUSTER REGIONS------\n");
  }

  /**
   *
   * Tests that response is for same region
   *
   * Test method for
   * {@link org.apache.geode.tools.pulse.internal.service.ClusterSelectedRegionService#execute(javax.servlet.http.HttpServletRequest)}.
   *
   */
  @Test
  public void testResponseNonExistentRegion() {
    System.out.println(
        "ClusterSelectedRegionServiceTest ::  ------TESTCASE BEGIN : NON-EXISTENT REGION CHECK FOR CLUSTER REGIONS------");
    if (httpclient != null) {
      try {
        var pulseupdate = RequestBuilder.post().setUri(new URI(PULSE_UPDATE_URL))
            .addParameter(PULSE_UPDATE_PARAM, PULSE_UPDATE_2_VALUE).build();
        var response = httpclient.execute(pulseupdate);
        try {
          var entity = response.getEntity();

          System.out.println("ClusterSelectedRegionServiceTest :: HTTP request status : "
              + response.getStatusLine());

          var respReader =
              new BufferedReader(new InputStreamReader(entity.getContent()));
          var sw = new StringWriter();
          var pw = new PrintWriter(sw);
          String sz = null;
          while ((sz = respReader.readLine()) != null) {
            pw.print(sz);
          }
          var jsonResp = sw.getBuffer().toString();
          System.out
              .println("ClusterSelectedRegionServiceTest :: JSON response returned : " + jsonResp);
          EntityUtils.consume(entity);

          var jsonObj = new JSONObject(jsonResp);
          var clusterSelectedRegionObj = jsonObj.getJSONObject("ClusterSelectedRegion");
          Assert.assertNotNull(
              "ClusterSelectedRegionServiceTest :: Server returned null response for ClusterSelectedRegion",
              clusterSelectedRegionObj);
          var jsonObjRegion = clusterSelectedRegionObj.getJSONObject("selectedRegion");
          Assert.assertNotNull(
              "ClusterSelectedRegionServiceTest :: Server returned null response for selectedRegion",
              jsonObjRegion);
          Assert.assertTrue(
              "ClusterSelectedRegionServiceTest :: Server did not return error on non-existent region",
              jsonObjRegion.has("errorOnRegion"));
        } finally {
          response.close();
        }
      } catch (Exception failed) {
        logException(failed);
        Assert.fail("Exception ! ");
      }
    } else {
      Assert.fail("ClusterSelectedRegionServiceTest :: No Http connection was established.");
    }
    System.out.println(
        "ClusterSelectedRegionServiceTest ::  ------TESTCASE END : NON-EXISTENT REGION CHECK FOR CLUSTER REGIONS------\n");
  }

  /**
   *
   * Tests that service returns json object
   *
   * Test method for
   * {@link org.apache.geode.tools.pulse.internal.service.ClusterSelectedRegionService#execute(javax.servlet.http.HttpServletRequest)}.
   *
   */
  @Test
  public void testResponseMemerberCount() {
    System.out.println(
        "ClusterSelectedRegionServiceTest ::  ------TESTCASE BEGIN : MISMATCHED MEMBERCOUNT FOR REGION CHECK FOR CLUSTER REGIONS------");
    if (httpclient != null) {
      try {
        var pulseupdate = RequestBuilder.post().setUri(new URI(PULSE_UPDATE_URL))
            .addParameter(PULSE_UPDATE_PARAM, PULSE_UPDATE_1_VALUE).build();
        var response = httpclient.execute(pulseupdate);
        try {
          var entity = response.getEntity();

          System.out.println("ClusterSelectedRegionServiceTest :: HTTP request status : "
              + response.getStatusLine());

          var respReader =
              new BufferedReader(new InputStreamReader(entity.getContent()));
          var sw = new StringWriter();
          var pw = new PrintWriter(sw);
          String sz = null;
          while ((sz = respReader.readLine()) != null) {
            pw.print(sz);
          }
          var jsonResp = sw.getBuffer().toString();
          System.out
              .println("ClusterSelectedRegionServiceTest :: JSON response returned : " + jsonResp);
          EntityUtils.consume(entity);

          var jsonObj = new JSONObject(jsonResp);
          var clusterSelectedRegionObj = jsonObj.getJSONObject("ClusterSelectedRegion");
          Assert.assertNotNull(
              "ClusterSelectedRegionServiceTest :: Server returned null ClusterSelectedRegion",
              clusterSelectedRegionObj);
          var jsonObjRegion = clusterSelectedRegionObj.getJSONObject("selectedRegion");
          Assert.assertNotNull(
              "ClusterSelectedRegionServiceTest :: Server returned null for selectedRegion",
              jsonObjRegion);
          Assert.assertTrue(
              "ClusterSelectedRegionServiceTest :: Server did not return 'memberCount' of region",
              jsonObjRegion.has("memberCount"));
          var memberCount = jsonObjRegion.getInt("memberCount");
          Assert.assertTrue(
              "ClusterSelectedRegionServiceTest :: Server did not return 'members' of region",
              jsonObjRegion.has("members"));
          var arrMembers = jsonObjRegion.getJSONArray("members");
          Assert.assertNotNull(
              "ClusterSelectedRegionServiceTest :: Server returned null response in selectedRegion",
              arrMembers);
          var members = arrMembers.length();
          Assert.assertEquals(
              "ClusterSelectedRegionServiceTest :: Server returned mismatched member count and region members",
              members, memberCount);
        } finally {
          response.close();
        }
      } catch (Exception failed) {
        logException(failed);
        Assert.fail("Exception ! ");
      }
    } else {
      Assert.fail("ClusterSelectedRegionServiceTest :: No Http connection was established.");
    }
    System.out.println(
        "ClusterSelectedRegionServiceTest ::  ------TESTCASE END : MISMATCHED MEMBERCOUNT FOR REGION CHECK FOR CLUSTER REGIONS------\n");
  }

}
