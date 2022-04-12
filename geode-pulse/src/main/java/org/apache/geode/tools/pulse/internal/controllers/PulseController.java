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

package org.apache.geode.tools.pulse.internal.controllers;

import java.io.IOException;
import java.util.Iterator;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.text.StringEscapeUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.security.oauth2.core.OAuth2AuthenticationException;
import org.springframework.security.oauth2.core.OAuth2AuthorizationException;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

import org.apache.geode.tools.pulse.internal.data.Cluster;
import org.apache.geode.tools.pulse.internal.data.PulseVersion;
import org.apache.geode.tools.pulse.internal.data.Repository;
import org.apache.geode.tools.pulse.internal.service.PulseServiceFactory;
import org.apache.geode.tools.pulse.internal.service.SystemAlertsService;

/**
 * Class PulseController
 *
 * This class contains the implementations for all http Ajax requests needs to be served in Pulse.
 *
 * @since GemFire version 7.5
 */
@Controller
public class PulseController {

  private static final Logger logger = LogManager.getLogger();

  private static final String QUERYSTRING_PARAM_ACTION = "action";
  private static final String QUERYSTRING_PARAM_QUERYID = "queryId";
  private static final String ACTION_VIEW = "view";
  private static final String ACTION_DELETE = "delete";

  private static final String STATUS_REPSONSE_SUCCESS = "success";
  private static final String STATUS_REPSONSE_FAIL = "fail";

  private static final String ERROR_REPSONSE_QUERYNOTFOUND = "No queries found";
  private static final String ERROR_REPSONSE_QUERYIDMISSING = "Query id is missing";

  private static final String EMPTY_JSON = "{}";

  // Shared object to hold pulse version details
  private final PulseVersion pulseVersion;

  private final ObjectMapper mapper = new ObjectMapper();
  private final PulseServiceFactory pulseServiceFactory;
  private final Repository repository;

  @Autowired
  public PulseController(PulseServiceFactory pulseServiceFactory, Repository repository,
      PulseVersion pulseVersion) {
    this.pulseServiceFactory = pulseServiceFactory;
    this.repository = repository;
    this.pulseVersion = pulseVersion;
  }

  public PulseVersion getPulseVersion() {
    return pulseVersion;
  }

  @RequestMapping(value = "/pulseUpdate", method = RequestMethod.POST)
  public void getPulseUpdate(HttpServletRequest request, HttpServletResponse response)
      throws IOException {
    var pulseData = request.getParameter("pulseData");

    var responseMap = mapper.createObjectNode();

    try {
      var requestMap = mapper.readTree(pulseData);
      Iterator<?> keys = requestMap.fieldNames();

      // Execute Services
      while (keys.hasNext()) {
        var serviceName = keys.next().toString();
        try {
          var pulseService = pulseServiceFactory.getPulseServiceInstance(serviceName);
          responseMap.set(serviceName, pulseService.execute(request));
        } catch (OAuth2AuthenticationException | OAuth2AuthorizationException e) {
          logger.warn("serviceException [for service {}] = {}", serviceName, e.getMessage());
          response.setStatus(HttpStatus.UNAUTHORIZED.value());
          return;
        } catch (Exception serviceException) {
          logger.warn("serviceException [for service {}] = {}", serviceName,
              serviceException.getMessage());
          responseMap.put(serviceName, EMPTY_JSON);
        }
      }
    } catch (Exception e) {
      logger.debug("Exception Occurred : ", e);
    }

    // Create Response
    response.getOutputStream().write(responseMap.toString().getBytes());
  }

  @RequestMapping(value = "/authenticateUser", method = RequestMethod.GET)
  public void authenticateUser(HttpServletRequest request, HttpServletResponse response) {
    // json object to be sent as response
    var responseJSON = mapper.createObjectNode();

    try {
      responseJSON.put("isUserLoggedIn", isUserLoggedIn(request));
      // Send json response
      response.getOutputStream().write(responseJSON.toString().getBytes());
    } catch (Exception e) {
      logger.debug("Exception Occurred : ", e);
    }
  }

  /**
   * Method isUserLoggedIn Check whether user is logged in or not.
   *
   */
  protected boolean isUserLoggedIn(HttpServletRequest request) {
    return null != request.getUserPrincipal();
  }

  @RequestMapping(value = "/pulseVersion", method = RequestMethod.GET)
  public void pulseVersion(@SuppressWarnings("unused") HttpServletRequest request,
      HttpServletResponse response)
      throws IOException {

    // json object to be sent as response
    var responseJSON = mapper.createObjectNode();

    try {
      // Response
      responseJSON.put("pulseVersion", pulseVersion.getPulseVersion());
      responseJSON.put("buildId", pulseVersion.getPulseBuildId());
      responseJSON.put("sourceDate", pulseVersion.getPulseSourceDate());
      responseJSON.put("sourceRevision", pulseVersion.getPulseSourceRevision());
      responseJSON.put("sourceRepository", pulseVersion.getPulseSourceRepository());

    } catch (Exception e) {
      logger.debug("Exception Occurred : ", e);
    }

    // Send json response
    response.getOutputStream().write(responseJSON.toString().getBytes());
  }

  @RequestMapping(value = "/clearAlerts", method = RequestMethod.GET)
  public void clearAlerts(HttpServletRequest request, HttpServletResponse response)
      throws IOException {
    int alertType;
    var responseJSON = mapper.createObjectNode();

    try {
      alertType = Integer.parseInt(request.getParameter("alertType"));
    } catch (NumberFormatException e) {
      // Empty json response
      response.getOutputStream().write(responseJSON.toString().getBytes());
      logger.debug(e);
      return;
    }

    try {
      var isClearAll = Boolean.parseBoolean(request.getParameter("clearAll"));
      // get cluster object
      var cluster = repository.getCluster();
      cluster.clearAlerts(alertType, isClearAll);
      responseJSON.put("status", "deleted");
      responseJSON.set("systemAlerts",
          SystemAlertsService.getAlertsJson(cluster, cluster.getNotificationPageNumber()));
      responseJSON.put("pageNumber", cluster.getNotificationPageNumber());

      var isGFConnected = cluster.isConnectedFlag();
      if (isGFConnected) {
        responseJSON.put("connectedFlag", isGFConnected);
      } else {
        responseJSON.put("connectedFlag", isGFConnected);
        responseJSON.put("connectedErrorMsg", cluster.getConnectionErrorMsg());
      }
    } catch (Exception e) {
      logger.debug("Exception Occurred : ", e);
    }

    // Send json response
    response.getOutputStream().write(responseJSON.toString().getBytes());
  }

  @RequestMapping(value = "/acknowledgeAlert", method = RequestMethod.GET)
  public void acknowledgeAlert(HttpServletRequest request, HttpServletResponse response)
      throws IOException {
    int alertId;
    var responseJSON = mapper.createObjectNode();

    try {
      alertId = Integer.parseInt(request.getParameter("alertId"));
    } catch (NumberFormatException e) {
      // Empty json response
      response.getOutputStream().write(responseJSON.toString().getBytes());
      logger.debug(e);
      return;
    }

    try {
      // get cluster object
      var cluster = repository.getCluster();

      // set alert is acknowledged
      cluster.acknowledgeAlert(alertId);
      responseJSON.put("status", "deleted");
    } catch (Exception e) {
      logger.debug("Exception Occurred", e);
    }

    // Send json response
    response.getOutputStream().write(responseJSON.toString().getBytes());
  }

  @RequestMapping(value = "/dataBrowserRegions", method = RequestMethod.GET)
  public void dataBrowserRegions(@SuppressWarnings("unused") HttpServletRequest request,
      HttpServletResponse response)
      throws IOException {
    // get cluster object
    var cluster = repository.getCluster();

    // json object to be sent as response
    var responseJSON = mapper.createObjectNode();

    try {
      // getting cluster's Regions
      responseJSON.put("clusterName", cluster.getServerName());
      var regionsData = getRegionsJson(cluster);
      responseJSON.set("clusterRegions", regionsData);
      responseJSON.put("connectedFlag", cluster.isConnectedFlag());
      responseJSON.put("connectedErrorMsg", cluster.getConnectionErrorMsg());
    } catch (Exception e) {
      logger.debug("Exception Occurred", e);
    }

    // Send json response
    response.getOutputStream().write(responseJSON.toString().getBytes());
  }

  /**
   * This method creates json for list of cluster regions
   *
   * @return ArrayNode JSON array
   */
  private ArrayNode getRegionsJson(Cluster cluster) {

    var clusterRegions = cluster.getClusterRegions().values();
    var regionsListJson = mapper.createArrayNode();

    if (!clusterRegions.isEmpty()) {
      for (var region : clusterRegions) {
        var regionJSON = mapper.createObjectNode();
        regionJSON.put("name", region.getName());
        regionJSON.put("fullPath", region.getFullPath());
        regionJSON.put("regionType", region.getRegionType());

        regionJSON.put("isPartition", region.getRegionType().contains("PARTITION"));

        regionJSON.put("memberCount", region.getMemberCount());
        var regionsMembers = region.getMemberName();
        var jsonRegionMembers = mapper.createArrayNode();

        for (var regionsMember : regionsMembers) {
          var member = cluster.getMembersHMap().get(regionsMember);
          var jsonMember = mapper.createObjectNode();
          jsonMember.put("key", regionsMember);
          jsonMember.put("id", member.getId());
          jsonMember.put("name", member.getName());

          jsonRegionMembers.add(jsonMember);
        }

        regionJSON.set("members", jsonRegionMembers);
        regionsListJson.add(regionJSON);
      }
    }
    return regionsListJson;
  }

  @RequestMapping(value = "/dataBrowserQuery", method = RequestMethod.GET)
  public void dataBrowserQuery(HttpServletRequest request, HttpServletResponse response)
      throws IOException {
    // get query string
    var query = request.getParameter("query");
    var members = request.getParameter("members");
    int limit;

    try {
      limit = Integer.parseInt(request.getParameter("limit"));
    } catch (NumberFormatException e) {
      limit = 0;
      logger.debug(e);
    }

    var queryResult = executeQuery(request, query, members, limit);

    response.getOutputStream().write(queryResult.toString().getBytes());
  }

  @RequestMapping(value = "/dataBrowserQueryHistory", method = RequestMethod.GET)
  public void dataBrowserQueryHistory(HttpServletRequest request, HttpServletResponse response)
      throws IOException {
    var responseJSON = mapper.createObjectNode();

    try {
      // get cluster object
      var cluster = repository.getCluster();
      var userName = request.getUserPrincipal().getName();

      // get query string
      var action = request.getParameter(QUERYSTRING_PARAM_ACTION);
      if (StringUtils.isBlank(action)) {
        action = ACTION_VIEW;
      }

      if (action.equalsIgnoreCase(ACTION_DELETE)) {
        var queryId = request.getParameter(QUERYSTRING_PARAM_QUERYID);
        if (StringUtils.isNotBlank(queryId)) {

          var deleteStatus = cluster.deleteQueryById(userName, queryId);
          if (deleteStatus) {
            responseJSON.put("status", STATUS_REPSONSE_SUCCESS);
          } else {
            responseJSON.put("status", STATUS_REPSONSE_FAIL);
            responseJSON.put("error", ERROR_REPSONSE_QUERYNOTFOUND);
          }
        } else {
          responseJSON.put("status", STATUS_REPSONSE_FAIL);
          responseJSON.put("error", ERROR_REPSONSE_QUERYIDMISSING);
        }
      }

      // Get list of past executed queries
      var queryResult = cluster.getQueryHistoryByUserId(userName);
      responseJSON.set("queryHistory", queryResult);
    } catch (Exception e) {
      logger.debug("Exception Occurred : ", e);
    }
    response.getOutputStream().write(responseJSON.toString().getBytes());


  }


  @RequestMapping(value = "/dataBrowserExport", method = RequestMethod.GET)
  public void dataBrowserExport(HttpServletRequest request, HttpServletResponse response)
      throws IOException {
    // get query string
    var query = request.getParameter("query");
    var members = request.getParameter("members");
    int limit;

    try {
      limit = Integer.parseInt(request.getParameter("limit"));
    } catch (NumberFormatException e) {
      limit = 0;
      logger.debug(e);
    }

    var queryResult = executeQuery(request, query, members, limit);

    response.setContentType("application/json");
    response.setHeader("Content-Disposition", "attachment; filename=results.json");
    response.getOutputStream().write(queryResult.toString().getBytes());
  }

  @RequestMapping(value = "/getQueryStatisticsGridModel", method = RequestMethod.GET)
  public void getQueryStatisticsGridModel(HttpServletRequest request,
      HttpServletResponse response) {

    var responseJSON = mapper.createObjectNode();
    // get cluster object
    var cluster = repository.getCluster();
    var userName = request.getUserPrincipal().getName();

    try {
      var arrColNames = Cluster.Statement.getGridColumnNames();
      var arrColAttribs = Cluster.Statement.getGridColumnAttributes();
      var arrColWidths = Cluster.Statement.getGridColumnWidths();

      var colNamesList = mapper.createArrayNode();
      for (var arrColName : arrColNames) {
        colNamesList.add(arrColName);
      }

      var colModelList = mapper.createArrayNode();
      for (var i = 0; i < arrColAttribs.length; ++i) {
        var columnJSON = mapper.createObjectNode();
        columnJSON.put("name", arrColAttribs[i]);
        columnJSON.put("index", arrColAttribs[i]);
        columnJSON.put("width", arrColWidths[i]);
        columnJSON.put("sortable", "true");
        columnJSON.put("sorttype", ((i == 0) ? "String" : "integer"));
        colModelList.add(columnJSON);
      }

      responseJSON.set("columnNames", colNamesList);
      responseJSON.set("columnModels", colModelList);
      responseJSON.put("clusterName", cluster.getServerName());
      responseJSON.put("userName", userName);

      // Send json response
      response.getOutputStream().write(responseJSON.toString().getBytes());
    } catch (Exception e) {
      logger.debug("Exception Occurred : ", e);
    }
  }

  private ObjectNode executeQuery(HttpServletRequest request, String query, String members,
      int limit) {
    var queryResult = mapper.createObjectNode();
    try {

      if (StringUtils.isNotBlank(query)) {
        // get cluster object
        var cluster = repository.getCluster();
        var userName = request.getUserPrincipal().getName();

        // Add html escaped query to history
        var escapedQuery = StringEscapeUtils.escapeHtml4(query);
        cluster.addQueryInHistory(escapedQuery, userName);

        // Call execute query method
        queryResult = cluster.executeQuery(query, members, limit);
      }
    } catch (Exception e) {
      logger.debug("Exception Occurred : ", e);
    }
    return queryResult;
  }
}
