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

package org.apache.geode.management.internal.api;

import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.Logger;

import org.apache.geode.annotations.Experimental;
import org.apache.geode.annotations.Immutable;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.GemFireCache;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.execute.FunctionException;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.distributed.internal.tcpserver.HostAndPort;
import org.apache.geode.distributed.internal.tcpserver.TcpClient;
import org.apache.geode.distributed.internal.tcpserver.TcpSocketFactory;
import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.net.SSLConfigurationFactory;
import org.apache.geode.internal.net.SSLUtil;
import org.apache.geode.internal.net.SocketCreatorFactory;
import org.apache.geode.internal.security.SecurableCommunicationChannel;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.management.api.ConnectionConfig;
import org.apache.geode.management.cluster.client.ClusterManagementServiceBuilder;
import org.apache.geode.management.internal.configuration.messages.ClusterManagementServiceInfo;
import org.apache.geode.management.internal.configuration.messages.ClusterManagementServiceInfoRequest;
import org.apache.geode.management.internal.functions.GetMemberInformationFunction;
import org.apache.geode.management.runtime.MemberInformation;
import org.apache.geode.security.AuthInitialize;

/**
 * Concrete implementation of {@link ConnectionConfig} which can be used to derive most (if not all)
 * of the connection properties from an existing {@link Cache} or {@link ClientCache}.
 *
 * @see ClusterManagementServiceBuilder
 */
@Experimental
public class GeodeConnectionConfig extends ConnectionConfig {

  private static final Logger logger = LogService.getLogger();

  @Immutable
  private static final GetMemberInformationFunction MEMBER_INFORMATION_FUNCTION =
      new GetMemberInformationFunction();

  public GeodeConnectionConfig(GemFireCache cache) {
    var cacheImpl = (GemFireCacheImpl) cache;
    if (cacheImpl.isServer()) {
      setServerCache(cacheImpl);
    } else if (cacheImpl.isClient()) {
      setClientCache(cacheImpl);
    } else {
      throw new IllegalArgumentException("Need a cache instance in order to build the service.");
    }
  }

  private void setServerCache(GemFireCacheImpl cache) {
    var locatorsWithClusterConfig =
        cache.getDistributionManager().getAllHostedLocatorsWithSharedConfiguration()
            .keySet();

    var cmsInfo =
        getClusterManagementServiceInfo(locatorsWithClusterConfig);

    configureBuilder(cache.getSystem().getConfig(), cmsInfo);
  }

  private void setClientCache(ClientCache clientCache) {
    DistributionConfig config;
    ClusterManagementServiceInfo cmsInfo;

    var locators = clientCache.getDefaultPool().getLocators();

    if (locators.size() == 0) {
      throw new IllegalStateException(
          "the client needs to have a client pool connected with a locator.");
    }
    config = ((GemFireCacheImpl) clientCache).getSystem().getConfig();
    SocketCreatorFactory.setDistributionConfig(config);
    var client =
        new TcpClient(SocketCreatorFactory
            .getSocketCreatorForComponent(SecurableCommunicationChannel.LOCATOR),
            InternalDataSerializer.getDSFIDSerializer().getObjectSerializer(),
            InternalDataSerializer.getDSFIDSerializer().getObjectDeserializer(),
            TcpSocketFactory.DEFAULT);
    cmsInfo = null;
    for (var locator : locators) {
      try {
        cmsInfo = (ClusterManagementServiceInfo) client.requestToServer(
            new HostAndPort(locator.getHostString(), locator.getPort()),
            new ClusterManagementServiceInfoRequest(), 1000, true);

        // do not try anymore if we found one that has cms running
        if (cmsInfo.isRunning()) {
          break;
        }
      } catch (Exception e) {
        logger.warn(
            "unable to discover the ClusterManagementService on locator " + locator.toString());
      }
    }

    // if cmsInfo is still null at this point, i.e. we failed to retrieve the cms information from
    // any locator
    if (cmsInfo == null || !cmsInfo.isRunning()) {
      throw new IllegalStateException(
          "Unable to discover a locator that has ClusterManagementService running.");
    }

    configureBuilder(config, cmsInfo);
  }

  private void configureBuilder(DistributionConfig config,
      ClusterManagementServiceInfo cmsInfo) {
    setHost(cmsInfo.getHostName());
    setPort(cmsInfo.getHttpPort());

    // if user didn't pass in a username and the locator requires credentials, use the credentials
    // user used to create the client cache
    if (cmsInfo.isSecured() && getUsername() == null) {
      var securityProps = config.getSecurityProps();
      var username = securityProps.getProperty(AuthInitialize.SECURITY_USERNAME);
      var password = securityProps.getProperty(AuthInitialize.SECURITY_PASSWORD);
      if (StringUtils.isBlank(username)) {
        var message =
            "You will need to set the buildWithHostAddress username and password or specify security-username and security-password in the properties when starting this geode server/client.";
        throw new IllegalStateException(message);
      }
      setUsername(username);
      setPassword(password);
    }

    if (cmsInfo.isSSL()) {
      var sslConfig = SSLConfigurationFactory.getSSLConfigForComponent(
          config, SecurableCommunicationChannel.WEB);
      if (!sslConfig.useDefaultSSLContext() && sslConfig.getTruststore() == null) {
        throw new IllegalStateException(
            "This server/client needs to have ssl-truststore or ssl-use-default-context specified in order to use cluster management service.");
      }

      setSslContext(SSLUtil.createAndConfigureSSLContext(sslConfig, false));
    }
  }

  private ClusterManagementServiceInfo getClusterManagementServiceInfo(
      Set<InternalDistributedMember> locators) {
    var info = new ClusterManagementServiceInfo();
    MemberInformation memberInfo = null;
    for (var locator : locators) {
      try {
        var resultCollector =
            FunctionService.onMember(locator).execute(MEMBER_INFORMATION_FUNCTION);
        var memberInformations =
            (List<MemberInformation>) resultCollector.getResult();
        // Is this even possible?
        if (memberInformations.isEmpty()) {
          continue;
        }
        memberInfo = memberInformations.get(0);
        break;
      } catch (FunctionException e) {
        logger.warn("Unable to execute GetMemberInformationFunction on " + locator.getId());
      }
    }

    if (memberInfo == null) {
      throw new IllegalStateException("Unable to determine ClusterManagementService endpoint");
    }

    info.setHostName(getHostName(memberInfo));
    info.setHttpPort(memberInfo.getHttpServicePort());
    info.setSSL(memberInfo.isWebSSL());
    info.setSecured(memberInfo.isSecured());
    return info;
  }

  private String getHostName(MemberInformation memberInformation) {
    String host;
    if (StringUtils.isNotBlank(memberInformation.getHttpServiceBindAddress())) {
      host = memberInformation.getHttpServiceBindAddress();
    } else if (StringUtils.isNotBlank(memberInformation.getServerBindAddress())) {
      host = memberInformation.getServerBindAddress();
    } else {
      host = memberInformation.getHost();
    }
    return host;
  }
}
