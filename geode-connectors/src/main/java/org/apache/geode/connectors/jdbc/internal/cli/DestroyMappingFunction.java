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
package org.apache.geode.connectors.jdbc.internal.cli;


import org.apache.geode.annotations.Experimental;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.asyncqueue.internal.InternalAsyncEventQueue;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.connectors.jdbc.JdbcLoader;
import org.apache.geode.connectors.jdbc.JdbcWriter;
import org.apache.geode.management.cli.CliFunction;
import org.apache.geode.management.internal.functions.CliFunctionResult;
import org.apache.geode.management.internal.functions.CliFunctionResult.StatusState;


@Experimental
public class DestroyMappingFunction extends CliFunction<String> {

  @Override
  public CliFunctionResult executeFunction(FunctionContext<String> context) {
    var cache = context.getCache();
    var service = FunctionContextArgumentProvider.getJdbcConnectorService(context);
    var regionName = context.getArguments();
    var member = context.getMemberName();
    var mapping = service.getMappingForRegion(regionName);
    if (mapping != null) {
      cleanupRegionAndQueue(cache, regionName);
      service.destroyRegionMapping(regionName);
      var message = "Destroyed JDBC mapping for region " + regionName + " on " + member;
      return new CliFunctionResult(member, StatusState.OK, message);
    } else {
      var message = "JDBC mapping for region \"" + regionName + "\" not found";
      return new CliFunctionResult(member, StatusState.ERROR, message);
    }
  }

  private void cleanupRegionAndQueue(Cache cache, String regionName) {
    var queueName = MappingCommandUtils.createAsyncEventQueueName(regionName);

    Region<?, ?> region = cache.getRegion(regionName);
    if (region != null) {
      var loader = region.getAttributes().getCacheLoader();
      if (loader instanceof JdbcLoader) {
        region.getAttributesMutator().setCacheLoader(null);
      }
      var writer = region.getAttributes().getCacheWriter();
      if (writer instanceof JdbcWriter) {
        region.getAttributesMutator().setCacheWriter(null);
      }
      var queueIds = region.getAttributes().getAsyncEventQueueIds();
      if (queueIds.contains(queueName)) {
        region.getAttributesMutator().removeAsyncEventQueueId(queueName);
      }
    }

    var queue = (InternalAsyncEventQueue) cache.getAsyncEventQueue(queueName);
    if (queue != null) {
      queue.stop();
      queue.destroy();
    }
  }
}
