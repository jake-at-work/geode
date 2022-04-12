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
package org.apache.geode.management.internal.cli.functions;

import java.io.File;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.snapshot.SnapshotOptions.SnapshotFormat;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.management.cli.CliFunction;
import org.apache.geode.management.internal.functions.CliFunctionResult;
import org.apache.geode.management.internal.i18n.CliStrings;

/****
 * Function which carries out the import of a region to a file on a member. Uses the
 * RegionSnapshotService to import the data
 *
 */
public class ImportDataFunction extends CliFunction<Object[]> {
  private static final long serialVersionUID = 1L;

  private static final String ID =
      "org.apache.geode.management.internal.cli.functions.ImportDataFunction";

  @Override
  public String getId() {
    return ID;
  }

  @Override
  public CliFunctionResult executeFunction(FunctionContext<Object[]> context) throws Exception {
    final var args = context.getArguments();
    if (args.length < 4) {
      throw new IllegalStateException(
          "Arguments length does not match required length. Import command may have been sent from incompatible older version");
    }
    final var regionName = (String) args[0];
    final var importFileName = (String) args[1];
    final var invokeCallbacks = (boolean) args[2];
    final var parallel = (boolean) args[3];

    CliFunctionResult result;
    final Cache cache =
        ((InternalCache) context.getCache()).getCacheForProcessingClientRequests();
    final var region = cache.getRegion(regionName);
    final var hostName = cache.getDistributedSystem().getDistributedMember().getHost();
    if (region != null) {
      var snapshotService = region.getSnapshotService();
      var options = snapshotService.createOptions();
      options.invokeCallbacks(invokeCallbacks);
      options.setParallelMode(parallel);
      var importFile = new File(importFileName);
      snapshotService.load(new File(importFileName), SnapshotFormat.GEODE, options);
      var successMessage = CliStrings.format(CliStrings.IMPORT_DATA__SUCCESS__MESSAGE,
          importFile.getCanonicalPath(), hostName, regionName);
      result = new CliFunctionResult(context.getMemberName(), CliFunctionResult.StatusState.OK,
          successMessage);
    } else {
      result = new CliFunctionResult(context.getMemberName(), CliFunctionResult.StatusState.ERROR,
          CliStrings.format(CliStrings.REGION_NOT_FOUND, regionName));
    }

    return result;
  }

}
