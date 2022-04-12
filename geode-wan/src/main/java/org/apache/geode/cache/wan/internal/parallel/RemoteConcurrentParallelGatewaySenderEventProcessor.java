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
package org.apache.geode.cache.wan.internal.parallel;


import java.util.Set;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.wan.internal.GatewaySenderEventRemoteDispatcher;
import org.apache.geode.internal.cache.wan.AbstractGatewaySender;
import org.apache.geode.internal.cache.wan.parallel.ConcurrentParallelGatewaySenderEventProcessor;
import org.apache.geode.internal.monitoring.ThreadsMonitoring;

/**
 * Remote version of GatewaySenderEvent Processor
 *
 */
public class RemoteConcurrentParallelGatewaySenderEventProcessor
    extends ConcurrentParallelGatewaySenderEventProcessor {

  public RemoteConcurrentParallelGatewaySenderEventProcessor(AbstractGatewaySender sender,
      ThreadsMonitoring tMonitoring, boolean cleanQueues) {
    super(sender, tMonitoring, cleanQueues);
  }

  @Override
  protected void createProcessors(int dispatcherThreads, Set<Region<?, ?>> targetRs,
      boolean cleanQueues) {
    processors = new RemoteParallelGatewaySenderEventProcessor[sender.getDispatcherThreads()];
    if (logger.isDebugEnabled()) {
      logger.debug("Creating GatewaySenderEventProcessor");
    }
    for (var i = 0; i < sender.getDispatcherThreads(); i++) {
      processors[i] = new RemoteParallelGatewaySenderEventProcessor(sender, targetRs, i,
          sender.getDispatcherThreads(), getThreadMonitorObj(), cleanQueues);
    }
  }

  @Override
  protected void rebalance() {
    var statistics = sender.getStatistics();
    var startTime = statistics.startLoadBalance();
    try {
      for (var parallelProcessor : processors) {
        var remoteDispatcher =
            (GatewaySenderEventRemoteDispatcher) parallelProcessor.getDispatcher();
        if (remoteDispatcher.isConnectedToRemote()) {
          remoteDispatcher.stopAckReaderThread();
          remoteDispatcher.destroyConnection();
        }
      }
    } finally {
      statistics.endLoadBalance(startTime);
    }
  }

  private ThreadsMonitoring getThreadMonitorObj() {
    var distributionManager = sender.getDistributionManager();
    if (distributionManager != null) {
      return distributionManager.getThreadMonitoring();
    } else {
      return null;
    }
  }
}
