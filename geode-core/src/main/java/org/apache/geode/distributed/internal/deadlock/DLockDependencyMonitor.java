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
package org.apache.geode.distributed.internal.deadlock;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.apache.geode.annotations.Immutable;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.locks.DLockService;

/**
 * Report dependencies on dlocks that exist in theis VM.
 *
 *
 */
public class DLockDependencyMonitor implements DependencyMonitor {
  @Immutable
  static final DLockDependencyMonitor INSTANCE;

  static {
    INSTANCE = new DLockDependencyMonitor();
  }



  @Override
  public Set<Dependency<Thread, Serializable>> getBlockedThreads(Thread[] allThreads) {
    Set<Dependency<Thread, Serializable>> results = new HashSet<>();

    DLockService.dumpAllServices(); // for investigating bug #43496

    var services = DLockService.snapshotAllServices();
    for (var entry : services.entrySet()) {
      var serviceName = entry.getKey();
      var service = entry.getValue();

      var blockedThreadLocal = service.getBlockedOn();
      for (var thread : allThreads) {
        var lockName = blockedThreadLocal.get(thread);
        if (lockName != null) {
          results.add(new Dependency<>(thread,
              new LockId(serviceName, (Serializable) lockName)));
        }
      }
    }

    return results;
  }

  @Override
  public Set<Dependency<Serializable, Thread>> getHeldResources(Thread[] allThreads) {

    var ds = InternalDistributedSystem.getAnyInstance();
    if (ds == null) {
      return Collections.emptySet();
    }
    Set<Dependency<Serializable, Thread>> results = new HashSet<>();

    var services = DLockService.snapshotAllServices();
    for (var entry : services.entrySet()) {
      var serviceName = entry.getKey();
      var service = entry.getValue();
      var tokens = service.snapshotService();
      for (var tokenEntry : tokens.entrySet()) {
        var tokenName = tokenEntry.getKey();
        var token = tokenEntry.getValue();
        synchronized (token) {
          var holdingThread = token.getThread();
          if (holdingThread != null) {
            results.add(
                new Dependency(new LockId(serviceName, (Serializable) tokenName), holdingThread));
          }
        }
      }
    }

    return results;
  }

  private static class LockId implements Serializable {
    private final String serviceName;
    private final Serializable tokenName;

    public LockId(String serviceName, Serializable tokenName) {
      this.serviceName = serviceName;
      this.tokenName = tokenName;
    }

    @Override
    public int hashCode() {
      final var prime = 31;
      var result = 1;
      result = prime * result + ((serviceName == null) ? 0 : serviceName.hashCode());
      result = prime * result + ((tokenName == null) ? 0 : tokenName.hashCode());
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null) {
        return false;
      }
      if (!(obj instanceof LockId)) {
        return false;
      }
      var other = (LockId) obj;
      if (serviceName == null) {
        if (other.serviceName != null) {
          return false;
        }
      } else if (!serviceName.equals(other.serviceName)) {
        return false;
      }
      if (tokenName == null) {
        return other.tokenName == null;
      } else
        return tokenName.equals(other.tokenName);
    }

    @Override
    public String toString() {
      return "DLock(" + serviceName + ", " + tokenName + ")";
    }
  }

}
