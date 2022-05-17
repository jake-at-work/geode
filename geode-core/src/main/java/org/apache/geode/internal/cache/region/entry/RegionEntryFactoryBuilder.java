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
package org.apache.geode.internal.cache.region.entry;

import org.apache.geode.internal.cache.RegionEntryFactory;
import org.apache.geode.internal.cache.entries.VMStatsDiskLRURegionEntryHeap;
import org.apache.geode.internal.cache.entries.VMStatsDiskRegionEntryHeap;
import org.apache.geode.internal.cache.entries.VMStatsLRURegionEntryHeap;
import org.apache.geode.internal.cache.entries.VMStatsRegionEntryHeap;
import org.apache.geode.internal.cache.entries.VMThinDiskLRURegionEntryHeap;
import org.apache.geode.internal.cache.entries.VMThinDiskRegionEntryHeap;
import org.apache.geode.internal.cache.entries.VMThinLRURegionEntryHeap;
import org.apache.geode.internal.cache.entries.VMThinRegionEntryHeap;
import org.apache.geode.internal.cache.entries.VersionedStatsDiskLRURegionEntryHeap;
import org.apache.geode.internal.cache.entries.VersionedStatsDiskRegionEntryHeap;
import org.apache.geode.internal.cache.entries.VersionedStatsLRURegionEntryHeap;
import org.apache.geode.internal.cache.entries.VersionedStatsRegionEntryHeap;
import org.apache.geode.internal.cache.entries.VersionedThinDiskLRURegionEntryHeap;
import org.apache.geode.internal.cache.entries.VersionedThinDiskRegionEntryHeap;
import org.apache.geode.internal.cache.entries.VersionedThinLRURegionEntryHeap;
import org.apache.geode.internal.cache.entries.VersionedThinRegionEntryHeap;

public class RegionEntryFactoryBuilder {
  public RegionEntryFactory create(boolean statsEnabled, boolean isLRU, boolean isDisk,
      boolean withVersioning) {
    int bitRepresentation = 0;
    bitRepresentation |= statsEnabled ? 1 : 0;
    bitRepresentation |= isLRU ? 2 : 0;
    bitRepresentation |= isDisk ? 4 : 0;
    bitRepresentation |= withVersioning ? 8 : 0;

    /*
     * The bits represent all options |versioning|disk|lru|stats|
     */
    switch (bitRepresentation) {
      case (0):
        return VMThinRegionEntryHeap.getEntryFactory(); // Bits: 00000
      case (1):
        return VMStatsRegionEntryHeap.getEntryFactory(); // Bits: 00001
      case (2):
        return VMThinLRURegionEntryHeap.getEntryFactory(); // Bits: 00010
      case (3):
        return VMStatsLRURegionEntryHeap.getEntryFactory(); // Bits: 00011
      case (4):
        return VMThinDiskRegionEntryHeap.getEntryFactory(); // Bits: 00100
      case (5):
        return VMStatsDiskRegionEntryHeap.getEntryFactory(); // Bits: 00101
      case (6):
        return VMThinDiskLRURegionEntryHeap.getEntryFactory(); // Bits: 00110
      case (7):
        return VMStatsDiskLRURegionEntryHeap.getEntryFactory(); // Bits: 00111
      case (8):
        return VersionedThinRegionEntryHeap.getEntryFactory(); // Bits: 01000
      case (9):
        return VersionedStatsRegionEntryHeap.getEntryFactory(); // Bits: 01001
      case (10):
        return VersionedThinLRURegionEntryHeap.getEntryFactory(); // Bits: 01010
      case (11):
        return VersionedStatsLRURegionEntryHeap.getEntryFactory(); // Bits: 01011
      case (12):
        return VersionedThinDiskRegionEntryHeap.getEntryFactory(); // Bits: 01100
      case (13):
        return VersionedStatsDiskRegionEntryHeap.getEntryFactory(); // Bits: 01101
      case (14):
        return VersionedThinDiskLRURegionEntryHeap.getEntryFactory(); // Bits: 01110
      case (15):
        return VersionedStatsDiskLRURegionEntryHeap.getEntryFactory(); // Bits: 01111
      default:
        throw new IllegalStateException("unexpected bitRepresentation " + bitRepresentation);
    }
  }
}
