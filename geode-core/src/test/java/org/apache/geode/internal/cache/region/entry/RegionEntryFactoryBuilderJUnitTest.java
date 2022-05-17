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

import static org.junit.Assert.assertEquals;

import junitparams.Parameters;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.geode.test.junit.runners.GeodeParamsRunner;


@RunWith(GeodeParamsRunner.class)
public class RegionEntryFactoryBuilderJUnitTest {

  private RegionEntryFactoryBuilder regionEntryFactoryBuilder;

  @Before
  public void setup() {
    regionEntryFactoryBuilder = new RegionEntryFactoryBuilder();
  }

  /**
   * This method will test that the correct RegionEntryFactory is created dependent on the 5
   * conditionals: enableStats, enableLRU, enableDisk, enableVersion
   */
  @Test
  @Parameters({"VMThinRegionEntryHeapFactory,false,false,false,false",
      "VersionedThinRegionEntryHeapFactory,false,false,false,true",
      "VMThinDiskRegionEntryHeapFactory,false,false,true,false",
      "VersionedThinDiskRegionEntryHeapFactory,false,false,true,true",
      "VMThinLRURegionEntryHeapFactory,false,true,false,false",
      "VersionedThinLRURegionEntryHeapFactory,false,true,false,true",
      "VMThinDiskLRURegionEntryHeapFactory,false,true,true,false",
      "VersionedThinDiskLRURegionEntryHeapFactory,false,true,true,true",
      "VMStatsRegionEntryHeapFactory,true,false,false,false",
      "VersionedStatsRegionEntryHeapFactory,true,false,false,true",
      "VMStatsDiskRegionEntryHeapFactory,true,false,true,false",
      "VersionedStatsDiskRegionEntryHeapFactory,true,false,true,true",
      "VMStatsLRURegionEntryHeapFactory,true,true,false,false",
      "VersionedStatsLRURegionEntryHeapFactory,true,true,false,true",
      "VMStatsDiskLRURegionEntryHeapFactory,true,true,true,false",
      "VersionedStatsDiskLRURegionEntryHeapFactory,true,true,true,true"})
  public void testRegionEntryFactoryUnitTest(String factoryName, boolean enableStats,
      boolean enableLRU, boolean enableDisk, boolean enableVersioning) {
    assertEquals(factoryName,
        regionEntryFactoryBuilder
            .create(enableStats, enableLRU, enableDisk, enableVersioning).getClass()
            .getSimpleName());
  }
}
