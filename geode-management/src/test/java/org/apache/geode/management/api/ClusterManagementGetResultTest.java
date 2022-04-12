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

package org.apache.geode.management.api;

import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.junit.Test;

import org.apache.geode.management.configuration.Region;
import org.apache.geode.management.runtime.RuntimeRegionInfo;
import org.apache.geode.util.internal.GeodeJsonMapper;

public class ClusterManagementGetResultTest {

  @Test
  public void serialization() throws JsonProcessingException {
    var region = createRegion();
    var runtimeRegionInfo = new RuntimeRegionInfo();

    var entityGroupInfo =
        new EntityGroupInfo<Region, RuntimeRegionInfo>(region, singletonList(runtimeRegionInfo));

    var configurationinfo =
        new EntityInfo<Region, RuntimeRegionInfo>("my.element", singletonList(entityGroupInfo));

    var original =
        new ClusterManagementGetResult<Region, RuntimeRegionInfo>(configurationinfo);

    var mapper = GeodeJsonMapper.getMapper();
    var json = mapper.writeValueAsString(original);

    assertThat(json).containsOnlyOnce("result").containsOnlyOnce("statusCode");

    System.out.println(json);

    @SuppressWarnings("unchecked")
    ClusterManagementGetResult<Region, RuntimeRegionInfo> deserialized =
        mapper.readValue(json, ClusterManagementGetResult.class);

    System.out.println(mapper.writeValueAsString(deserialized));

    assertThat(deserialized).isEqualTo(original);
  }

  public static Region createRegion() {
    var region = new Region();
    region.setName("my.region");
    region.setGroup("my.group");
    return region;
  }
}
