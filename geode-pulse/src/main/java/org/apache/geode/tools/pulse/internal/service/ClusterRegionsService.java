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

package org.apache.geode.tools.pulse.internal.service;

import static org.apache.geode.tools.pulse.internal.data.PulseConstants.FOUR_PLACE_DECIMAL_FORMAT;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import javax.servlet.http.HttpServletRequest;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

import org.apache.geode.tools.pulse.internal.data.Cluster;
import org.apache.geode.tools.pulse.internal.data.Repository;

/**
 * Class ClusterRegionsService
 *
 * This class contains implementations of getting Cluster's regions details
 *
 * @since GemFire version 7.5
 */

@Component
@Service("ClusterRegions")
@Scope("singleton")
public class ClusterRegionsService implements PulseService {

  private final ObjectMapper mapper = new ObjectMapper();

  // String constants used for forming a json response
  private static final String ENTRY_SIZE = "entrySize";
  private final Repository repository;

  @Autowired
  public ClusterRegionsService(Repository repository) {
    this.repository = repository;
  }

  // Comparator based upon regions entry count
  private static final Comparator<Cluster.Region> regionEntryCountComparator = (r1, r2) -> {
    var r1Cnt = r1.getSystemRegionEntryCount();
    var r2Cnt = r2.getSystemRegionEntryCount();
    return Long.compare(r1Cnt, r2Cnt);
  };

  @Override
  public ObjectNode execute(final HttpServletRequest request) throws Exception {
    // get cluster object
    var cluster = repository.getCluster();

    // json object to be sent as response
    var responseJSON = mapper.createObjectNode();

    // getting cluster's Regions
    responseJSON.set("regions", getRegionJson(cluster));

    // Send json response
    return responseJSON;
  }

  /**
   * This method is used to get various regions associated with the given cluster and create json
   * for each region fields and returns Array List for all the regions associated with given cluster
   *
   * @return JSONObject Array List
   */
  private ArrayNode getRegionJson(Cluster cluster) {

    Long totalHeapSize = cluster.getTotalHeapSize();
    Long totalDiskUsage = cluster.getTotalBytesOnDisk();

    var clusterRegions = cluster.getClusterRegions();

    List<Cluster.Region> clusterRegionsList = new ArrayList<>(clusterRegions.values());

    clusterRegionsList.sort(regionEntryCountComparator);

    var regionListJson = mapper.createArrayNode();
    for (var reg : clusterRegionsList) {
      var regionJSON = mapper.createObjectNode();

      regionJSON.put("name", reg.getName());
      regionJSON.put("totalMemory", totalHeapSize);
      regionJSON.put("systemRegionEntryCount", reg.getSystemRegionEntryCount());
      regionJSON.put("memberCount", reg.getMemberCount());

      final var regionType = reg.getRegionType();
      regionJSON.put("type", regionType);
      regionJSON.put("getsRate", reg.getGetsRate());
      regionJSON.put("putsRate", reg.getPutsRate());

      var clusterMembersList = cluster.getMembers();

      var memberNameArray = mapper.createArrayNode();
      for (var memberName : reg.getMemberName()) {
        for (var member : clusterMembersList) {
          var name = member.getName();
          name = name.replace(":", "-");
          var id = member.getId();
          id = id.replace(":", "-");

          if ((memberName.equals(id)) || (memberName.equals(name))) {
            var regionMember = mapper.createObjectNode();
            regionMember.put("id", member.getId());
            regionMember.put("name", member.getName());
            memberNameArray.add(regionMember);
            break;
          }
        }
      }

      regionJSON.set("memberNames", memberNameArray);
      regionJSON.put("entryCount", reg.getSystemRegionEntryCount());

      var persistent = reg.getPersistentEnabled();
      if (persistent) {
        regionJSON.put("persistence", VALUE_ON);
      } else {
        regionJSON.put("persistence", VALUE_OFF);
      }

      var isEnableOffHeapMemory = reg.isEnableOffHeapMemory();
      if (isEnableOffHeapMemory) {
        regionJSON.put("isEnableOffHeapMemory", VALUE_ON);
      } else {
        regionJSON.put("isEnableOffHeapMemory", VALUE_OFF);
      }

      var regCompCodec = reg.getCompressionCodec();
      if (StringUtils.isNotBlank(regCompCodec)) {
        regionJSON.put("compressionCodec", reg.getCompressionCodec());
      } else {
        regionJSON.put("compressionCodec", VALUE_NA);
      }

      regionJSON.put("regionPath", reg.getFullPath());
      regionJSON.put("id", reg.getFullPath());

      regionJSON.set("memoryReadsTrend", mapper
          .valueToTree(reg.getRegionStatisticTrend(Cluster.Region.REGION_STAT_GETS_PER_SEC_TREND)));
      regionJSON.set("memoryWritesTrend", mapper
          .valueToTree(reg.getRegionStatisticTrend(Cluster.Region.REGION_STAT_PUTS_PER_SEC_TREND)));
      regionJSON.set("diskReadsTrend", mapper.valueToTree(
          reg.getRegionStatisticTrend(Cluster.Region.REGION_STAT_DISK_READS_PER_SEC_TREND)));
      regionJSON.set("diskWritesTrend", mapper.valueToTree(
          reg.getRegionStatisticTrend(Cluster.Region.REGION_STAT_DISK_WRITES_PER_SEC_TREND)));
      regionJSON.put("emptyNodes", reg.getEmptyNode());
      var entrySize = reg.getEntrySize();
      var entrySizeInMB = FOUR_PLACE_DECIMAL_FORMAT.format(entrySize / (1024f * 1024f));

      if (entrySize < 0) {
        regionJSON.put(ENTRY_SIZE, VALUE_NA);
      } else {
        regionJSON.put(ENTRY_SIZE, entrySizeInMB);
      }
      regionJSON.put("dataUsage", reg.getDiskUsage());
      regionJSON.put("wanEnabled", reg.getWanEnabled());
      regionJSON.put("totalDataUsage", totalDiskUsage);

      regionJSON.put("memoryUsage", entrySizeInMB);

      regionListJson.add(regionJSON);
    }

    return regionListJson;
  }
}
