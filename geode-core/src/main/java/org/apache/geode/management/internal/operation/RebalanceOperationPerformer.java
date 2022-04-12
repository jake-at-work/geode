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
package org.apache.geode.management.internal.operation;

import static org.apache.geode.cache.Region.SEPARATOR;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.collections.CollectionUtils;
import org.jetbrains.annotations.NotNull;

import org.apache.geode.annotations.Experimental;
import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.serialization.KnownVersion;
import org.apache.geode.management.ManagementService;
import org.apache.geode.management.internal.MBeanJMXAdapter;
import org.apache.geode.management.internal.exceptions.NoMembersException;
import org.apache.geode.management.internal.functions.RebalanceFunction;
import org.apache.geode.management.internal.i18n.CliStrings;
import org.apache.geode.management.internal.util.ManagementUtils;
import org.apache.geode.management.operation.RebalanceOperation;
import org.apache.geode.management.runtime.RebalanceRegionResult;
import org.apache.geode.management.runtime.RebalanceResult;

@Experimental
public class RebalanceOperationPerformer
    implements OperationPerformer<RebalanceOperation, RebalanceResult> {

  public RebalanceResult perform(Cache cache, RebalanceOperation parameters) {
    var includeRegions = parameters.getIncludeRegions();
    var excludeRegions = parameters.getExcludeRegions();
    var simulate = parameters.isSimulate();

    var result = new RebalanceResultImpl();
    result.setSuccess(false);

    if (!includeRegions.isEmpty()) {

      List<RebalanceRegionResult> rebalanceRegionResults = new ArrayList<>();

      NoMembersException latestNoMembersException = null;

      for (var regionName : includeRegions) {

        // Handle exclude / include regions
        RebalanceRegionResult rebalanceResult;
        try {
          rebalanceResult = performRebalance(cache, regionName, simulate);
        } catch (NoMembersException ex) {
          latestNoMembersException = ex;
          continue;
        } catch (Exception e) {
          result.setStatusMessage(e.getMessage());
          continue;
        }
        rebalanceRegionResults.add(rebalanceResult);
        result.setSuccess(true);
      }

      if (latestNoMembersException != null && !result.getSuccess()) {
        result.setStatusMessage(latestNoMembersException.getMessage());
      } else {
        result.setRebalanceSummary(rebalanceRegionResults);
      }

      return result;
    } else {
      result =
          (RebalanceResultImpl) executeRebalanceOnDS(ManagementService.getManagementService(cache),
              (InternalCache) cache,
              String.valueOf(simulate), excludeRegions, new FunctionExecutor());
    }

    return result;
  }

  private RebalanceRegionResult performRebalance(Cache cache, String regionName,
      boolean simulate)
      throws InterruptedException {
    // To be removed after region Name specification with "/" is fixed
    regionName = regionName.startsWith(SEPARATOR) ? regionName : (SEPARATOR + regionName);
    Region region = cache.getRegion(regionName);

    if (region == null) {
      var member = getAssociatedMembers(regionName, (InternalCache) cache);

      if (member == null) {
        throw new NoMembersException(MessageFormat.format(
            CliStrings.REBALANCE__MSG__NO_ASSOCIATED_DISTRIBUTED_MEMBER, regionName));
      }

      var functionArgs = new Object[3];
      functionArgs[0] = simulate ? "true" : "false";
      Set<String> setRegionName = new HashSet<>();
      setRegionName.add(regionName);
      functionArgs[1] = setRegionName;

      functionArgs[2] = null;
      var function = getRebalanceFunction((InternalDistributedMember) member);
      List<String> resultList = null;
      try {
        resultList = (List<String>) ManagementUtils
            .executeFunction(function, functionArgs, Collections.singleton(member))
            .getResult();
      } catch (Exception ignored) {

      }

      RebalanceRegionResult result = new RebalanceRegionResultImpl();
      if (resultList != null && !resultList.isEmpty()) {
        var rstList = Arrays.asList(resultList.get(0).split(","));
        result = toRebalanceRegionResult(rstList);
      }

      return result;
    } else {

      var manager = cache.getResourceManager();
      var rbFactory = manager.createRebalanceFactory();
      Set<String> includeRegionSet = new HashSet<>();
      includeRegionSet.add(regionName);
      rbFactory.includeRegions(includeRegionSet);

      org.apache.geode.cache.control.RebalanceOperation op;
      if (simulate) {
        op = manager.createRebalanceFactory().simulate();
      } else {
        op = manager.createRebalanceFactory().start();
      }
      // Wait until the rebalance is complete and then get the results
      var results = op.getResults();

      // translate to the return type we want
      var result = new RebalanceRegionResultImpl();
      result.setRegionName(regionName);
      result.setBucketCreateBytes(results.getTotalBucketCreateBytes());
      result.setBucketCreateTimeInMilliseconds(results.getTotalBucketCreateTime());
      result.setBucketCreatesCompleted(results.getTotalBucketCreatesCompleted());
      result.setBucketTransferBytes(results.getTotalBucketTransferBytes());
      result.setBucketTransferTimeInMilliseconds(results.getTotalBucketTransferTime());
      result.setBucketTransfersCompleted(results.getTotalBucketTransfersCompleted());
      result.setPrimaryTransferTimeInMilliseconds(results.getTotalPrimaryTransferTime());
      result.setPrimaryTransfersCompleted(results.getTotalPrimaryTransfersCompleted());
      result.setNumOfMembers(results.getTotalMembersExecutedOn());
      result.setTimeInMilliseconds(results.getTotalTime());

      return result;
    }
  }

  public static DistributedMember getAssociatedMembers(String region, InternalCache cache) {
    var bean =
        ManagementService.getManagementService(cache).getDistributedRegionMXBean(region);

    DistributedMember member = null;

    if (bean == null) {
      return null;
    }

    var membersName = bean.getMembers();
    var dsMembers = ManagementUtils.getAllMembers(cache);
    var it = dsMembers.iterator();

    var matchFound = false;

    if (membersName.length > 1) {
      while (it.hasNext() && !matchFound) {
        var dsMember = it.next();
        for (var memberName : membersName) {
          if (MBeanJMXAdapter.getMemberNameOrUniqueId(dsMember).equals(memberName)) {
            member = dsMember;
            matchFound = true;
            break;
          }
        }
      }
    }
    return member;
  }

  public static List<MemberPRInfo> getMemberRegionList(ManagementService managementService,
      InternalCache cache,
      List<String> listExcludedRegion) {
    List<MemberPRInfo> listMemberPRInfo = new ArrayList<>();
    var listDSRegions =
        managementService.getDistributedSystemMXBean().listAllRegionPaths();
    var dsMembers = ManagementUtils.getAllMembers(cache);

    for (var regionName : listDSRegions) {
      // check for excluded regions
      var excludedRegionMatch = false;
      if (listExcludedRegion != null) {
        for (var aListExcludedRegion : listExcludedRegion) {
          // this is needed since region name may start with / or without it
          var excludedRegion = aListExcludedRegion.trim();
          if (regionName.startsWith(SEPARATOR) && !excludedRegion.startsWith(SEPARATOR)) {
            excludedRegion = SEPARATOR + excludedRegion;
          }

          if (excludedRegion.startsWith(SEPARATOR) && !regionName.startsWith(SEPARATOR)) {
            regionName = SEPARATOR + regionName;
          }

          if (excludedRegion.equals(regionName)) {
            excludedRegionMatch = true;
            break;
          }
        }
      }

      if (excludedRegionMatch) {
        // ignore this region
        continue;
      }

      if (!regionName.startsWith(SEPARATOR)) {
        regionName = SEPARATOR + regionName;
      }
      // remove this prefix /
      var bean = managementService.getDistributedRegionMXBean(regionName);

      if (bean != null) {
        if (bean.getRegionType().equals(DataPolicy.PARTITION.toString())
            || bean.getRegionType().equals(DataPolicy.PERSISTENT_PARTITION.toString())) {

          var memberNames = bean.getMembers();
          for (var dsMember : dsMembers) {
            for (var memberName : memberNames) {
              if (MBeanJMXAdapter.getMemberNameOrUniqueId(dsMember).equals(memberName)) {
                var memberAndItsPRRegions = new MemberPRInfo();
                memberAndItsPRRegions.region = regionName;
                memberAndItsPRRegions.dsMemberList.add(dsMember);
                if (listMemberPRInfo.contains(memberAndItsPRRegions)) {
                  // add member for appropriate region
                  var index = listMemberPRInfo.indexOf(memberAndItsPRRegions);
                  var listMember = listMemberPRInfo.get(index);
                  listMember.dsMemberList.add(dsMember);
                } else {
                  listMemberPRInfo.add(memberAndItsPRRegions);
                }
                break;
              }
            }
          }
        }
      }
    }

    return listMemberPRInfo;
  }

  private boolean checkMemberPresence(InternalCache cache, DistributedMember dsMember) {
    // check if member's presence just before executing function
    // this is to avoid running a function on departed members #47248
    var dsMemberList = ManagementUtils.getAllNormalMembers(cache);
    return dsMemberList.contains(dsMember);
  }

  private String listOfAllMembers(List<DistributedMember> dsMemberList) {
    var listMembersId = new StringBuilder();
    for (var j = 0; j < dsMemberList.size() - 1; j++) {
      listMembersId.append(dsMemberList.get(j).getId());
      listMembersId.append(" ; ");
    }
    return listMembersId.toString();
  }

  private boolean checkResultList(List<String> errors, List<Object> resultList,
      DistributedMember member) {
    var toContinueForOtherMembers = false;
    if (CollectionUtils.isNotEmpty(resultList)) {
      for (var object : resultList) {
        if (object instanceof Exception) {
          errors.add(
              MessageFormat.format(CliStrings.REBALANCE__MSG__NO_EXECUTION, member.getId()) + ": " +
                  ((Exception) object).getMessage());

          toContinueForOtherMembers = true;
          break;
        } else if (object instanceof Throwable) {
          errors.add(
              MessageFormat.format(CliStrings.REBALANCE__MSG__NO_EXECUTION, member.getId()) + ": " +
                  ((Throwable) object).getMessage());

          toContinueForOtherMembers = true;
          break;
        }
      }
    } else {
      toContinueForOtherMembers = true;
    }
    return toContinueForOtherMembers;
  }

  /**
   * This class was introduced so that it can be mocked
   * to all executeRebalanceOnDS to be unit tested
   */
  @VisibleForTesting
  static class FunctionExecutor {
    public List<Object> execute(Function rebalanceFunction, Object[] functionArgs,
        DistributedMember dsMember) {
      return (List<Object>) ManagementUtils.executeFunction(rebalanceFunction,
          functionArgs, Collections.singleton(dsMember)).getResult();
    }
  }

  @VisibleForTesting
  RebalanceResult executeRebalanceOnDS(ManagementService managementService,
      InternalCache cache, String simulate,
      List<String> excludeRegionsList, FunctionExecutor functionExecutor) {
    var rebalanceResult = new RebalanceResultImpl();
    rebalanceResult.setSuccess(false);
    List<String> errors = new ArrayList<>();

    var listMemberRegion =
        getMemberRegionList(managementService, cache, excludeRegionsList);

    if (listMemberRegion.isEmpty()) {
      rebalanceResult.setStatusMessage(CliStrings.REBALANCE__MSG__NO_REBALANCING_REGIONS_ON_DS);
      rebalanceResult.setSuccess(true);
      return rebalanceResult;
    }

    var iterator = listMemberRegion.iterator();
    var flagToContinueWithRebalance = false;

    // check if list has some members that can be rebalanced
    while (iterator.hasNext()) {
      if (iterator.next().dsMemberList.size() > 1) {
        flagToContinueWithRebalance = true;
        break;
      }
    }

    if (!flagToContinueWithRebalance) {
      rebalanceResult.setStatusMessage(CliStrings.REBALANCE__MSG__NO_REBALANCING_REGIONS_ON_DS);
      rebalanceResult.setSuccess(true);
      return rebalanceResult;
    }

    List<RebalanceRegionResult> rebalanceRegionResults = new ArrayList<>();
    for (var memberPR : listMemberRegion) {
      try {
        // check if there are more than one members associated with region for rebalancing
        if (memberPR.dsMemberList.size() > 1) {
          for (var i = 0; i < memberPR.dsMemberList.size(); i++) {
            var dsMember = memberPR.dsMemberList.get(i);
            var rebalanceFunction = getRebalanceFunction(
                (InternalDistributedMember) dsMember);
            var functionArgs = new Object[3];
            functionArgs[0] = simulate;
            Set<String> regionSet = new HashSet<>();

            regionSet.add(memberPR.region);
            functionArgs[1] = regionSet;

            Set<String> excludeRegionSet = new HashSet<>();
            functionArgs[2] = excludeRegionSet;

            List<Object> resultList = new ArrayList<>();

            try {
              if (checkMemberPresence(cache, dsMember)) {
                resultList = functionExecutor.execute(rebalanceFunction, functionArgs, dsMember);
                if (checkResultList(errors, resultList, dsMember)) {
                  continue;
                }

                var rstList = Arrays.asList(((String) resultList.get(0)).split(","));
                rebalanceRegionResults.add(toRebalanceRegionResult(rstList));
                rebalanceResult.setSuccess(true);

                // Rebalancing for region is done so break and continue with other region
                break;
              } else {
                if (i == memberPR.dsMemberList.size() - 1) {
                  // The last member hosting this region departed so no need to rebalance it.
                  // So act as if we never tried to rebalance this region.
                  // Break to get out of this inner loop and try the next region (if any).
                  break;
                } else {
                  continue;
                }
              }
            } catch (Exception ex) {
              if (i == memberPR.dsMemberList.size() - 1) {
                errors.add(
                    MessageFormat.format(
                        CliStrings.REBALANCE__MSG__NO_EXECUTION_FOR_REGION_0_ON_MEMBERS_1,
                        memberPR.region, listOfAllMembers(memberPR.dsMemberList)) + ", " +
                        CliStrings.REBALANCE__MSG__REASON + ex.getMessage());
              } else {
                continue;
              }
            }

            if (checkResultList(errors, resultList, dsMember)) {
              continue;
            }

            var rstList = Arrays.asList(((String) resultList.get(0)).split(","));
            rebalanceRegionResults.add(toRebalanceRegionResult(rstList));
            rebalanceResult.setSuccess(true);
          }
        }
      } catch (NoMembersException e) {
        rebalanceResult.setStatusMessage(e.getMessage());
        rebalanceResult.setRebalanceSummary(rebalanceRegionResults);
        return rebalanceResult;
      }
    }
    rebalanceResult.setRebalanceSummary(rebalanceRegionResults);
    if (rebalanceRegionResults.isEmpty()) {
      rebalanceResult.setSuccess(false);
    }
    return rebalanceResult;
  }

  @NotNull
  private Function getRebalanceFunction(InternalDistributedMember dsMember) {
    Function rebalanceFunction;
    if (dsMember.getVersion()
        .isOlderThan(KnownVersion.GEODE_1_12_0)) {
      rebalanceFunction =
          new org.apache.geode.management.internal.cli.functions.RebalanceFunction();
    } else {
      rebalanceFunction = new RebalanceFunction();
    }
    return rebalanceFunction;
  }

  private static RebalanceRegionResult toRebalanceRegionResult(List<String> rstList) {
    var result = new RebalanceRegionResultImpl();
    result.setBucketCreateBytes(Long.parseLong(rstList.get(0)));
    result.setBucketCreateTimeInMilliseconds(Long.parseLong(rstList.get(1)));
    result.setBucketCreatesCompleted(Integer.parseInt(rstList.get(2)));
    result.setBucketTransferBytes(Long.parseLong(rstList.get(3)));
    result.setBucketTransferTimeInMilliseconds(Long.parseLong(rstList.get(4)));
    result.setBucketTransfersCompleted(Integer.parseInt(rstList.get(5)));
    result.setPrimaryTransferTimeInMilliseconds(Long.parseLong(rstList.get(6)));
    result.setPrimaryTransfersCompleted(Integer.parseInt(rstList.get(7)));
    result.setTimeInMilliseconds(Long.parseLong(rstList.get(8)));
    if (rstList.size() < 11) {
      result.setNumOfMembers(-1);
      result.setRegionName(rstList.get(9));
    } else {
      result.setNumOfMembers(Integer.parseInt(rstList.get(9)));
      result.setRegionName(rstList.get(10));
    }


    return result;
  }

  public static class MemberPRInfo {
    public List<DistributedMember> dsMemberList;
    public String region;

    public MemberPRInfo() {
      region = "";
      dsMemberList = new ArrayList<>();
    }

    @Override
    public boolean equals(Object o2) {
      if (o2 instanceof MemberPRInfo) {
        return region.equals(((MemberPRInfo) o2).region);
      } else {
        return false;
      }
    }

    @Override
    public int hashCode() {
      return region.hashCode();
    }
  }
}
