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
package org.apache.geode.internal.cache.execute;

import static java.lang.String.format;
import static org.apache.geode.internal.cache.PartitionedRegionHelper.getHashKey;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.logging.log4j.Logger;

import org.apache.geode.cache.Operation;
import org.apache.geode.cache.execute.FunctionException;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.BucketSetHelper;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.partitioned.BucketId;
import org.apache.geode.logging.internal.log4j.api.LogService;

public class FunctionExecutionNodePruner {
  public static final Logger logger = LogService.getLogger();

  public static HashMap<InternalDistributedMember, int[]> pruneNodes(
      PartitionedRegion pr, Set<BucketId> buckets) {

    final boolean isDebugEnabled = logger.isDebugEnabled();

    if (isDebugEnabled) {
      logger.debug("FunctionExecutionNodePruner: The buckets to be pruned are: {}", buckets);
    }
    HashMap<InternalDistributedMember, int[]> nodeToBucketsMap = new HashMap<>();
    HashMap<InternalDistributedMember, int[]> prunedNodeToBucketsMap = new HashMap<>();

    try {
      for (BucketId bucketId : buckets) {
        Set<InternalDistributedMember> nodes = pr.getRegionAdvisor().getBucketOwners(bucketId);
        if (nodes.isEmpty()) {
          if (isDebugEnabled) {
            logger.debug(
                "FunctionExecutionNodePruner: The buckets owners of the bucket: {} are empty, double check if they are all offline",
                bucketId);
          }
          nodes.add(pr.getOrCreateNodeForBucketRead(bucketId));
        }

        if (isDebugEnabled) {
          logger.debug("FunctionExecutionNodePruner: The buckets owners of the bucket: {} are: {}",
              bucketId, nodes);
        }
        for (InternalDistributedMember node : nodes) {
          if (nodeToBucketsMap.get(node) == null) {
            int[] bucketArray = new int[buckets.size() + 1];
            bucketArray[0] = 0;
            BucketSetHelper.add(bucketArray, bucketId.intValue());
            nodeToBucketsMap.put(node, bucketArray);
          } else {
            int[] bucketArray = nodeToBucketsMap.get(node);
            BucketSetHelper.add(bucketArray, bucketId.intValue());
          }
        }
      }
    } catch (NoSuchElementException ignored) {
    }
    if (isDebugEnabled) {
      logger.debug("FunctionExecutionNodePruner: The node to buckets map is: {}", nodeToBucketsMap);
    }
    int[] currentBucketArray = new int[buckets.size() + 1];
    currentBucketArray[0] = 0;

    /*
     * First Logic: Just implement the Greedy algorithm where you keep adding nodes which has the
     * biggest set of non-currentBucketSet. // Deterministic but it (almost)always chooses minimum
     * no of nodes to execute the function on.
     *
     * Second Logic: Give highest preference to the local node and after that use First Logic. //
     * Local Node gets preference but still it's deterministic for all the execution taking // place
     * at that node which require same set of buckets.
     *
     * Third Logic: After including local node, choose random nodes among the remaining nodes in
     * step until your currentBucketSet has all the required buckets. // No optimization for number
     * of nodes to execute the function
     */


    InternalDistributedMember localNode = pr.getRegionAdvisor().getDistributionManager().getId();
    if (nodeToBucketsMap.get(localNode) != null) {
      int[] bucketArray = nodeToBucketsMap.get(localNode);
      if (isDebugEnabled) {
        logger.debug(
            "FunctionExecutionNodePruner: Adding the node: {} which is local and buckets {} to prunedMap",
            localNode, bucketArray);
      }
      System.arraycopy(bucketArray, 0, currentBucketArray, 0, bucketArray[0] + 1);
      prunedNodeToBucketsMap.put(localNode, bucketArray);
      nodeToBucketsMap.remove(localNode);
    }
    while (!arrayAndSetAreEqual(buckets, currentBucketArray)) {
      if (nodeToBucketsMap.size() == 0) {
        break;
      }
      // continue
      InternalDistributedMember node =
          findNextNode(nodeToBucketsMap.entrySet(), currentBucketArray);
      if (node == null) {
        if (isDebugEnabled) {
          logger.debug(
              "FunctionExecutionNodePruner: Breaking out of prunedMap calculation due to no available nodes for remaining buckets");
        }
        break;
      }
      int[] bucketArray = nodeToBucketsMap.get(node);
      bucketArray = removeAllElements(bucketArray, currentBucketArray);
      if (BucketSetHelper.length(bucketArray) != 0) {
        currentBucketArray = addAllElements(currentBucketArray, bucketArray);
        prunedNodeToBucketsMap.put(node, bucketArray);
        if (isDebugEnabled) {
          logger.debug(
              "FunctionExecutionNodePruner: Adding the node: {} and buckets {} to prunedMap", node,
              bucketArray);
        }
      }
      nodeToBucketsMap.remove(node);
    }
    if (isDebugEnabled) {
      logger.debug("FunctionExecutionNodePruner: The final prunedNodeToBucket calculated is: {}",
          prunedNodeToBucketsMap);
    }
    return prunedNodeToBucketsMap;
  }


  private static InternalDistributedMember findNextNode(
      Set<Map.Entry<InternalDistributedMember, int[]>> entrySet,
      int[] currentBucketArray) {

    InternalDistributedMember node = null;
    int max = -1;
    List<InternalDistributedMember> nodesOfEqualSize = new ArrayList<>();

    for (Map.Entry<InternalDistributedMember, int[]> entry : entrySet) {
      int[] buckets = entry.getValue();
      int[] tempBuckets = new int[buckets.length];
      System.arraycopy(buckets, 0, tempBuckets, 0, buckets[0] + 1);
      tempBuckets = removeAllElements(tempBuckets, currentBucketArray);

      if (max < BucketSetHelper.length(tempBuckets)) {
        max = BucketSetHelper.length(tempBuckets);
        node = entry.getKey();
        nodesOfEqualSize.clear();
        nodesOfEqualSize.add(node);
      } else if (max == BucketSetHelper.length(tempBuckets)) {
        nodesOfEqualSize.add(node);
      }
    }

    // return node;
    return (nodesOfEqualSize.size() > 0
        ? nodesOfEqualSize.get(PartitionedRegion.RANDOM.nextInt(nodesOfEqualSize.size())) : null);
  }

  public static <K> Map<BucketId, Set<K>> groupByBucket(PartitionedRegion pr, Set<K> routingKeys,
      final boolean primaryMembersNeeded, final boolean hasRoutingObjects,
      final boolean isBucketSetAsFilter) {
    HashMap<BucketId, Set<K>> bucketToKeysMap = new HashMap<>();

    for (final K routingKey : routingKeys) {
      final BucketId bucketId;
      if (isBucketSetAsFilter) {
        bucketId = BucketId.valueOf((Integer) routingKey);
      } else {
        if (hasRoutingObjects) {
          bucketId = BucketId.valueOf(getHashKey(pr, routingKey));
        } else {
          bucketId = BucketId
              .valueOf(getHashKey(pr, Operation.FUNCTION_EXECUTION, routingKey, null, null));
        }
      }
      final InternalDistributedMember mem;
      if (primaryMembersNeeded) {
        mem = pr.getOrCreateNodeForBucketWrite(bucketId, null);
      } else {
        mem = pr.getOrCreateNodeForBucketRead(bucketId);
      }
      if (mem == null) {
        throw new FunctionException(format("No target node found for KEY, %s", routingKey));
      }
      bucketToKeysMap.computeIfAbsent(bucketId, k -> new HashSet<>()).add(routingKey);
    }
    return bucketToKeysMap;
  }


  public static <K> int[] getBucketSet(PartitionedRegion pr, Set<K> routingKeys,
      final boolean hasRoutingObjects, boolean isBucketSetAsFilter) {
    int[] bucketArray = null;
    for (K key : routingKeys) {
      final BucketId bucketId;
      if (isBucketSetAsFilter) {
        bucketId = BucketId.valueOf((Integer) key);
      } else {
        if (hasRoutingObjects) {
          bucketId = BucketId.valueOf(getHashKey(pr, key));
        } else {
          bucketId =
              BucketId.valueOf(getHashKey(pr, Operation.FUNCTION_EXECUTION, key, null, null));
        }
      }
      if (bucketArray == null) {
        bucketArray = new int[routingKeys.size() + 1];
        bucketArray[0] = 0;
      }
      BucketSetHelper.add(bucketArray, bucketId.intValue());
    }
    return bucketArray;
  }

  public static HashMap<InternalDistributedMember, int[]> groupByMemberToBuckets(
      PartitionedRegion pr, Set<BucketId> bucketSet, boolean primaryOnly) {
    if (primaryOnly) {
      HashMap<InternalDistributedMember, int[]> memberToBucketsMap = new HashMap<>();
      try {
        for (BucketId bucketId : bucketSet) {
          InternalDistributedMember mem = pr.getOrCreateNodeForBucketWrite(bucketId, null);
          int[] bucketArray = memberToBucketsMap.get(mem);
          if (bucketArray == null) {
            bucketArray = new int[bucketSet.size() + 1]; // faster if this was an ArrayList
            memberToBucketsMap.put(mem, bucketArray);
            bucketArray[0] = 0;
          }
          BucketSetHelper.add(bucketArray, bucketId.intValue());

        }
      } catch (NoSuchElementException ignored) {
      }
      return memberToBucketsMap;
    } else {
      return pruneNodes(pr, bucketSet);
    }
  }

  private static boolean arrayAndSetAreEqual(Set<BucketId> setA, int[] arrayB) {
    final int len = BucketSetHelper.length(arrayB);
    if (len == 0) {
      return setA.isEmpty();
    }
    final Set<BucketId> setB =
        Arrays.stream(arrayB, 1, len).mapToObj(BucketId::valueOf).collect(Collectors.toSet());
    return setA.equals(setB);
  }

  private static int[] removeAllElements(int[] arrayA, int[] arrayB) {

    if (BucketSetHelper.length(arrayA) == 0 || BucketSetHelper.length(arrayB) == 0) {
      return arrayA;
    }

    Set<Integer> inSet = BucketSetHelper.toSet(arrayA);

    Set<Integer> subSet = BucketSetHelper.toSet(arrayB);

    inSet.removeAll(subSet);

    return BucketSetHelper.fromSet(inSet);

  }

  private static int[] addAllElements(int[] arrayA, int[] arrayB) {
    if (BucketSetHelper.length(arrayB) == 0) {
      return arrayA;
    }

    Set<Integer> inSet = BucketSetHelper.toSet(arrayA);

    Set<Integer> addSet = BucketSetHelper.toSet(arrayB);

    inSet.addAll(addSet);

    return BucketSetHelper.fromSet(inSet);

  }

}
