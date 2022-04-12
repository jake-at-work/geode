/**
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
package org.apache.geode.internal.cache;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;

import org.junit.Test;

public class TxCommitMessageBCServerToServerTxBothTest extends TxCommitMessageBCTestBase {

  @Test
  public void test() throws Exception {
    var regionNameRepl = REPLICATE_REGION_NAME;
    var regionNamePart = PARTITION_REGION_NAME;

    var beforeValuesRepl =
        server1.invoke(() -> TxCommitMessageBCTestBase.doGets(regionNameRepl));
    var beforeValuesPart =
        server1.invoke(() -> TxCommitMessageBCTestBase.doGets(regionNamePart));
    server1.invoke(() -> TxCommitMessageBCTestBase.doTxPutsBoth(regionNameRepl,
        regionNamePart));
    var afterValuesRepl1 =
        server1.invoke(() -> TxCommitMessageBCTestBase.doGets(regionNameRepl));
    var afterValuesRepl2 =
        server2.invoke(() -> TxCommitMessageBCTestBase.doGets(regionNameRepl));
    var afterValuesPart1 =
        server1.invoke(() -> TxCommitMessageBCTestBase.doGets(regionNamePart));
    var afterValuesPart2 =
        server2.invoke(() -> TxCommitMessageBCTestBase.doGets(regionNamePart));

    Integer expectedRepl1 = beforeValuesRepl.get(0) == null ? 500 : beforeValuesRepl.get(0) + 500;
    Integer expectedRepl2 = beforeValuesRepl.get(1) == null ? 1000 : beforeValuesRepl.get(1) + 1000;
    Integer expectedPart1 = beforeValuesPart.get(0) == null ? 1500 : beforeValuesPart.get(0) + 1500;
    Integer expectedPart2 = beforeValuesPart.get(1) == null ? 2000 : beforeValuesPart.get(1) + 2000;

    assertThat(afterValuesRepl1, contains(expectedRepl1, expectedRepl2));
    assertThat(afterValuesRepl2, contains(expectedRepl1, expectedRepl2));
    assertThat(afterValuesPart1, contains(expectedPart1, expectedPart2));
    assertThat(afterValuesPart2, contains(expectedPart1, expectedPart2));
  }

}
