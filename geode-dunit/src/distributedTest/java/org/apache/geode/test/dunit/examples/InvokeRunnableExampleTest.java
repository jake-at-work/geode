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
package org.apache.geode.test.dunit.examples;

import static org.apache.geode.test.dunit.VM.getAllVMs;
import static org.apache.geode.test.dunit.VM.getVM;
import static org.apache.geode.test.dunit.VM.getVMCount;

import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.test.dunit.rules.DistributedRule;

public class InvokeRunnableExampleTest {

  @Rule
  public DistributedRule distributedRule = new DistributedRule();

  @Test
  public void invokeHelloWorldForEachVMInGetAllVMs() {
    for (var vm : getAllVMs()) {
      vm.invoke(() -> System.out.println(vm + " says Hello World!"));
    }
  }

  @Test
  public void invokeHelloWorldInEachVMInOrder() {
    for (var whichVM = 0; whichVM < getVMCount(); whichVM++) {
      var vm = getVM(whichVM);
      vm.invoke(() -> System.out.println(vm + " says Hello World!"));
    }
  }
}
