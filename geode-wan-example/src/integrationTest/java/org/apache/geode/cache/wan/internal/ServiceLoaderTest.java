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

package org.apache.geode.cache.wan.internal;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.geode.wan.ExampleGatewaySenderTypeFactory;
import org.junit.jupiter.api.Test;

import org.apache.geode.cache.wan.internal.spi.GatewaySenderTypeFactory;

public class ServiceLoaderTest {

  public static final String SHORT_NAME = "ExampleGatewaySender";
  public static final String FULL_NAME = "com.example.geode.wan.ExampleGatewaySender";

  @Test
  public void foundByShortname() throws ClassNotFoundException {
    final GatewaySenderTypeFactory gatewaySenderTypeFactory =
        GatewaySenderFactoryImpl.findGatewaySenderTypeFactory(SHORT_NAME);
    assertThat(gatewaySenderTypeFactory).isInstanceOf(ExampleGatewaySenderTypeFactory.class);
  }

  @Test
  public void foundByFullName() throws ClassNotFoundException {
    final GatewaySenderTypeFactory gatewaySenderTypeFactory =
        GatewaySenderFactoryImpl.findGatewaySenderTypeFactory(FULL_NAME);
    assertThat(gatewaySenderTypeFactory).isInstanceOf(ExampleGatewaySenderTypeFactory.class);
  }
}
