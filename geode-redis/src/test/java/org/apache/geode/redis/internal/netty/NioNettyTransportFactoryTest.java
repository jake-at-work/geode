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
 *
 */

package org.apache.geode.redis.internal.netty;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import java.util.concurrent.ThreadFactory;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.ServerChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import org.junit.Test;

public class NioNettyTransportFactoryTest {

  @Test
  public void createEventLoopGroup() {
    final NioNettyTransportFactory factory = new NioNettyTransportFactory();
    final ThreadFactory mock = mock(ThreadFactory.class);
    final EventLoopGroup eventLoopGroup = factory.createEventLoopGroup(0, mock);
    assertThat(eventLoopGroup).isInstanceOf(NioEventLoopGroup.class);
  }

  @Test
  public void getServerChannelClass() {
    final NioNettyTransportFactory factory = new NioNettyTransportFactory();
    final Class<? extends ServerChannel> serverChannelClass = factory.getServerChannelClass();
    assertThat(serverChannelClass).isEqualTo(NioServerSocketChannel.class);
  }
}
