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

package org.apache.geode.modules.session.catalina;

import static org.apache.geode.modules.session.catalina.Tomcat8CommitSessionValve.getOutputBuffer;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import org.apache.catalina.Context;
import org.apache.catalina.connector.Connector;
import org.apache.catalina.connector.Request;
import org.apache.catalina.connector.Response;
import org.apache.coyote.OutputBuffer;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;


public class Tomcat8CommitSessionValveTest {

  private final Tomcat8CommitSessionValve valve = new Tomcat8CommitSessionValve();
  private final OutputBuffer outputBuffer = mock(OutputBuffer.class);
  private Response response;
  private org.apache.coyote.Response coyoteResponse;

  @Before
  public void before() {
    final var connector = mock(Connector.class);

    final var context = mock(Context.class);

    final var request = mock(Request.class);
    doReturn(context).when(request).getContext();

    coyoteResponse = new org.apache.coyote.Response();
    coyoteResponse.setOutputBuffer(outputBuffer);

    response = new Response();
    response.setConnector(connector);
    response.setRequest(request);
    response.setCoyoteResponse(coyoteResponse);
  }

  @Test
  public void wrappedOutputBufferForwardsToDelegate() throws IOException {
    wrappedOutputBufferForwardsToDelegate(new byte[] {'a', 'b', 'c'});
  }

  @Test
  public void recycledResponseObjectDoesNotWrapAlreadyWrappedOutputBuffer() throws IOException {
    wrappedOutputBufferForwardsToDelegate(new byte[] {'a', 'b', 'c'});
    response.recycle();
    reset(outputBuffer);
    wrappedOutputBufferForwardsToDelegate(new byte[] {'d', 'e', 'f'});
  }

  private void wrappedOutputBufferForwardsToDelegate(final byte[] bytes) throws IOException {
    final OutputStream outputStream =
        valve.wrapResponse(response).getResponse().getOutputStream();
    outputStream.write(bytes);
    outputStream.flush();

    final var byteBuffer = ArgumentCaptor.forClass(ByteBuffer.class);

    final var inOrder = inOrder(outputBuffer);
    inOrder.verify(outputBuffer).doWrite(byteBuffer.capture());
    inOrder.verifyNoMoreInteractions();

    final var wrappedOutputBuffer = getOutputBuffer(coyoteResponse);
    assertThat(wrappedOutputBuffer).isInstanceOf(Tomcat8CommitSessionOutputBuffer.class);
    assertThat(((Tomcat8CommitSessionOutputBuffer) wrappedOutputBuffer).getDelegate())
        .isNotInstanceOf(Tomcat8CommitSessionOutputBuffer.class);

    assertThat(byteBuffer.getValue().array()).contains(bytes);
  }

}
