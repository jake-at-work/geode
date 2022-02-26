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

package org.apache.geode.modules.session.internal.filter;

import com.mockrunner.mock.web.MockFilterConfig;
import com.mockrunner.mock.web.WebMockObjectFactory;
import org.junit.Before;
import org.junit.experimental.categories.Category;

import org.apache.geode.modules.session.filter.SessionCachingFilter;
import org.apache.geode.test.junit.categories.SessionTest;
import org.apache.geode.common.internal.utils.GeodeGlossary;

/**
 * This runs all tests with a local cache disabled
 */
@Category({SessionTest.class})
public class SessionReplicationJUnitTest extends CommonTests {

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();

    WebMockObjectFactory factory = getWebMockObjectFactory();
    MockFilterConfig config = factory.getMockFilterConfig();

    config.setInitParameter(GeodeGlossary.GEMFIRE_PREFIX + "property.mcast-port", "0");
    config.setInitParameter("cache-type", "peer-to-peer");

    factory.getMockServletContext().setContextPath(CONTEXT_PATH);

    factory.getMockRequest().setRequestURL("/test/foo/bar");
    factory.getMockRequest().setContextPath(CONTEXT_PATH);

    createFilter(SessionCachingFilter.class);
    createServlet(CallbackServlet.class);

    setDoChain(true);
  }
}
