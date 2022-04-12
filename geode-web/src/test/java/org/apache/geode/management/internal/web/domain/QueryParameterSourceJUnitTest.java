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
package org.apache.geode.management.internal.web.domain;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.Query;

import org.junit.Test;

import org.apache.geode.internal.util.IOUtils;

/**
 * The QueryParameterSourceJUnitTest class is a test suite of test cases testing the contract and
 * functionality of the QueryParameterSource class.
 * <p/>
 *
 * @see org.apache.geode.management.internal.web.domain.QueryParameterSource
 * @see org.junit.Assert
 * @see org.junit.Test
 * @since GemFire 8.0
 */
public class QueryParameterSourceJUnitTest {

  @Test
  public void testCreateQueryParameterSource() throws MalformedObjectNameException {
    final var expectedObjectName = ObjectName.getInstance("GemFire:type=Member,*");

    final var expectedQueryExpression = Query.eq(Query.attr("id"), Query.value("12345"));

    final var query =
        new QueryParameterSource(expectedObjectName, expectedQueryExpression);

    assertNotNull(query);
    assertSame(expectedObjectName, query.getObjectName());
    assertSame(expectedQueryExpression, query.getQueryExpression());
  }

  @Test
  public void testSerialization()
      throws ClassNotFoundException, IOException, MalformedObjectNameException {
    final var expectedObjectName = ObjectName.getInstance("GemFire:type=Member,*");

    final var expectedQueryExpression =
        Query.or(Query.eq(Query.attr("name"), Query.value("myName")),
            Query.eq(Query.attr("id"), Query.value("myId")));

    final var expectedQuery =
        new QueryParameterSource(expectedObjectName, expectedQueryExpression);

    assertNotNull(expectedQuery);
    assertSame(expectedObjectName, expectedQuery.getObjectName());
    assertSame(expectedQueryExpression, expectedQuery.getQueryExpression());

    final var queryBytes = IOUtils.serializeObject(expectedQuery);

    assertNotNull(queryBytes);
    assertTrue(queryBytes.length != 0);

    final var queryObj = IOUtils.deserializeObject(queryBytes);

    assertTrue(queryObj instanceof QueryParameterSource);

    final var actualQuery = (QueryParameterSource) queryObj;

    assertNotSame(expectedQuery, actualQuery);
    assertNotNull(actualQuery.getObjectName());
    assertEquals(expectedQuery.getObjectName().toString(), actualQuery.getObjectName().toString());
    assertNotNull(actualQuery.getQueryExpression());
    assertEquals(expectedQuery.getQueryExpression().toString(),
        actualQuery.getQueryExpression().toString());
  }

}
