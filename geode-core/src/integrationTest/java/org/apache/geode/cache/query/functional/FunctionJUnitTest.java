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
/*
 * FunctionJUnitTest.java JUnit based test
 *
 * Created on March 10, 2005, 4:13 PM
 */
package org.apache.geode.cache.query.functional;

import static org.apache.geode.cache.Region.SEPARATOR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.query.CacheUtils;
import org.apache.geode.cache.query.FunctionDomainException;
import org.apache.geode.cache.query.Query;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.data.Portfolio;
import org.apache.geode.cache.query.internal.CompiledFunction;
import org.apache.geode.cache.query.internal.CompiledLiteral;
import org.apache.geode.cache.query.internal.CompiledValue;
import org.apache.geode.cache.query.internal.ExecutionContext;
import org.apache.geode.cache.query.internal.parse.OQLLexerTokenTypes;
import org.apache.geode.test.junit.categories.OQLQueryTest;

@Category({OQLQueryTest.class})
public class FunctionJUnitTest {

  @Before
  public void setUp() throws java.lang.Exception {
    CacheUtils.startCache();
    var region = CacheUtils.createRegion("Portfolios", Portfolio.class);
    region.put("0", new Portfolio(0));
    region.put("1", new Portfolio(1));
    region.put("2", new Portfolio(2));
    region.put("3", new Portfolio(3));
  }

  @After
  public void tearDown() throws java.lang.Exception {
    CacheUtils.closeCache();
  }

  @Test
  public void testCanonicalization() throws Exception {
    CompiledValue cv1 = new CompiledLiteral("str1");
    CompiledValue cv2 = new CompiledLiteral("str2");
    CompiledValue cv3 = new CompiledLiteral(null);
    CompiledValue cv4 = new CompiledLiteral(null);
    CompiledValue cv5 = new CompiledLiteral(10);
    CompiledValue cv6 = new CompiledLiteral(5);
    var cvArr = new CompiledValue[][] {{cv1, cv2, cv3, cv4, cv5, cv6},
        {cv6, cv5, cv3, cv4, cv2, cv1}, {cv1, cv3, cv5, cv2, cv4, cv6}, {cv1}, {cv3}, {cv5}};

    var canonicalizedArgs =
        new String[] {"('str1','str2',null,null,10,5)", "(5,10,null,null,'str2','str1')",
            "('str1',null,10,'str2',null,5)", "('str1')", "(null)", "(10)"};

    ExecutionContext context = null;
    for (var i = 0; i < 6; i++) {
      CompiledValue cf = new CompiledFunction(cvArr[i], OQLLexerTokenTypes.LITERAL_nvl);
      var clauseBuffer = new StringBuilder();
      cf.generateCanonicalizedExpression(clauseBuffer, context);
      if (!clauseBuffer.toString().equals("NVL" + canonicalizedArgs[i])) {
        fail("Canonicalization not done properly");
      }

      cf = new CompiledFunction(cvArr[i], OQLLexerTokenTypes.LITERAL_element);
      clauseBuffer = new StringBuilder();
      cf.generateCanonicalizedExpression(clauseBuffer, context);
      if (!clauseBuffer.toString().equals("ELEMENT" + canonicalizedArgs[i])) {
        fail("Canonicalization not done properly");
      }
    }
  }

  @Test
  public void testIS_DEFINED() throws Exception {
    var query = CacheUtils.getQueryService()
        .newQuery("SELECT DISTINCT * FROM " + SEPARATOR + "Portfolios where IS_DEFINED(P2.secId)");
    var result = query.execute();
    if (result instanceof Collection) {
      for (final var o : (Collection) result) {
        var p = (Portfolio) o;
        if (p.getP2() == null) {
          fail(query.getQueryString());
        }
      }
    }
    var testData = new Object[][] {{"string", Boolean.TRUE}, {0, Boolean.TRUE},
        {QueryService.UNDEFINED, Boolean.FALSE}, {null, Boolean.TRUE}};

    for (final var testDatum : testData) {
      query = CacheUtils.getQueryService().newQuery("IS_DEFINED($1)");
      result = query.execute(testDatum);
      if (!result.equals(testDatum[1])) {
        fail(query.getQueryString() + " for " + testDatum[0]);
      }
    }
  }

  @Test
  public void testIS_UNDEFINED() throws Exception {
    var query = CacheUtils.getQueryService()
        .newQuery(
            "SELECT DISTINCT * FROM " + SEPARATOR + "Portfolios where IS_UNDEFINED(P2.secId)");
    var result = query.execute();
    if (result instanceof Collection) {
      for (final var o : (Collection) result) {
        var p = (Portfolio) o;
        if (p.getP2() != null) {
          fail(query.getQueryString());
        }
      }
    }
    var testData = new Object[][] {{"string", Boolean.FALSE}, {0, Boolean.FALSE},
        {QueryService.UNDEFINED, Boolean.TRUE}, {null, Boolean.FALSE}};

    for (final var testDatum : testData) {
      query = CacheUtils.getQueryService().newQuery("IS_UNDEFINED($1)");
      result = query.execute(testDatum);
      if (!result.equals(testDatum[1])) {
        fail(query.getQueryString() + " for " + testDatum[0]);
      }
    }
  }

  @Test
  public void testELEMENT() throws Exception {
    var query = CacheUtils.getQueryService()
        .newQuery("ELEMENT(SELECT DISTINCT * FROM " + SEPARATOR + "Portfolios where ID =1).status");
    var result = query.execute();
    if (!result.equals("inactive")) {
      fail(query.getQueryString());
    }
    try {
      query = CacheUtils.getQueryService()
          .newQuery(
              "ELEMENT(SELECT DISTINCT * FROM " + SEPARATOR + "Portfolios where ID <= 1).status");
      result = query.execute();
      fail(query.getQueryString());
    } catch (FunctionDomainException ignored) {

    }
  }

  @Test
  public void testNVL() throws Exception {
    var query = CacheUtils.getQueryService().newQuery("nvl(NULL, 'foundNull')");
    var result = query.execute();
    if (!result.equals("foundNull")) {
      fail(query.getQueryString());
    }

    query = CacheUtils.getQueryService().newQuery("nvl('notNull', 'foundNull')");
    result = query.execute();
    if (result.equals("foundNull")) {
      fail(query.getQueryString());
    }

    query = CacheUtils.getQueryService().newQuery(
        "select distinct * from " + SEPARATOR
            + "Portfolios pf where nvl(pf.position2,'foundNull') = 'foundNull'");
    result = query.execute();

    if (((Collection) result).size() != 2) {
      fail(query.getQueryString());
    }

    query = CacheUtils.getQueryService().newQuery(
        "select distinct nvl(pf.position2, 'inProjection') from " + SEPARATOR
            + "Portfolios pf where nvl(pf.position2,'foundNull') = 'foundNull'");
    result = query.execute();
    // CacheUtils.log("Size of result :" + ((Collection)result).size());
    if (((Collection) result).size() != 1) {
      fail(query.getQueryString());
    }
  }

  @Test
  public void testTo_Date() throws Exception {
    var queries =
        new String[] {"to_date('10/09/05', 'MM/dd/yy')", "to_date('09/10/05', 'dd/MM/yy')",
            "to_date('05/10/09', 'yy/MM/dd')", "to_date('05/09/10', 'yy/dd/MM')",
            "to_date('10/05/09', 'MM/yy/dd')", "to_date('09/05/10', 'dd/yy/MM')",

            "to_date('10/09/2005', 'MM/dd/yy')", "to_date('09/10/2005', 'dd/MM/yy')",
            "to_date('2005/10/09', 'yy/MM/dd')", "to_date('2005/09/10', 'yy/dd/MM')",
            "to_date('10/2005/09', 'MM/yy/dd')", "to_date('09/2005/10', 'dd/yy/MM')",

            "to_date('10/09/2005', 'MM/dd/yyyy')", "to_date('09/10/2005', 'dd/MM/yyyy')",
            "to_date('2005/10/09', 'yyyy/MM/dd')", "to_date('2005/09/10', 'yyyy/dd/MM')",
            "to_date('10/2005/09', 'MM/yyyy/dd')", "to_date('09/2005/10', 'dd/yyyy/MM')",
            ////////////////////////////////////////////////////////////////////

            "to_date('100905', 'MMddyy')", "to_date('091005', 'ddMMyy')",
            "to_date('051009', 'yyMMdd')",
            "to_date('050910', 'yyddMM')", "to_date('100509', 'MMyydd')",
            "to_date('090510', 'ddyyMM')",

            "to_date('10092005', 'MMddyy')", "to_date('09102005', 'ddMMyy')",

            "to_date('10092005', 'MMddyyyy')", "to_date('09102005', 'ddMMyyyy')",
            "to_date('20051009', 'yyyyMMdd')", "to_date('20050910', 'yyyyddMM')",
            "to_date('10200509', 'MMyyyydd')", "to_date('09200510', 'ddyyyyMM')",
        //

        };

    var noCheckQueries =
        new String[] {"to_date('100936', 'MMddyyyy')", "to_date('09/10/05', 'dd/MM/yyyy')",
            "to_date('05/10/09', 'yyyy/MM/dd')", "to_date('05/09/10', 'yyyy/dd/MM')",
            "to_date('10/05/09', 'MM/yyyy/dd')", "to_date('09/05/10', 'dd/yyyy/MM')",

            "to_date('20051009', 'yyMMdd')", "to_date('20050910', 'yyddMM')",
            "to_date('10200509', 'MMyydd')", "to_date('09200510', 'ddyyMM')",

        };

    var fineGrainedQueries = new String[][] {new String[] {"10092005121314", "MMddyyyyHHmmss"},
        new String[] {"10092005121314567", "MMddyyyyHHmmssSSS"}};

    Query query = null;
    Object result = null;
    var date = new Date(105, 9, 9);
    var qs = CacheUtils.getQueryService();
    for (final var s : queries) {
      query = qs.newQuery(s);
      result = query.execute();
      // CacheUtils.log(((Date)result));
      if (!result.equals(date)) {
        fail(query.getQueryString());
      }
    }

    for (var i = 0; i < noCheckQueries.length; i++) {
      query = qs.newQuery(queries[i]);
      result = query.execute();
      // CacheUtils.log(((Date)result));

    }

    for (var dateStringAndFormat : fineGrainedQueries) {
      var dateString = dateStringAndFormat[0];
      var format = dateStringAndFormat[1];
      var sdf = new SimpleDateFormat(format);
      var sdfDate = sdf.parse(dateString);
      query = qs.newQuery("to_date('" + dateString + "', '" + format + "')");
      var qsDate = (Date) query.execute();
      assertEquals(sdfDate, qsDate);
    }

  }

}
