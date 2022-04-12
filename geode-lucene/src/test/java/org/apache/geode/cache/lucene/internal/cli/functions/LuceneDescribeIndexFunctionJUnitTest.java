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
package org.apache.geode.cache.lucene.internal.cli.functions;

import static org.apache.geode.cache.Region.SEPARATOR;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.ArgumentCaptor;

import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.ResultSender;
import org.apache.geode.cache.lucene.internal.InternalLuceneService;
import org.apache.geode.cache.lucene.internal.LuceneIndexImpl;
import org.apache.geode.cache.lucene.internal.LuceneServiceImpl;
import org.apache.geode.cache.lucene.internal.cli.LuceneIndexDetails;
import org.apache.geode.cache.lucene.internal.cli.LuceneIndexInfo;
import org.apache.geode.test.fake.Fakes;
import org.apache.geode.test.junit.categories.LuceneTest;

@Category({LuceneTest.class})

public class LuceneDescribeIndexFunctionJUnitTest {

  @Test
  @SuppressWarnings("unchecked")
  public void testExecute() throws Throwable {
    var cache = Fakes.cache();
    final var serverName = "mockServer";
    var service = mock(LuceneServiceImpl.class);
    when(cache.getService(InternalLuceneService.class)).thenReturn(service);
    var context = mock(FunctionContext.class);
    var resultSender = mock(ResultSender.class);
    var indexInfo = getMockLuceneInfo("index1");
    var index1 = getMockLuceneIndex("index1");
    var function = new LuceneDescribeIndexFunction();

    doReturn(indexInfo).when(context).getArguments();
    doReturn(resultSender).when(context).getResultSender();
    doReturn(cache).when(context).getCache();
    when(service.getIndex(indexInfo.getIndexName(), indexInfo.getRegionPath())).thenReturn(index1);

    function.execute(context);
    var resultCaptor =
        ArgumentCaptor.forClass(LuceneIndexDetails.class);
    verify(resultSender).lastResult(resultCaptor.capture());
    var result = resultCaptor.getValue();
    var expected = new LuceneIndexDetails(index1, "mockServer");

    assertEquals(expected.getIndexName(), result.getIndexName());
    assertEquals(expected.getRegionPath(), result.getRegionPath());
    assertEquals(expected.getIndexStats(), result.getIndexStats());
    assertEquals(expected.getFieldAnalyzersString(), result.getFieldAnalyzersString());
    assertEquals(expected.getSearchableFieldNamesString(), result.getSearchableFieldNamesString());
  }

  private LuceneIndexInfo getMockLuceneInfo(final String index1) {
    var mockInfo = mock(LuceneIndexInfo.class);
    doReturn(index1).when(mockInfo).getIndexName();
    doReturn(SEPARATOR + "region").when(mockInfo).getRegionPath();
    return mockInfo;
  }

  private LuceneIndexImpl getMockLuceneIndex(final String indexName) {
    var searchableFields = new String[] {"field1", "field2"};
    Map<String, Analyzer> fieldAnalyzers = new HashMap<>();
    fieldAnalyzers.put("field1", new StandardAnalyzer());
    fieldAnalyzers.put("field2", new KeywordAnalyzer());

    var index = mock(LuceneIndexImpl.class);
    when(index.getName()).thenReturn(indexName);
    when(index.getRegionPath()).thenReturn(SEPARATOR + "region");
    when(index.getFieldNames()).thenReturn(searchableFields);
    when(index.getFieldAnalyzers()).thenReturn(fieldAnalyzers);
    return index;
  }

}
