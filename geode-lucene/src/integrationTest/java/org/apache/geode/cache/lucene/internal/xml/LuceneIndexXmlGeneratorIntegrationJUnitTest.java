/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
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

package org.apache.geode.cache.lucene.internal.xml;

import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.PrintWriter;
import java.util.Properties;

import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.lucene.LuceneIndex;
import org.apache.geode.cache.lucene.LuceneService;
import org.apache.geode.cache.lucene.LuceneServiceProvider;
import org.apache.geode.cache.lucene.test.LuceneDeclarable2TestSerializer;
import org.apache.geode.cache.lucene.test.LuceneTestSerializer;
import org.apache.geode.internal.cache.xmlcache.CacheXmlGenerator;
import org.apache.geode.test.junit.categories.LuceneTest;

@Category({LuceneTest.class})
public class LuceneIndexXmlGeneratorIntegrationJUnitTest {

  protected Cache cache;

  @After
  public void closeCache() {
    cache.close();
  }

  protected void createDataRegionAndLuceneIndex(LuceneService service) {
    service.createIndexFactory().setFields("a", "b", "c").create("index", "region");
    cache.createRegionFactory(RegionShortcut.PARTITION).create("region");
  }

  protected void createDataRegionAndLuceneIndexWithSerializer(LuceneTestSerializer luceneSerializer,
      LuceneService service) {
    service.createIndexFactory().setLuceneSerializer(luceneSerializer).setFields("a", "b", "c")
        .create("index", "region");
    cache.createRegionFactory(RegionShortcut.PARTITION).create("region");
  }

  /**
   * Test of generating and reading cache configuration back in.
   */
  @Test
  public void generateWithFields() {
    cache = new CacheFactory().set(MCAST_PORT, "0").create();
    var service = LuceneServiceProvider.get(cache);
    createDataRegionAndLuceneIndex(service);

    var index = generateAndParseXml(service);

    assertArrayEquals(new String[] {"a", "b", "c"}, index.getFieldNames());
  }

  @Test
  public void generateWithDeclarable2SerializerWithStringProperty() {
    var luceneSerializer = new LuceneDeclarable2TestSerializer();
    luceneSerializer.getConfig().setProperty("param", "value");
    var p = generateAndParseDeclarable2Serializer(luceneSerializer);
    assertEquals("value", p.getProperty("param"));
  }

  @Test
  public void generateWithDeclarable2SerializerWithNoProperties() {
    var luceneSerializer = new LuceneDeclarable2TestSerializer();
    var p = generateAndParseDeclarable2Serializer(luceneSerializer);
    assertEquals(new Properties(), p);
  }

  @Test
  public void generateWithDeclarable2SerializerWithDeclarableProperty() {
    var luceneSerializer = new LuceneDeclarable2TestSerializer();
    luceneSerializer.getConfig().put("param", new LuceneTestSerializer());
    var p = generateAndParseDeclarable2Serializer(luceneSerializer);
    assertThat(p.get("param")).isInstanceOf(LuceneTestSerializer.class);
  }

  private Properties generateAndParseDeclarable2Serializer(
      LuceneDeclarable2TestSerializer luceneSerializer) {
    cache = new CacheFactory().set(MCAST_PORT, "0").create();
    var service = LuceneServiceProvider.get(cache);
    createDataRegionAndLuceneIndexWithSerializer(luceneSerializer, service);

    var index = generateAndParseXml(service);

    assertArrayEquals(new String[] {"a", "b", "c"}, index.getFieldNames());

    var testSerializer = index.getLuceneSerializer();
    return ((LuceneDeclarable2TestSerializer) testSerializer).getConfig();
  }

  @Test
  public void generateWithSerializer() {
    cache = new CacheFactory().set(MCAST_PORT, "0").create();
    var service = LuceneServiceProvider.get(cache);
    createDataRegionAndLuceneIndexWithSerializer(new LuceneTestSerializer(), service);

    var index = generateAndParseXml(service);

    assertArrayEquals(new String[] {"a", "b", "c"}, index.getFieldNames());

    var testSerializer = index.getLuceneSerializer();
    assertThat(testSerializer).isInstanceOf(LuceneTestSerializer.class);
  }

  /**
   * Generate an xml configuration from the LuceneService and parse it, returning the index that was
   * created from the xml.
   */
  private LuceneIndex generateAndParseXml(LuceneService service) {
    var baos = new ByteArrayOutputStream();
    var pw = new PrintWriter(baos);
    CacheXmlGenerator.generate(cache, pw, false, false);
    pw.flush();

    cache.close();
    cache = new CacheFactory().set(MCAST_PORT, "0").create();

    var bytes = baos.toByteArray();
    cache.loadCacheXml(new ByteArrayInputStream(bytes));

    var service2 = LuceneServiceProvider.get(cache);
    assertNotSame(service, service2);

    var index = service2.getIndex("index", "region");
    assertNotNull(index);
    return index;
  }

}
