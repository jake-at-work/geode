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
package com.examples.ds;

import java.util.Date;
import java.util.Properties;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.distributed.DistributedSystem;

/**
 * Places various objects that use {@link org.apache.geode.DataSerializer}s and
 * {@link org.apache.geode.Instantiator}s into a cache {@link Region}. Among other things, this is
 * used to test bug 31573.
 *
 * @since GemFire 3.5
 */
public class PutDataSerializables {

  public static void main(String[] args) throws Throwable {
    var props = new Properties();
    var system = DistributedSystem.connect(props);
    var cache = CacheFactory.create(system);
    var factory = new AttributesFactory();
    var region = cache.createRegion("DataSerializable", factory.create());
    region.put("User", new User("Fred", 42));

    new CompanySerializer();
    var address = new Address();
    var company = new Company("My Company", address);

    region.put("Company", company);
    region.put("Employee", new Employee(43, "Bob", new Date(), company));

    Thread.sleep(60 * 1000);
  }

}
