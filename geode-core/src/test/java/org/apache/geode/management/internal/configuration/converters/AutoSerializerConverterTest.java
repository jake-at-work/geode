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

package org.apache.geode.management.internal.configuration.converters;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

import org.apache.geode.cache.configuration.DeclarableType;
import org.apache.geode.cache.configuration.ParameterType;
import org.apache.geode.management.configuration.AutoSerializer;

public class AutoSerializerConverterTest {
  private final AutoSerializerConverter converter = new AutoSerializerConverter();

  @Test
  public void convertNull() throws Exception {
    assertThat(converter.fromConfigObject(null)).isNull();
    assertThat(converter.fromXmlObject(null)).isNull();
  }

  @Test
  public void fromConfig() {
    var autoSerializer = new AutoSerializer(true, "pat1", "pat2");
    var declarableType = converter.fromConfigObject(autoSerializer);
    assertThat(declarableType.getClassName())
        .isEqualTo("org.apache.geode.pdx.ReflectionBasedAutoSerializer");
    var parameters = declarableType.getParameters();
    assertThat(parameters).containsExactlyInAnyOrder(new ParameterType("classes", "pat1,pat2"),
        new ParameterType("check-portability", "true"));
  }

  @Test
  public void fromDeclarableThatIsNotReflectionBasedAutoSerializer() {
    var type = new DeclarableType();
    type.setClassName("NotReflectionBasedAutoSerializer");
    var autoSerializer = converter.fromXmlObject(type);
    assertThat(autoSerializer).isNull();
  }

  @Test
  public void fromDeclarableThatHasOtherParameters() {
    var type = new DeclarableType();
    type.setClassName("org.apache.geode.pdx.ReflectionBasedAutoSerializer");
    var param1 = new ParameterType("field1", "value1");
    type.getParameters().add(param1);
    var autoSerializer = converter.fromXmlObject(type);
    assertThat(autoSerializer).isNull();
  }

  @Test
  public void fromDeclarable() {
    var type = new DeclarableType();
    type.setClassName("org.apache.geode.pdx.ReflectionBasedAutoSerializer");
    type.getParameters().add(new ParameterType("classes", "pat1 , pat2"));
    type.getParameters().add(new ParameterType("check-portability", "true"));
    var autoSerializer = converter.fromXmlObject(type);
    assertThat(autoSerializer).isNotNull();
    assertThat(autoSerializer.isPortable()).isTrue();
    assertThat(autoSerializer.getPatterns()).containsExactly("pat1", "pat2");
  }
}
