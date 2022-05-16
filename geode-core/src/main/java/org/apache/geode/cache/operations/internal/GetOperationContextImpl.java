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
package org.apache.geode.cache.operations.internal;

import org.apache.geode.SerializationException;
import org.apache.geode.cache.operations.GetOperationContext;

/**
 * This subclass's job is to keep customers from getting a reference to a value that is off-heap.
 * Any access to an off-heap value should appear to the customer as a serialized value.
 *
 * @deprecated since Geode1.0, use {@link org.apache.geode.security.ResourcePermission} instead
 */
@Deprecated
public class GetOperationContextImpl extends GetOperationContext {

  public GetOperationContextImpl(Object key, boolean postOperation) {
    super(key, postOperation);
  }

  /**
   * This method is for internal use and should not be on the public apis.
   */
  public Object getRawValue() {
    return super.getValue();
  }

  @Override
  public Object getObject() {
    return super.getObject();
  }

  @Override
  public void setObject(Object value, boolean isObject) {
    super.setObject(value, isObject);
  }

  @Override
  public void setValue(Object value, boolean isObject) {
    super.setValue(value, isObject);
  }

  @Override
  public byte[] getSerializedValue() {
    return super.getSerializedValue();
  }

  @Override
  public Object getDeserializedValue() throws SerializationException {
    return super.getDeserializedValue();
  }

  @Override
  public Object getValue() {
    return super.getValue();
  }
}
