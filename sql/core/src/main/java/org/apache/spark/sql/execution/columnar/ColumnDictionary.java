/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/*
 * Changes for TIBCO Project SnappyData data platform.
 *
 * Portions Copyright (c) 2017-2020 TIBCO Software Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

package org.apache.spark.sql.execution.columnar;

import java.nio.ByteBuffer;

import org.apache.spark.sql.execution.vectorized.Dictionary;

public final class ColumnDictionary implements Dictionary {
  private int[] intDictionary;
  private long[] longDictionary;

  public ColumnDictionary(int[] dictionary) {
    this.intDictionary = dictionary;
  }

  public ColumnDictionary(long[] dictionary) {
    this.longDictionary = dictionary;
  }

  @Override
  public int decodeToInt(int id) {
    return intDictionary[id];
  }

  @Override
  public long decodeToLong(int id) {
    return longDictionary[id];
  }

  @Override
  public float decodeToFloat(int id) {
    throw new UnsupportedOperationException("Dictionary encoding does not support float");
  }

  @Override
  public double decodeToDouble(int id) {
    throw new UnsupportedOperationException("Dictionary encoding does not support double");
  }

  @Override
  public byte[] decodeToBinary(int id) {
    throw new UnsupportedOperationException("Dictionary encoding does not support String");
  }

  @Override
  public ByteBuffer decodeToBuffer(int id) {
    throw new UnsupportedOperationException("Dictionary encoding does not support String");
  }
}
