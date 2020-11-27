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

package org.apache.spark.ml.r

import org.apache.spark.SparkFunSuite
import org.apache.spark.ml.feature.{RFormula, RFormulaModel}
import org.apache.spark.mllib.util.MLlibTestSparkContext

class RWrapperUtilsSuite extends SparkFunSuite with MLlibTestSparkContext {

  test("avoid libsvm data column name conflicting") {
    val rFormula = new RFormula().setFormula("label ~ features")
    val dataDir = sys.props.get("spark.project.home") match {
      case Some(h) => h
      case None => ".."
    }
    val data = spark.read.format("libsvm").load(s"$dataDir/data/mllib/sample_libsvm_data.txt")
    // if not checking column name, then IllegalArgumentException
    intercept[IllegalArgumentException] {
      rFormula.fit(data)
    }

    // after checking, model build is ok
    RWrapperUtils.checkDataColumns(rFormula, data)

    assert(rFormula.getLabelCol == "label")
    assert(rFormula.getFeaturesCol.startsWith("features_"))

    val model = rFormula.fit(data)
    assert(model.isInstanceOf[RFormulaModel])

    assert(model.getLabelCol == "label")
    assert(model.getFeaturesCol.startsWith("features_"))
  }

}
