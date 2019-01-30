/*
 * Copyright (c) 2018 SnappyData, Inc. All rights reserved.
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
package org.apache.spark.sql.streaming.ui

import javax.servlet.http.HttpServletRequest

import scala.xml.Node

import org.apache.spark.sql.streaming.StreamingQueryManager
import org.apache.spark.ui.{UIUtils, WebUIPage}

class StreamingQueriesPage extends WebUIPage("") {
  private val listener = StreamingQueryManager.snappyStreamingQueryListener

  override def render(request: HttpServletRequest): Seq[Node] = {


    var contentToPrint = <div>
      {listener.queries.map(q => q._2.name).mkString(",")}
    </div>

    UIUtils.basicSparkPage(contentToPrint, "Sample Streaming page", false)
  }
}
