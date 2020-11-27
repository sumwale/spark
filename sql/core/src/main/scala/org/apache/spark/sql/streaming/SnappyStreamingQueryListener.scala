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
 * Copyright (c) 2017-2020 TIBCO Software Inc. All rights reserved.
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

package org.apache.spark.sql.streaming

class SnappyStreamingQueryListener extends StreamingQueryListener {

  private val streamingRepo = StreamingRepository.getInstance

  override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit = {
    val queryName = {
      if (event.name == null || event.name.isEmpty) {
        event.id.toString
      } else {
        event.name
      }
    }

    streamingRepo.addQuery(event.id,
      new StreamingQueryStatistics(
        event.id,
        queryName,
        event.runId,
        System.currentTimeMillis(),
        event.triggerInterval))
  }

  override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit = {
    streamingRepo.updateQuery(event.progress)
  }

  override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit = {
    streamingRepo.setQueryStatus(event.id, false)
  }

}
