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

package org.apache.spark.scheduler

import java.nio.ByteBuffer
import java.util.Properties

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.{Input, Output}

import org.apache.spark._
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.internal.config.APP_CALLER_CONTEXT
import org.apache.spark.memory.{MemoryMode, TaskMemoryManager}
import org.apache.spark.metrics.MetricsSystem
import org.apache.spark.rdd.InputFileBlockHolder
import org.apache.spark.util._

/**
 * A unit of execution. We have two kinds of Task's in Spark:
 *
 *  - [[org.apache.spark.scheduler.ShuffleMapTask]]
 *  - [[org.apache.spark.scheduler.ResultTask]]
 *
 * A Spark job consists of one or more stages. The very last stage in a job consists of multiple
 * ResultTasks, while earlier stages consist of ShuffleMapTasks. A ResultTask executes the task
 * and sends the task output back to the driver application. A ShuffleMapTask executes the task
 * and divides the task output to multiple buckets (based on the task's partitioner).
 *
 * @param _stageId id of the stage this task belongs to
 * @param _stageAttemptId attempt id of the stage this task belongs to
 * @param _partitionId index of the number in the RDD
 * @param localProperties copy of thread-local properties set by the user on the driver side.
 * @param serializedTaskMetrics a `TaskMetrics` that is created and serialized on the driver side
 *                              and sent to executor side.
 *
 * The parameters below are optional:
 * @param _jobId id of the job this task belongs to
 * @param _appId id of the app this task belongs to
 * @param _appAttemptId attempt id of the app this task belongs to
 * @param _isBarrier whether this task belongs to a barrier stage. Spark must launch all the tasks
 *                   at the same time for a barrier stage.
 */
private[spark] abstract class Task[T](
    private var _stageId: Int,
    private var _stageAttemptId: Int,
    private var _partitionId: Int,
    @transient var localProperties: Properties = new Properties,
    // The default value is only used in tests.
    private var serializedTaskMetrics: Array[Byte] =
      SparkEnv.get.closureSerializer.newInstance().serialize(TaskMetrics.registered).array(),
    private var _jobId: Option[Int] = None,
    private var _appId: Option[String] = None,
    private var _appAttemptId: Option[String] = None,
    private var _isBarrier: Boolean = false) extends Serializable {

  @transient lazy val metrics: TaskMetrics =
    SparkEnv.get.closureSerializer.newInstance().deserialize(ByteBuffer.wrap(serializedTaskMetrics))

  @transient private[spark] var cpusPerTask: Int = _

  final def stageId: Int = _stageId

  final def stageAttemptId: Int = _stageAttemptId

  final def partitionId: Int = _partitionId

  final def jobId: Option[Int] = _jobId

  final def appId: Option[String] = _appId

  final def appAttemptId: Option[String] = _appAttemptId

  final def isBarrier: Boolean = _isBarrier

  /**
   * Called by [[org.apache.spark.executor.Executor]] to run this task.
   *
   * @param taskAttemptId an identifier for this task attempt that is unique within a SparkContext.
   * @param attemptNumber how many times this task has been attempted (0 for the first attempt)
   * @return the result of the task along with updates of Accumulators.
   */
  final def run(
      taskAttemptId: Long,
      attemptNumber: Int,
      metricsSystem: MetricsSystem): T = {
    SparkEnv.get.blockManager.registerTask(taskAttemptId)
    // TODO SPARK-24874 Allow create BarrierTaskContext based on partitions, instead of whether
    // the stage is barrier.
    val taskContext = new TaskContextImpl(
      stageId,
      stageAttemptId, // stageAttemptId and stageAttemptNumber are semantically equal
      partitionId,
      taskAttemptId,
      attemptNumber,
      taskMemoryManager,
      localProperties,
      metricsSystem,
      metrics)

    context = if (isBarrier) {
      new BarrierTaskContext(taskContext)
    } else {
      taskContext
    }

    InputFileBlockHolder.initialize()
    TaskContext.setTaskContext(context)
    taskThread = Thread.currentThread()

    if (_reasonIfKilled != null) {
      kill(interruptThread = false, _reasonIfKilled)
    }

    new CallerContext(
      "TASK",
      SparkEnv.get.conf.get(APP_CALLER_CONTEXT),
      appId,
      appAttemptId,
      jobId,
      Option(stageId),
      Option(stageAttemptId),
      Option(taskAttemptId),
      Option(attemptNumber)).setCurrentContext()

    try {
      runTask(context)
    } catch {
      case e: Throwable =>
        // Catch all errors; run task failure callbacks, and rethrow the exception.
        try {
          context.markTaskFailed(e)
        } catch {
          case t: Throwable =>
            e.addSuppressed(t)
        }
        context.markTaskCompleted(Some(e))
        throw e
    } finally {
      try {
        // Call the task completion callbacks. If "markTaskCompleted" is called twice, the second
        // one is no-op.
        context.markTaskCompleted(None)
      } finally {
        try {
          Utils.tryLogNonFatalError {
            // Release memory used by this thread for unrolling blocks
            SparkEnv.get.blockManager.memoryStore.releaseUnrollMemoryForThisTask(MemoryMode.ON_HEAP)
            SparkEnv.get.blockManager.memoryStore.releaseUnrollMemoryForThisTask(
              MemoryMode.OFF_HEAP)
            // Notify any tasks waiting for execution memory to be freed to wake up and try to
            // acquire memory again. This makes impossible the scenario where a task sleeps forever
            // because there are no other tasks left to notify it. Since this is safe to do but may
            // not be strictly necessary, we should revisit whether we can remove this in the
            // future.
            val memoryManager = SparkEnv.get.memoryManager
            memoryManager.synchronized { memoryManager.notifyAll() }
          }
        } finally {
          // Though we unset the ThreadLocal here, the context member variable itself is still
          // queried directly in the TaskRunner to check for FetchFailedExceptions.
          TaskContext.unset()
          InputFileBlockHolder.unset()
        }
      }
    }
  }

  @transient private var taskMemoryManager: TaskMemoryManager = _

  def setTaskMemoryManager(taskMemoryManager: TaskMemoryManager): Unit = {
    this.taskMemoryManager = taskMemoryManager
  }

  def runTask(context: TaskContext): T

  def preferredLocations: Seq[TaskLocation] = Nil

  // Map output tracker epoch. Will be set by TaskSetManager.
  var epoch: Long = -1

  // Task context, to be initialized in run().
  @transient var context: TaskContext = _

  // The actual Thread on which the task is running, if any. Initialized in run().
  @volatile @transient private var taskThread: Thread = _

  // If non-null, this task has been killed and the reason is as specified. This is used in case
  // context is not yet initialized when kill() is invoked.
  @volatile @transient private var _reasonIfKilled: String = null

  protected var _executorDeserializeTime: Long = 0
  protected var _executorDeserializeCpuTime: Long = 0

  /**
   * If defined, this task has been killed and this option contains the reason.
   */
  def reasonIfKilled: Option[String] = Option(_reasonIfKilled)

  /**
   * Returns the amount of time spent deserializing the RDD and function to be run in nanos.
   */
  def executorDeserializeTime: Long = _executorDeserializeTime
  def executorDeserializeCpuTime: Long = _executorDeserializeCpuTime

  /**
   * Collect the latest values of accumulators used in this task. If the task failed,
   * filter out the accumulators whose values should not be included on failures.
   */
  def collectAccumulatorUpdates(taskFailed: Boolean = false): Seq[AccumulatorV2[_, _]] = {
    if (context != null) {
      // Note: internal accumulators representing task metrics always count failed values
      context.taskMetrics.nonZeroInternalAccums() ++
        // zero value external accumulators may still be useful, e.g. SQLMetrics, we should not
        // filter them out.
        context.taskMetrics.externalAccums.filter(a => !taskFailed || a.countFailedValues)
    } else {
      Nil
    }
  }

  /**
   * Kills a task by setting the interrupted flag to true. This relies on the upper level Spark
   * code and user code to properly handle the flag. This function should be idempotent so it can
   * be called multiple times.
   * If interruptThread is true, we will also call Thread.interrupt() on the Task's executor thread.
   */
  def kill(interruptThread: Boolean, reason: String) {
    require(reason != null)
    _reasonIfKilled = reason
    if (context != null) {
      context.markInterrupted(reason)
    }
    if (interruptThread && taskThread != null) {
      taskThread.interrupt()
    }
  }

  protected final def writeKryo(kryo: Kryo, output: Output): Unit = {
    output.writeInt(_stageId)
    output.writeVarInt(_stageAttemptId, true)
    output.writeVarInt(_partitionId, true)
    output.writeVarInt(serializedTaskMetrics.length, true)
    output.write(serializedTaskMetrics)
    output.writeVarInt(if (_jobId.isDefined) _jobId.get else -1, true)
    output.writeString(if (_appId.isDefined) _appId.get else null)
    output.writeString(if (_appAttemptId.isDefined) _appAttemptId.get else null)
    output.writeBoolean(_isBarrier)
    output.writeLong(epoch)
    output.writeLong(_executorDeserializeTime)
    output.writeLong(_executorDeserializeCpuTime)
  }

  protected final def readKryo(kryo: Kryo, input: Input): Unit = {
    _stageId = input.readInt()
    _stageAttemptId = input.readVarInt(true)
    _partitionId = input.readVarInt(true)
    val len = input.readVarInt(true)
    serializedTaskMetrics = input.readBytes(len)
    _jobId = input.readVarInt(true) match {
      case -1 => None
      case v => Some(v)
    }
    _appId = Option(input.readString())
    _appAttemptId = Option(input.readString())
    _isBarrier = input.readBoolean()
    epoch = input.readLong()
    _executorDeserializeTime = input.readLong()
    _executorDeserializeCpuTime = input.readLong()
  }
}
