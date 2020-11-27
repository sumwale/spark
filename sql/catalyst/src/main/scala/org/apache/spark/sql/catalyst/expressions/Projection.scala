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

package org.apache.spark.sql.catalyst.expressions

import scala.util.control.NonFatal

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.{GenerateSafeProjection, GenerateUnsafeProjection}
import org.apache.spark.sql.types._

/**
 * A [[Projection]] that is calculated by calling the `eval` of each of the specified expressions.
 *
 * @param expressions a sequence of expressions that determine the value of each column of the
 *                    output row.
 */
class InterpretedProjection(expressions: Seq[Expression]) extends Projection {
  def this(expressions: Seq[Expression], inputSchema: Seq[Attribute]) =
    this(expressions.map(BindReferences.bindReference(_, inputSchema)))

  override def initialize(partitionIndex: Int): Unit = {
    expressions.foreach(_.foreach {
      case n: Nondeterministic => n.initialize(partitionIndex)
      case _ =>
    })
  }

  // null check is required for when Kryo invokes the no-arg constructor.
  protected val exprArray = if (expressions != null) expressions.toArray else null

  def apply(input: InternalRow): InternalRow = {
    val outputArray = new Array[Any](exprArray.length)
    var i = 0
    while (i < exprArray.length) {
      outputArray(i) = exprArray(i).eval(input)
      i += 1
    }
    new GenericInternalRow(outputArray)
  }

  override def toString(): String = s"Row => [${exprArray.mkString(",")}]"
}

/**
 * A [[MutableProjection]] that is calculated by calling `eval` on each of the specified
 * expressions.
 *
 * @param expressions a sequence of expressions that determine the value of each column of the
 *                    output row.
 */
case class InterpretedMutableProjection(expressions: Seq[Expression]) extends MutableProjection {
  def this(expressions: Seq[Expression], inputSchema: Seq[Attribute]) =
    this(expressions.map(BindReferences.bindReference(_, inputSchema)))

  private[this] val buffer = new Array[Any](expressions.size)

  override def initialize(partitionIndex: Int): Unit = {
    expressions.foreach(_.foreach {
      case n: Nondeterministic => n.initialize(partitionIndex)
      case _ =>
    })
  }

  private var targetUnsafe = false
  type UnsafeSetter = (UnsafeRow, Any) => Unit
  private var setters: Array[UnsafeSetter] = _
  private[this] val exprArray = expressions.toArray
  private[this] var mutableRow: InternalRow = new GenericInternalRow(exprArray.length)
  def currentValue: InternalRow = mutableRow

  override def target(row: InternalRow): MutableProjection = {
    mutableRow = row
    targetUnsafe = row match {
      case _: UnsafeRow =>
        if (setters == null) {
          setters = Array.ofDim[UnsafeSetter](exprArray.length)
          for (i <- exprArray.indices) {
            setters(i) = exprArray(i).dataType match {
              case IntegerType => (target: UnsafeRow, value: Any) =>
                target.setInt(i, value.asInstanceOf[Int])
              case LongType => (target: UnsafeRow, value: Any) =>
                target.setLong(i, value.asInstanceOf[Long])
              case DoubleType => (target: UnsafeRow, value: Any) =>
                target.setDouble(i, value.asInstanceOf[Double])
              case FloatType => (target: UnsafeRow, value: Any) =>
                target.setFloat(i, value.asInstanceOf[Float])
              case NullType => (target: UnsafeRow, value: Any) =>
                target.setNullAt(i)
              case BooleanType => (target: UnsafeRow, value: Any) =>
                target.setBoolean(i, value.asInstanceOf[Boolean])
              case ByteType => (target: UnsafeRow, value: Any) =>
                target.setByte(i, value.asInstanceOf[Byte])
              case ShortType => (target: UnsafeRow, value: Any) =>
                target.setShort(i, value.asInstanceOf[Short])
            }
          }
        }
        true
      case _ => false
    }

    this
  }

  override def apply(input: InternalRow): InternalRow = {
    var i = 0
    while (i < exprArray.length) {
      // Store the result into buffer first, to make the projection atomic (needed by aggregation)
      buffer(i) = exprArray(i).eval(input)
      i += 1
    }
    i = 0
    while (i < exprArray.length) {
      if (targetUnsafe) {
        setters(i)(mutableRow.asInstanceOf[UnsafeRow], buffer(i))
      } else {
        mutableRow(i) = buffer(i)
      }
      i += 1
    }
    mutableRow
  }
}

/**
 * A projection that returns UnsafeRow.
 *
 * CAUTION: the returned projection object should *not* be assumed to be thread-safe.
 */
abstract class UnsafeProjection extends Projection {
  override def apply(row: InternalRow): UnsafeRow
}

/**
 * The factory object for `UnsafeProjection`.
 */
object UnsafeProjection
    extends CodeGeneratorWithInterpretedFallback[Seq[Expression], UnsafeProjection] {

  override protected def createCodeGeneratedObject(in: Seq[Expression]): UnsafeProjection = {
    GenerateUnsafeProjection.generate(in)
  }

  override protected def createInterpretedObject(in: Seq[Expression]): UnsafeProjection = {
    InterpretedUnsafeProjection.createProjection(in)
  }

  protected def toBoundExprs(
      exprs: Seq[Expression],
      inputSchema: Seq[Attribute]): Seq[Expression] = {
    exprs.map(BindReferences.bindReference(_, inputSchema))
  }

  protected def toUnsafeExprs(exprs: Seq[Expression]): Seq[Expression] = {
    exprs.map(_ transform {
      case CreateNamedStruct(children) => CreateNamedStructUnsafe(children)
    })
  }

  /**
   * Returns an UnsafeProjection for given StructType.
   *
   * CAUTION: the returned projection object is *not* thread-safe.
   */
  def create(schema: StructType): UnsafeProjection = create(schema.fields.map(_.dataType))

  /**
   * Returns an UnsafeProjection for given Array of DataTypes.
   *
   * CAUTION: the returned projection object is *not* thread-safe.
   */
  def create(fields: Array[DataType]): UnsafeProjection = {
    create(fields.zipWithIndex.map(x => BoundReference(x._2, x._1, true)))
  }

  /**
   * Returns an UnsafeProjection for given sequence of bound Expressions.
   */
  def create(exprs: Seq[Expression]): UnsafeProjection = {
    createObject(toUnsafeExprs(exprs))
  }

  def create(expr: Expression): UnsafeProjection = create(Seq(expr))

  /**
   * Returns an UnsafeProjection for given sequence of Expressions, which will be bound to
   * `inputSchema`.
   */
  def create(exprs: Seq[Expression], inputSchema: Seq[Attribute]): UnsafeProjection = {
    create(toBoundExprs(exprs, inputSchema))
  }

  /**
   * Same as other create()'s but allowing enabling/disabling subexpression elimination.
   * The param `subexpressionEliminationEnabled` doesn't guarantee to work. For example,
   * when fallbacking to interpreted execution, it is not supported.
   */
  def create(
      exprs: Seq[Expression],
      inputSchema: Seq[Attribute],
      subexpressionEliminationEnabled: Boolean): UnsafeProjection = {
    val unsafeExprs = toUnsafeExprs(toBoundExprs(exprs, inputSchema))
    try {
      GenerateUnsafeProjection.generate(unsafeExprs, subexpressionEliminationEnabled)
    } catch {
      case NonFatal(_) =>
        // We should have already seen the error message in `CodeGenerator`
        logWarning("Expr codegen error and falling back to interpreter mode")
        InterpretedUnsafeProjection.createProjection(unsafeExprs)
    }
  }
}

/**
 * A projection that could turn UnsafeRow into GenericInternalRow
 */
object FromUnsafeProjection {

  /**
   * Returns a Projection for given StructType.
   */
  def apply(schema: StructType): Projection = {
    apply(schema.fields.map(_.dataType))
  }

  /**
   * Returns an UnsafeProjection for given Array of DataTypes.
   */
  def apply(fields: Seq[DataType]): Projection = {
    create(fields.zipWithIndex.map(x => new BoundReference(x._2, x._1, true)))
  }

  /**
   * Returns a Projection for given sequence of Expressions (bounded).
   */
  private def create(exprs: Seq[Expression]): Projection = {
    GenerateSafeProjection.generate(exprs)
  }
}
