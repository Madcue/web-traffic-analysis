package com.twq.spark.web

import scala.collection.IterableLike
import scala.collection.generic.CanBuildFrom

/**
  * Builds a new sequence from this sequence without any duplicate elements. Note: will not terminate for infinite-sized collections.
  * Returns:
  * A new sequence which contains the first occurrence of every element of this sequence.
  * def distinct: Repr = {
  * val b = newBuilder
  * val seen = mutable.HashSet[A]()
  * for (x <- this) {
  * if (!seen(x)) {
  * b += x
  * seen += x
  * }
  * }
  * b.result()
  * }
  * @param xs
  * @tparam A
  * @tparam Repr
  *
  */

class RichCollection[A, Repr](val xs: IterableLike[A, Repr]) extends AnyVal {
  def distinctBy[B, That](f: A => B)(implicit cbf: CanBuildFrom[Repr, A, That]) = {
    val builder = cbf(xs.repr)
    val i = xs.iterator
    var set = Set[B]()
    while (i.hasNext) {
      val o = i.next()
      val b = f(o)
      if (!set(b)) { //如果set中没有b
        set += b
        builder += o //构建器添加 o
      }
    }
    builder.result()
  }
}

object RichCollection { //隐式的给RichCollection添加val xs: IterableLike[A, Repr],并实例
  implicit def toRichCollection[A, Repr](xs: IterableLike[A, Repr]) = new RichCollection(xs)
}
