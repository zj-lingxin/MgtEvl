package com.asto.dmp.mgtevl.util

import org.apache.spark.Logging
import org.apache.spark.rdd.{CoGroupedRDD, RDD}
import scala.reflect.ClassTag

class RichPairRDD[K, V](self: RDD[(K, V)])
                       (implicit kt: ClassTag[K], vt: ClassTag[V])
  extends Logging with Serializable {

  import org.apache.spark.Partitioner.defaultPartitioner

  def cogroup[W1, W2, W3, W4, W5](other1: RDD[(K, W1)], other2: RDD[(K, W2)],
                                  other3: RDD[(K, W3)], other4: RDD[(K, W4)], other5: RDD[(K, W5)])
  : RDD[(K, (Iterable[V], Iterable[W1], Iterable[W2], Iterable[W3], Iterable[W4], Iterable[W5]))] = {
    val cg = new CoGroupedRDD[K](
      Seq(self, other1, other2, other3, other4, other5),
      defaultPartitioner(self, other1, other2, other3, other4, other5))
    cg.mapValues { case Array(vs, w1s, w2s, w3s, w4s, w5s) =>
      (vs.asInstanceOf[Iterable[V]],
        w1s.asInstanceOf[Iterable[W1]],
        w2s.asInstanceOf[Iterable[W2]],
        w3s.asInstanceOf[Iterable[W3]],
        w4s.asInstanceOf[Iterable[W4]],
        w5s.asInstanceOf[Iterable[W5]]
        )
    }
  }

  def cogroup[W1, W2, W3, W4, W5, W6, W7, W8, W9](other1: RDD[(K, W1)], other2: RDD[(K, W2)], other3: RDD[(K, W3)],
                                                  other4: RDD[(K, W4)], other5: RDD[(K, W5)], other6: RDD[(K, W6)],
                                                  other7: RDD[(K, W7)], other8: RDD[(K, W8)], other9: RDD[(K, W9)]
                                                   )
  : RDD[(K, (Iterable[V], Iterable[W1], Iterable[W2], Iterable[W3], Iterable[W4], Iterable[W5], Iterable[W6], Iterable[W7], Iterable[W8], Iterable[W9]))] = {
    val cg = new CoGroupedRDD[K](
      Seq(self, other1, other2, other3, other4, other5, other6, other7, other8, other9),
      defaultPartitioner(self, other1, other2, other3, other4, other5, other6, other7, other8, other9))
    cg.mapValues { case Array(vs, w1s, w2s, w3s, w4s, w5s, w6s, w7s, w8s, w9s) =>
      (vs.asInstanceOf[Iterable[V]],
        w1s.asInstanceOf[Iterable[W1]],
        w2s.asInstanceOf[Iterable[W2]],
        w3s.asInstanceOf[Iterable[W3]],
        w4s.asInstanceOf[Iterable[W4]],
        w5s.asInstanceOf[Iterable[W5]],
        w6s.asInstanceOf[Iterable[W6]],
        w7s.asInstanceOf[Iterable[W7]],
        w8s.asInstanceOf[Iterable[W8]],
        w9s.asInstanceOf[Iterable[W9]]
        )
    }
  }

}

object RichPairRDD {
  implicit def toRichPairRDD[K, V](self: RDD[(K, V)])(implicit kt: ClassTag[K], vt: ClassTag[V]) = new RichPairRDD[K, V](self)
}

object Utils {
  def firstItrAsDouble(iter: Iterable[Double], defaultValue: Any = None) = {
    if (iter.isEmpty) {
      defaultValue
    } else {
      retainDecimal(iter.head, 2)
    }
  }
  def firstItrAsInt(iter: Iterable[Int], defaultValue: Any = None) = {
    if (iter.isEmpty) {
      defaultValue
    } else {
      iter.head
    }
  }


  def firstItr[T](iter: Iterable[T], defaultValue: T = None): T = {
    if (iter.isEmpty) {
      defaultValue
    } else {
      iter.head
    }
  }


  def toProduct[A <: Object](seq: Seq[A]) =
    Class.forName("scala.Tuple" + seq.size).getConstructors.apply(0).newInstance(seq: _*).asInstanceOf[Product]

  def trimIterable[A <: Iterable[String]](iterable: A): A = {
    iterable.map(_.trim).asInstanceOf[A]
  }

  def trimTuple(x: Product) = toProduct((for (e <- x.productIterator) yield {
    e.toString.trim
  }).toList)

  /**
   * 保留小数位数
   */
  def retainDecimal(number: Double, bits: Int = 2): Double = {
    BigDecimal(number).setScale(bits, BigDecimal.RoundingMode.HALF_UP).doubleValue()
  }

}


