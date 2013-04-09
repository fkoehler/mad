package com.github.mad

import scala.language.implicitConversions
import org.joda.time.DateTime


case object Bson {
  def doc(elements: (String, BsonElement)*): BsonDoc = new BsonDoc(elements)

  def arr(elements: BsonElement*): BsonArray = new BsonArray(elements)
}

trait ToBsonElement[-T] {
  def toBson(e: T): BsonElement
}

trait FromBsonElement[T] {
  def fromBson(d: BsonElement): T
}

trait ToBsonDoc[T] {
  def toBson(m: T): BsonDoc
}

trait FromBsonDoc[T] {
  def fromBson(d: BsonDoc): T
}

trait BsonImplicits extends ToBsonImplicits with FromBsonImplicits

trait ToBsonImplicits {

  implicit def seqToBsonElement[T](implicit c: ToBsonElement[T]) = new ToBsonElement[Seq[T]] {
    def toBson(v: Seq[T]): BsonElement = new BsonArray(v.map(c.toBson(_)))
  }

  implicit def mapToBsonElement[T](implicit c: ToBsonElement[T]) = new ToBsonElement[Map[String, T]] {
    def toBson(v: Map[String, T]): BsonElement = v.foldLeft(Bson.doc())((doc, kv) => doc + (kv._1 -> kv._2))
  }

  implicit def optionToBsonElement[T](implicit c: ToBsonElement[T]) = new ToBsonElement[Option[T]] {
    def toBson(v: Option[T]): BsonElement = v match {
      case Some(v) => v
      case None => BsonNull
    }
  }

  implicit def anyType2BsonElement[T](element: T)(implicit c: ToBsonElement[T]): BsonElement = c.toBson(element)

  implicit object StringToBsonElement extends ToBsonElement[String] {
    def toBson(v: String): BsonElement = new BsonString(v)
  }

  implicit object IntToBsonElement extends ToBsonElement[Int] {
    def toBson(v: Int): BsonElement = new BsonInt(v)
  }

  implicit object LongToBsonElement extends ToBsonElement[Long] {
    def toBson(v: Long): BsonElement = new BsonLong(v)
  }

  implicit object DoubleToBsonElement extends ToBsonElement[Double] {
    def toBson(v: Double): BsonElement = new BsonDouble(v)
  }

  implicit object JodaDateTimeToBsonElement extends ToBsonElement[DateTime] {
    def toBson(v: DateTime): BsonElement = new BsonDateTime(v)
  }

  implicit object BooleanToBsonElement extends ToBsonElement[Boolean] {
    def toBson(v: Boolean): BsonElement = new BsonBoolean(v)
  }

}

trait FromBsonImplicits {

  implicit def seqFromBsonElement[T](implicit c: FromBsonElement[T]) = new FromBsonElement[Seq[T]] {
    def fromBson(v: BsonElement): Seq[T] = v.asInstanceOf[BsonArray].elements.map(c.fromBson(_))
  }

  implicit def listFromBsonElement[T](implicit c: FromBsonElement[T]) = new FromBsonElement[List[T]] {
    def fromBson(v: BsonElement): List[T] = v.asInstanceOf[BsonArray].elements.map(c.fromBson(_)).toList
  }

  implicit def mapFromBsonElement[T](implicit c: FromBsonElement[T]) = new FromBsonElement[Map[String, T]] {
    def fromBson(v: BsonElement): Map[String, T] = v.asInstanceOf[BsonDoc].elements.foldLeft(Map[String, T]())((map, kv) => map + (kv._1 -> c.fromBson(kv._2)))
  }

  implicit def optionFromBsonElement[T](implicit c: FromBsonElement[T]) = new FromBsonElement[Option[T]] {
    def fromBson(v: BsonElement): Option[T] = v match {
      case BsonNull => None
      case e: BsonElement => Some(c.fromBson(e))
    }
  }

  implicit def anyTypeFromBsonElement[T](element: BsonElement)(implicit c: FromBsonElement[T]): T = c.fromBson(element)

  implicit object StringFromBsonElement extends FromBsonElement[String] {
    def fromBson(v: BsonElement): String = v.asInstanceOf[BsonString].value
  }

  implicit object IntFromBsonElement extends FromBsonElement[Int] {
    def fromBson(v: BsonElement): Int = v.asInstanceOf[BsonInt].value
  }

  implicit object LongFromBsonElement extends FromBsonElement[Long] {
    def fromBson(v: BsonElement): Long = v.asInstanceOf[BsonLong].value
  }

  implicit object DoubleFromBsonElement extends FromBsonElement[Double] {
    def fromBson(v: BsonElement): Double = v.asInstanceOf[BsonDouble].value
  }

  implicit object JodaDateTimeFromBsonElement extends FromBsonElement[DateTime] {
    def fromBson(v: BsonElement): DateTime = v.asInstanceOf[BsonDateTime].value
  }

  implicit object BooleanFromBsonElement extends FromBsonElement[Boolean] {
    def fromBson(v: BsonElement): Boolean = v.asInstanceOf[BsonBoolean].value
  }

}

case class BsonDoc(elements: Seq[(String, BsonElement)]) extends BsonElement {
  /** merge with other doc */
  def ++(doc: BsonDoc): BsonDoc = BsonDoc(elements ++ doc.elements)

  /** add key, value pair */
  def +(keyValue: (String, BsonElement)): BsonDoc = BsonDoc(elements :+ keyValue)

  def keys: Seq[String] = elements.map(_._1)

  def get(key: String): BsonElement = elements.find(kv => kv._1 == key).get._2

  def getAs[T](key: String): T = get(key).asInstanceOf[T]

  def apply[T](key: String)(implicit c: FromBsonElement[T]): T = as(key)

  def as[T](key: String)(implicit c: FromBsonElement[T]): T = c.fromBson(get(key))

  def asOpt[T](key: String)(implicit c: FromBsonElement[T]): Option[T] = elements.find(kv => kv._1 == key) match {
    case Some(kv) => Some(c.fromBson(kv._2))
    case None => None
  }

  override def toString() = toStr()

  private def toStr(level: Int = 0): String = elements.foldLeft("") {
    (s, kv) =>
      s + s"${"  " * level}${kv._1}: " + (if (kv._2.isInstanceOf[BsonDoc]) {
        kv._2.asInstanceOf[BsonDoc].toStr(level + 1)
      } else {
        kv._2.toString
      })
  }

}
case class BsonArray(elements: Seq[BsonElement]) extends BsonElement {
  def asDocs: Seq[BsonDoc] = elements.map(_.asInstanceOf[BsonDoc])
  /** append to array */
  def :+(element: BsonElement): BsonArray = BsonArray(elements :+ element)
}
case class BsonInt(value: Int) extends BsonElement
case class BsonLong(value: Long) extends BsonElement
case class BsonString(value: String) extends BsonElement
case class BsonDouble(value: Double) extends BsonElement
case class BsonDateTime(value: DateTime) extends BsonElement
case class BsonBoolean(value: Boolean) extends BsonElement
case object BsonNull extends BsonElement

abstract class BsonElement {
  def getAs[T]: T = this.asInstanceOf[T]
  def as[T](implicit c: FromBsonElement[T]): T = c.fromBson(this)
}