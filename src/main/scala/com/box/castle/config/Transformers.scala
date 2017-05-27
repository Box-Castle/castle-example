package com.box.castle.config

import java.util.concurrent.TimeUnit

import org.joda.time.Duration

import scala.concurrent.duration.FiniteDuration

import scala.collection.JavaConverters._

object Transformers {
  def asString(value: Object): String = value.asInstanceOf[String]

  def asFiniteDuration(timeUnit: TimeUnit = TimeUnit.MILLISECONDS): Object => FiniteDuration =
    (value: Object) => FiniteDuration(asLong(value), timeUnit)

  def asFiniteDurationMs(value: Object): FiniteDuration =
    FiniteDuration(asLong(value), TimeUnit.MILLISECONDS)

  def asDurationMs(value: Object): Duration =
    new Duration(asLong(value))

  def asInteger(value: Object): Integer =
    try {
      value match {
        case _: Integer => value.asInstanceOf[Integer]
        case _ => asString(value).toInt
      }
    } catch {
      case e: NumberFormatException => throw new NumberFormatException(s"$value cannot be converted to integer")
    }

  def asDouble(value: Object): Double =
    try {
      value match {
        case _: java.lang.Double => value.asInstanceOf[java.lang.Double]
        case _: Integer => value.asInstanceOf[Integer].toDouble
        case _: java.lang.Long => value.asInstanceOf[java.lang.Long].toDouble
        case _ => asString(value).toInt
      }
    } catch {
      case e: NumberFormatException => throw new NumberFormatException(s"$value cannot be converted to integer")
    }

  def asLong(value: Object): Long =
    try {
      value match {
        case _: java.lang.Long => value.asInstanceOf[java.lang.Long]
        case _: Integer => value.asInstanceOf[Integer].toLong
        case _ => asString(value).toLong
      }
    } catch {
      case e: NumberFormatException => throw new NumberFormatException(s"$value cannot be converted to long")
    }

  def asBoolean(value: Object): Boolean = {
    try {
      value match {
        case _: java.lang.Boolean => value.asInstanceOf[java.lang.Boolean]
        case _ => asString(value).toBoolean
      }
    } catch {
      case e: IllegalArgumentException =>
        throw new IllegalArgumentException(s"$value cannot be converted to Boolean")
    }
  }

  def asArrayList[T](value: Object): List[T] = {
    value.asInstanceOf[java.util.ArrayList[T]].asScala.toList
  }
}
