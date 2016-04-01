package com.asto.dmp.mgtevl.dao

class SQL(private var _select: String, private var _where: String) {
  private var _orderBy: String = _
  private var _limit: Integer = _

  def this(select: String) {
    this(select, null)
  }

  def this() {
    this("*")
  }

  def select(select: String): this.type = {
    this._select = select
    this
  }

  def select = _select

  def where(where: String): this.type = {
    this._where = where
    this
  }

  def where = _where

  def orderBy(orderBy: String): this.type = {
    this._orderBy = orderBy
    this
  }

  def orderBy = _orderBy

  def limit(limit: Integer): this.type = {
    this._limit = limit
    this
  }

  def limit = _limit
}

object SQL {
  def apply() = new SQL()
  def apply(select: String) = new SQL(select)
  def apply(select: String, where: String) = new SQL(select, where)
}