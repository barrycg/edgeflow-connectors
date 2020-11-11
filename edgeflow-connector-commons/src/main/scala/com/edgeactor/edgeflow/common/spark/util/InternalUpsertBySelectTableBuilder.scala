package com.edgeactor.edgeflow.common.spark.util

import org.apache.spark.internal.Logging
import org.apache.spark.sql.jdbc.JdbcDialect
import org.apache.spark.sql.types.{StructField, StructType}

trait InternalUpsertBySelectTableBuilder {

  def statement(sinkTable: String,
                sourceTable: String,
                dialect: JdbcDialect,
                keyFields: Option[Seq[StructField]],
                schema: StructType,
                isCaseSensitive: Boolean): Option[InternalUpsertBySelectTableInfo]
}

case class InternalUpsertBySelectTableInfo(statement: String, schema: StructType)


object InternalInsertBySelectTableBuilder {
  val b = Map("mysql" -> GenericInternalInsertBySelectTableBuilder,
    "postgresql" -> GenericInternalInsertBySelectTableBuilder,
    "oracle" -> GenericInternalInsertBySelectTableBuilder)

  def forDriver(driver: String): InternalUpsertBySelectTableBuilder = {
    val builder = b.filterKeys(k => driver.toLowerCase().contains(k.toLowerCase()))
    require(builder.size == 1, "No upsert dialect registered for " + driver)
    builder.head._2
  }

}

object GenericInternalInsertBySelectTableBuilder extends InternalUpsertBySelectTableBuilder with Logging {
  override def statement(sinkTable: String,
                         sourceTable: String,
                         dialect: JdbcDialect,
                         keyFields: Option[Seq[StructField]],
                         schema: StructType,
                         isCaseSensitive: Boolean): Option[InternalUpsertBySelectTableInfo] = {
    val keyWhere = keyFields.getOrElse(List.empty).toArray.map(f => s"a.${dialect.quoteIdentifier(f.name)} = b.${dialect.quoteIdentifier(f.name)}").mkString(" AND ")
    keyFields match {
      case Some(key) => {
        val columns = schema.fields.map(f => dialect.quoteIdentifier(f.name)).mkString(",")
        val sql =
          s"""
             |INSERT INTO ${sinkTable} (${columns})
             |SELECT ${columns}
             |FROM ${sourceTable} a
             |WHERE NOT EXISTS(SELECT 1 FROM ${sinkTable} b WHERE ${keyWhere})
             |""".stripMargin
        log.info(s"Using insert sql $sql")
        Some(InternalUpsertBySelectTableInfo(sql, schema))
      }
      case None =>
        None
    }
  }
}


object InternalUpdateBySelectTableBuilder {
  val b = Map("mysql" -> GenericInternalUpdateBySelectTableBuilder,
    "postgresql" -> GenericInternalUpdateBySelectTableBuilder,
    "oracle" -> GenericInternalUpdateBySelectTableBuilder)

  def forDriver(driver: String): InternalUpsertBySelectTableBuilder = {
    val builder = b.filterKeys(k => driver.toLowerCase().contains(k.toLowerCase()))
    require(builder.size == 1, "No upsert dialect registered for " + driver)
    builder.head._2
  }

}

object GenericInternalUpdateBySelectTableBuilder extends InternalUpsertBySelectTableBuilder with Logging {
  override def statement(sinkTable: String,
                         sourceTable: String,
                         dialect: JdbcDialect,
                         keyFields: Option[Seq[StructField]],
                         schema: StructType,
                         isCaseSensitive: Boolean): Option[InternalUpsertBySelectTableInfo] = {
    val keyWhere = keyFields.getOrElse(List.empty).toArray.map(f => s"${sourceTable}.${dialect.quoteIdentifier(f.name)}=${sinkTable}.${dialect.quoteIdentifier(f.name)}").mkString(" AND ")
    keyFields match {
      case Some(key) => {
        //        val columns = schema.fields.map(f => dialect.quoteIdentifier(f.name)).mkString(",")
        val updateSchema = StructType(schema.fields.filterNot(k => key.map(f => f.name).toSet.contains(k.name)))
        val updateColumns = updateSchema.fields.map(f => dialect.quoteIdentifier(f.name)).mkString(",")
        val setClause = updateSchema.fields.map(f => s"${dialect.quoteIdentifier(f.name)}=${sourceTable}.${dialect.quoteIdentifier(f.name)}").mkString(",")
        val sql =
          s"""
             |UPDATE ${sinkTable}
             |SET ${setClause}
             |FROM ${sourceTable}
             |WHERE ${keyWhere}
             |""".stripMargin
        log.info(s"Using update sql $sql")
        Some(InternalUpsertBySelectTableInfo(sql, updateSchema))
      }
      case None =>
        None
    }
  }
}
