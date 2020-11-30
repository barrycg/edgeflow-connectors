package org.apache.spark.util.jdbc

import java.sql.{Connection, PreparedStatement}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.jdbc.JdbcDialect
import org.apache.spark.sql.types.{StructField, StructType}

trait DeleteBuilder {

  def statement(conn: Connection,
                table: String,
                dialect: JdbcDialect,
                criteriaFields: Option[Seq[StructField]],
                schema: StructType,
                isCaseSensitive: Boolean): Option[DeleteInfo]
}

case class DeleteInfo(stmt: PreparedStatement, schema: StructType)

object DeleteBuilder {

  def forDriver(driver: String): DeleteBuilder = GenericDeleteBuilder
}

object GenericDeleteBuilder extends DeleteBuilder with Logging {
  override def statement(conn: Connection,
                         table: String,
                         dialect: JdbcDialect,
                         whereFields: Option[Seq[StructField]],
                         schema: StructType,
                         isCaseSensitive: Boolean): Option[DeleteInfo] = {

    whereFields match {
      case Some(id) => {
        val whereClause = whereFields.getOrElse(List.empty)
          .toArray.map(f => dialect.quoteIdentifier(f.name) + " = ?").mkString(" AND ")
        val sql =
          s"""
             |DELETE FROM ${table} WHERE ${whereClause}
             |""".stripMargin

        log.info(s"Using delete sql $sql")

        val deleteSchema = StructType(schema.fields.filter(k => id.map(f => f.name).toSet.contains(k.name)))
        Option(DeleteInfo(conn.prepareStatement(sql), deleteSchema))
      }
      case None => {
        None
      }
    }

  }
}

