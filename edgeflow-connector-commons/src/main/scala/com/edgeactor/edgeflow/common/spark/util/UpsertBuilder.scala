package com.edgeactor.edgeflow.common.spark.util

import java.sql.{Connection, PreparedStatement}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils
import org.apache.spark.sql.jdbc.JdbcDialect
import org.apache.spark.sql.types.{StructField, StructType}


trait UpsertBuilder {

  def upsertStatement(conn: Connection,
                      table: String,
                      dialect: JdbcDialect,
                      idField: Option[Seq[StructField]],
                      schema: StructType,
                      isCaseSensitive: Boolean): UpsertInfo
}


case class UpsertInfo(stmt: PreparedStatement, schema: StructType)

/**
 *
 * @param stmt
 * @param schema The modified schema.  Postgres upserts, for instance, add fields to the SQL, so we update the
 *               schema to reflect that.
 */

object UpsertBuilder {
  val b = Map("mysql" -> MysqlUpsertBuilder,
    "postgresql" -> PostgresUpsertBuilder)

  def forDriver(driver: String): UpsertBuilder = {
    val builder = b.filterKeys(k => driver.toLowerCase().contains(k.toLowerCase()))
    require(builder.size == 1, "No upsert dialect registered for " + driver)
    builder.head._2
  }

}

object PostgresUpsertBuilder extends UpsertBuilder with Logging {
  override def upsertStatement(conn: Connection, table: String, dialect: JdbcDialect, idField: Option[Seq[StructField]],
                               schema: StructType, isCaseSensitive: Boolean): UpsertInfo = {
    val ids = idField.getOrElse(List.empty).toArray.map(f => dialect.quoteIdentifier(f.name)).mkString(",")
    idField match {
      case Some(id) => {
        val columns = schema.fields.map(f => dialect.quoteIdentifier(f.name)).mkString(",")
        val placeholders = schema.fields.map(_ => "?").mkString(",")
        val updateSchema = StructType(schema.fields.filterNot(k => id.map(f => f.name).toSet.contains(k.name)))
        val updateColumns = updateSchema.fields.map(f => dialect.quoteIdentifier(f.name)).mkString(",")
        val updatePlaceholders = updateSchema.fields.map(_ => "?").mkString(",")
        val updateFields = updateColumns.split(",").zip(updatePlaceholders.split(",")).map(f => s"${f._1} = ${f._2}").mkString(",")
        val sql =
          s"""INSERT INTO ${table} ($columns) VALUES ($placeholders)
             |ON CONFLICT (${ids}) DO UPDATE
             |SET ${updateFields}
             |;""".stripMargin
        log.info(s"Using sql $sql")

        val schemaFields = schema.fields ++ updateSchema.fields
        val upsertSchema = StructType(schemaFields)
        UpsertInfo(conn.prepareStatement(sql), upsertSchema)
      }
      case None => {
        UpsertInfo(conn.prepareStatement(JdbcUtils.getInsertStatement(table, schema, None, isCaseSensitive, dialect)), schema)
      }
    }

  }
}

object MysqlUpsertBuilder extends UpsertBuilder with Logging {
  def upsertStatement(conn: Connection, table: String, dialect: JdbcDialect, idField: Option[Seq[StructField]],
                      schema: StructType, isCaseSensitive: Boolean) = {
    idField match {
      case Some(id) => {
        val columns = schema.fields.map(f => dialect.quoteIdentifier(f.name)).mkString(",")
        val placeholders = schema.fields.map(_ => "?").mkString(",")
        val updateSchema = StructType(schema.fields.filterNot(k => id.map(f => f.name).toSet.contains(k.name)))
        val updateColumns = updateSchema.fields.map(f => dialect.quoteIdentifier(f.name)).mkString(",")
        val updatePlaceholders = updateSchema.fields.map(_ => "?").mkString(",")
        val updateFields = updateColumns.split(",").zip(updatePlaceholders.split(",")).map(f => s"${f._1} = ${f._2}").mkString(",")
        val sql =
          s"""insert into ${table} ($columns) values ($placeholders)
             |ON DUPLICATE KEY UPDATE
             |${updateFields}
             |;""".stripMargin

        log.info(s"Using sql $sql")

        val schemaFields = schema.fields ++ updateSchema.fields
        val upsertSchema = StructType(schemaFields)
        UpsertInfo(conn.prepareStatement(sql), upsertSchema)
      }
      case None => {
        UpsertInfo(conn.prepareStatement(JdbcUtils.getInsertStatement(table, schema, None, isCaseSensitive, dialect)), schema)
      }
    }
  }
}

