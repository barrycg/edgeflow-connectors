package com.edgeactor.edgeflow.common.spark.util

import java.sql.{BatchUpdateException, Connection}
import java.util.Properties

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, Row, SaveMode}
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCOptions, JdbcOptionsInWrite, JdbcUtils}
import org.apache.spark.sql.jdbc.{JdbcDialect, JdbcDialects, JdbcType}
import org.apache.spark.sql.types.{ArrayType, BinaryType, BooleanType, ByteType, DataType, DateType, DecimalType, DoubleType, FloatType, IntegerType, LongType, ShortType, StringType, StructField, StructType, TimestampType}

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

object SparkJdbcUtils extends Logging {

  def internalInsertBySelectTable(schema: StructType,
                                  sinkTable: String,
                                  sourceTable: String,
                                  keyCols: Option[String],
                                  url: String,
                                  properties: Properties): Unit = {

    val jdbcOptions = new JDBCOptions(url, sinkTable, properties.asScala.toMap)
    val conn = JdbcUtils.createConnectionFactory(jdbcOptions)()

    val insertBuilder = InternalInsertBySelectTableBuilder.forDriver(conn.getMetaData.getDriverName)
    internalUpsertBySelectTable(schema,
                                    sinkTable,
                                    sourceTable,
                                    keyCols,
                                    insertBuilder,
                                    url,
                                    properties)
  }

  def internalUpdateBySelectTable(schema: StructType,
                                  sinkTable: String,
                                  sourceTable: String,
                                  keyCols: Option[String],
                                  url: String,
                                  properties: Properties): Unit = {

    val jdbcOptions = new JDBCOptions(url, sinkTable, properties.asScala.toMap)
    val conn = JdbcUtils.createConnectionFactory(jdbcOptions)()

    val updateBuilder = InternalUpdateBySelectTableBuilder.forDriver(conn.getMetaData.getDriverName)
    internalUpsertBySelectTable(schema,
                                sinkTable,
                                sourceTable,
                                keyCols,
                                updateBuilder,
                                url,
                                properties)
  }

  def internalUpsertBySelectTable(schema: StructType,
                                  sinkTable: String,
                                  sourceTable: String,
                                  keyCols: Option[String],
                                  internalUpsertBySelectTableBuilder: InternalUpsertBySelectTableBuilder,
                                  url: String,
                                  properties: Properties): Unit = {
    val jdbcOptions = new JDBCOptions(url, sinkTable, properties.asScala.toMap)
    val conn = JdbcUtils.createConnectionFactory(jdbcOptions)()
    val keyFields = keyCols.map(f => schema.filter(s => f.split(",").contains(s.name)))
    val dialect = JdbcDialects.get(jdbcOptions.url)
    val isCaseSensitive = false
    try {
      val _upsertStmt = internalUpsertBySelectTableBuilder
        .statement(sinkTable, sourceTable, dialect, keyFields, schema, isCaseSensitive)

      _upsertStmt match {
        case None => log.warn("Insert sql is None")
        case Some(upsertStmt) => {
          val upsertSql = upsertStmt.statement
          val stmt = conn.prepareStatement(upsertSql)
          try {
            stmt.execute()
          } catch {
            case jdbce: BatchUpdateException => jdbce.getNextException().printStackTrace()
          } finally {
            stmt.close()
          }
        }
      }
    } finally {
      try {
        conn.close()
      } catch {
        case e: Exception => log.warn("Conn closing failed", e)
      }
    }
  }

  def upsert(df: DataFrame,
             idCol: Option[String],
             url: String,
             table: String,
             properties: Properties,
             mode: SaveMode,
             batchSize: Int) {
    val jdbcOptions = new JDBCOptions(url, table, properties.asScala.toMap)
    val idColumn = idCol.map(f => df.schema.filter(s => f.split(",")
      .map { it => it.trim() }.contains(s.name)))
    log.info("key columns: ")
    log.info(idColumn.toString)
    //    val table = jdbcOptions.tableOrQuery
    val conn = JdbcUtils.createConnectionFactory(jdbcOptions)()
    //    val isCaseSensitive = df.sqlContext.conf.caseSensitiveAnalysis
    val isCaseSensitive = false
    val writeOption = new JdbcOptionsInWrite(url, table, jdbcOptions.parameters)

    try {
      var tableExists = JdbcUtils.tableExists(conn, writeOption)
      if (mode == SaveMode.Ignore && tableExists) {
        return
      }

      if (mode == SaveMode.ErrorIfExists && tableExists) {
        sys.error(s"Table $table already exists.")
      }

      if (mode == SaveMode.Overwrite && tableExists) {
        JdbcUtils.dropTable(conn, table, writeOption)
        tableExists = false
      }

      if (!tableExists) {
        val schema = JdbcUtils.schemaString(df, url, jdbcOptions.createTableColumnTypes)
        val dialect = JdbcDialects.get(url)
        val pk = idColumn.map { f =>
          val key = f.map(c => s"${dialect.quoteIdentifier(c.name)}").mkString(",")
          s",primary key(${key})"
        }.getOrElse("")

        val sql = s"CREATE TABLE $table ( $schema $pk )"
        log.info("Create Table SQL:")
        log.info(sql)
        val statement = conn.createStatement
        try {
          statement.executeUpdate(sql)
        } finally {
          statement.close()
        }
      }
    } finally {
      conn.close()
    }

    idColumn match {
      case Some(id) => upsert(df, idColumn, jdbcOptions, isCaseSensitive, batchSize)
      case None => JdbcUtils.saveTable(df, Some(df.schema), isCaseSensitive, options = writeOption)
    }
  }

  def upsert(df: DataFrame,
             idCol: Option[Seq[StructField]],
             jdbcOptions: JDBCOptions,
             isCaseSensitive: Boolean,
             batchSize: Int): Unit = {

    val dialect = JdbcDialects.get(jdbcOptions.url)
    val nullTypes: Array[Int] = df.schema.fields.map { field =>
      getJdbcType(field.dataType, dialect).jdbcNullType
    }

    val rddSchema = df.schema
    val getConnection: () => Connection = JdbcUtils.createConnectionFactory(jdbcOptions)
    df.foreachPartition { iterator =>
      upsertPartition(getConnection, jdbcOptions.tableOrQuery, iterator, idCol, rddSchema, nullTypes, batchSize,
        dialect, isCaseSensitive)
    }
  }

  private def getJdbcType(dt: DataType, dialect: JdbcDialect): JdbcType = {
    dialect.getJDBCType(dt).orElse(JdbcUtils.getCommonJDBCType(dt)).getOrElse(
      throw new IllegalArgumentException(s"Can't get JDBC type for ${
        dt.simpleString
      }")
    )
  }

  /**
    * Saves a partition of a DataFrame to the JDBC database.  This is done in
    * a single database transaction in order to avoid repeatedly inserting
    * data as much as possible.
    *
    * It is still theoretically possible for rows in a DataFrame to be
    * inserted into the database more than once if a stage somehow fails after
    * the commit occurs but before the stage can return successfully.
    *
    * This is not a closure inside saveTable() because apparently cosmetic
    * implementation changes elsewhere might easily render such a closure
    * non-Serializable.  Instead, we explicitly close over all variables that
    * are used.
    */
  def upsertPartition(
                       getConnection: () => Connection,
                       table: String,
                       iterator: Iterator[Row],
                       idColumn: Option[Seq[StructField]],
                       rddSchema: StructType,
                       nullTypes: Array[Int],
                       batchSize: Int,
                       dialect: JdbcDialect,
                       isCaseSensitive: Boolean
                     ): Iterator[Byte] = {
    val conn = getConnection()
    var committed = false
    val supportsTransactions = try {
      conn.getMetaData.supportsDataManipulationTransactionsOnly() ||
        conn.getMetaData.supportsDataDefinitionAndDataManipulationTransactions()
    } catch {
      case NonFatal(e) =>
        log.warn("Exception while detecting transaction support", e)
        true
    }

    try {
      if (supportsTransactions) {
        conn.setAutoCommit(false) // Everything in the same db transaction.
      }
      val upsert = UpsertBuilder.forDriver(conn.getMetaData.getDriverName)
        .upsertStatement(conn, table, dialect, idColumn, rddSchema, isCaseSensitive)

      val stmt = upsert.stmt
      val uschema = upsert.schema

      try {
        var rowCount = 0
        while (iterator.hasNext) {
          val row = iterator.next()
          val numFields = uschema.fields.length
          uschema.fields.zipWithIndex.foreach {
            case (f, idx) =>
              val i = row.fieldIndex(f.name)
              if (row.isNullAt(i)) {
                stmt.setNull(idx + 1, nullTypes(i))
              } else {
                uschema.fields(i).dataType match {
                  case IntegerType => stmt.setInt(idx + 1, row.getInt(i))
                  case LongType => stmt.setLong(idx + 1, row.getLong(i))
                  case DoubleType => stmt.setDouble(idx + 1, row.getDouble(i))
                  case FloatType => stmt.setFloat(idx + 1, row.getFloat(i))
                  case ShortType => stmt.setInt(idx + 1, row.getShort(i))
                  case ByteType => stmt.setInt(idx + 1, row.getByte(i))
                  case BooleanType => stmt.setBoolean(idx + 1, row.getBoolean(i))
                  case StringType => stmt.setString(idx + 1, row.getString(i))
                  case BinaryType => stmt.setBytes(idx + 1, row.getAs[Array[Byte]](i))
                  case TimestampType => stmt.setTimestamp(idx + 1, row.getAs[java.sql.Timestamp](i))
                  case DateType => stmt.setDate(idx + 1, row.getAs[java.sql.Date](i))
                  case t: DecimalType => stmt.setBigDecimal(idx + 1, row.getDecimal(i))
                  case ArrayType(et, _) =>
                    val array = conn.createArrayOf(
                      getJdbcType(et, dialect).databaseTypeDefinition.toLowerCase,
                      row.getSeq[AnyRef](i).toArray
                    )
                    stmt.setArray(idx + 1, array)
                  case _ => throw new IllegalArgumentException(
                    s"Can't translate non-null value for field $i"
                  )
                }
              }

          }
          stmt.addBatch()
          rowCount += 1
          if (rowCount % batchSize == 0) {
            stmt.executeBatch()
            conn.commit()
            rowCount = 0
          }
        }
        if (rowCount > 0) {
          stmt.executeBatch()
        }
      } catch {
        case jdbce: BatchUpdateException => jdbce.getNextException().printStackTrace()
      } finally {
        stmt.close()
      }
      if (supportsTransactions) {
        conn.commit()
      }
      committed = true
    } finally {
      if (!committed) {
        // The stage must fail.  We got here through an exception path, so
        // let the exception through unless rollback() or close() want to
        // tell the user about another problem.
        if (supportsTransactions) {
          conn.rollback()
        }
        conn.close()
      } else {
        // The stage must succeed.  We cannot propagate any exception close() might throw.
        try {
          conn.close()
        } catch {
          case e: Exception => log.warn("Transaction succeeded, but closing failed", e)
        }
      }
    }
    Array[Byte]().iterator
  }


  def tableExists(url: String, table: String, properties: Properties): Boolean = {
    val jdbcOptions = new JDBCOptions(url, table, properties.asScala.toMap)
    val conn = JdbcUtils.createConnectionFactory(jdbcOptions)()
    val writeOption = new JdbcOptionsInWrite(url, table, jdbcOptions.parameters)
    JdbcUtils.tableExists(conn, writeOption)
  }

  def createTable(df: DataFrame, keyCols: Option[String], url: String, table: String, properties: Properties, forceIfExists: Boolean): Unit = {
    val jdbcOptions = new JDBCOptions(url, table, properties.asScala.toMap)
    val certainKeyCols = keyCols.map(f => df.schema.filter(s => f.split(",").contains(s.name)))
    val conn = JdbcUtils.createConnectionFactory(jdbcOptions)()
    val writeOption = new JdbcOptionsInWrite(url, table, jdbcOptions.parameters)

    try {
      val tableExists = JdbcUtils.tableExists(conn, writeOption)
      if (tableExists && !forceIfExists) {
        log.info(s"表 $table 已存在，且不强制重建，则跳过")
        return
      } else if (tableExists && forceIfExists) {
        val dropSql = s"DROP TABLE $table"
        val stmt = conn.createStatement
        try {
          stmt.execute(dropSql)
        } finally {
          stmt.close()
        }
      }
      val schema = JdbcUtils.schemaString(df, url, jdbcOptions.createTableColumnTypes)
      val dialect = JdbcDialects.get(url)
      val pk = certainKeyCols.map { f =>
        val key = f.map(c => s"${dialect.quoteIdentifier(c.name)}").mkString(",")
        s",primary key(${key})"
      }.getOrElse("")
      val sql = s"CREATE TABLE $table ( $schema $pk)"
      log.info("Create table SQL:")
      log.info(sql)
      val statement = conn.createStatement
      try {
        statement.execute(sql)
      } finally {
        statement.close()
      }
    } finally {
      conn.close()
    }
  }

}
