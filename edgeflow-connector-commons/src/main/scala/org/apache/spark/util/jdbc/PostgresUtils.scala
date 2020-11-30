package org.apache.spark.util.jdbc

import java.io.{BufferedInputStream, BufferedOutputStream, File, FileInputStream, FileOutputStream}
import java.nio.charset.StandardCharsets
import java.sql.{Connection, Date, Timestamp}
import java.util.{Properties, UUID}
import java.util.concurrent.{TimeUnit, TimeoutException}

import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCOptions, JdbcUtils}
import org.apache.spark.sql.types.{BinaryType, BooleanType, ByteType, DataType, DateType, DecimalType, DoubleType, FloatType, IntegerType, LongType, ShortType, StringType, StructType, TimestampType, UserDefinedType}
import org.apache.spark.util.{LongAccumulator, ThreadUtils, Utils}
import org.postgresql.copy.CopyManager
import org.postgresql.core.BaseConnection

import scala.concurrent.Promise
import scala.concurrent.duration.Duration
import scala.util.Try


/**
  *  This util will  apply some unique features belonging to postgres.
  */
object PostgresUtils extends Logging{

  def nonTransactionalCopy(
                            df: DataFrame,
                            schema: StructType,
                            tableName: String,
                            url: String,
                            properties: Properties ): Unit = {

    val jdbcOptions: JDBCOptions = new JDBCOptions(Map("url"->url,
                                        "user"->properties.getProperty("user"),
                                        "password"->properties.getProperty("password"),
                                        "driver"->"org.postgresql.Driver",
                                        "dbtable"->tableName));

    df.foreachPartition { rows =>
      copyPartition(rows, schema, tableName,jdbcOptions )
    }
  }


  /**
    * Copy a partition's data to a postgres table .
    *
    * @param rows        rows of a partition will be copy to the postgres
    * @param schema      the table schema in postgres
    * @param tableName   the tableName, to which the data will be copy
    * @param accumulator account for recording the successful partition num
    */
  def copyPartition( rows: Iterator[Row],
                     schema: StructType,
                     tableName: String,
                     jdbcOptions: JDBCOptions,
                     accumulator: Option[LongAccumulator] = None): Unit = {

    val valueConverters: Array[(Row, Int) => String] =
      schema.map(s => makeConverter(s.dataType)).toArray
    val tmpDir = Utils.createTempDir(Utils.getLocalDir(SparkEnv.get.conf), "postgres")
    val dataFile = new File(tmpDir, UUID.randomUUID().toString)
    logInfo(s"Start to write data to local tmp file: ${dataFile.getCanonicalPath}")
    val out = new BufferedOutputStream(new FileOutputStream(dataFile))
    val startW = System.nanoTime()
    try {
      rows.foreach(r => out.write(
        convertRow(r, schema.length, ";" , valueConverters)))
    } finally {
      out.close()
    }
    val endW = System.nanoTime()
    logInfo(s"Finished writing data to local tmp file: ${dataFile.getCanonicalPath}, " +
      s"time taken: ${(endW - startW) / math.pow(10, 9)}s")
    val in = new BufferedInputStream(new FileInputStream(dataFile))
    val sql = s"COPY $tableName" +
      s" FROM STDIN WITH NULL AS 'NU' DELIMITER AS E'${";"}'"

    val promisedCopyNums = Promise[Long]
    val conn = JdbcUtils.createConnectionFactory(jdbcOptions)()
    val copyManager = new CopyManager(conn.asInstanceOf[BaseConnection])


    val copyThread = new Thread("copy-to-gp-thread") {
      override def run(): Unit = promisedCopyNums.complete(Try(copyManager.copyIn(sql, in)))
    }

    try {
      logInfo(s"Start copy steam to Greenplum with copy command $sql")
      val start = System.nanoTime()
      copyThread.start()
      try {
        val nums = ThreadUtils.awaitResult(promisedCopyNums.future,
          Duration( 50000, TimeUnit.MILLISECONDS))
        val end = System.nanoTime()
        logInfo(s"Copied $nums row(s) to Postgres," +
          s" time taken: ${(end - start) / math.pow(10, 9)}s")
      } catch {
        case _: TimeoutException =>
          throw new TimeoutException(
            s"""
               | The copy operation for copying this partition's data to postgres has been running for
               | more than the timeout: ${TimeUnit.MILLISECONDS.toSeconds(50000)}s.
               | You can configure this timeout with option copyTimeout, such as "2h", "100min",
               | and default copyTimeout is "1h".
               """.stripMargin)
      }
      accumulator.foreach(_.add(1L))
    } finally {
      copyThread.interrupt()
      copyThread.join()
      in.close()
      closeConnSilent(conn)
    }
  }

  def closeConnSilent(conn: Connection): Unit = {
    try {
      conn.close()
    } catch {
      case e: Exception => logWarning("Exception occured when closing connection.", e)
    }
  }

  def convertRow(
                  row: Row,
                  length: Int,
                  delimiter: String,
                  valueConverters: Array[(Row, Int) => String]): Array[Byte] = {
    var i = 0
    val values = new Array[String](length)
    while (i < length) {
      if (!row.isNullAt(i)) {
        values(i) = convertValue(valueConverters(i).apply(row, i), delimiter.charAt(0))
      } else {
        values(i) = "NULL"
      }
      i += 1
    }
    (values.mkString(delimiter) + "\n").getBytes("UTF-8")
  }

  def convertValue(str: String, delimiter: Char): String = {
    str.flatMap {
      case '\\' => "\\\\"
      case '\n' => "\\n"
      case '\r' => "\\r"
      case `delimiter` => s"\\$delimiter"
      case c if c == 0 => "" // If this char is an empty character, drop it.
      case c => s"$c"
    }
  }

  def makeConverter(dataType: DataType): (Row, Int) => String = dataType match {
    case StringType => (r: Row, i: Int) => r.getString(i)
    case BooleanType => (r: Row, i: Int) => r.getBoolean(i).toString
    case ByteType => (r: Row, i: Int) => r.getByte(i).toString
    case ShortType => (r: Row, i: Int) => r.getShort(i).toString
    case IntegerType => (r: Row, i: Int) => r.getInt(i).toString
    case LongType => (r: Row, i: Int) => r.getLong(i).toString
    case FloatType => (r: Row, i: Int) => r.getFloat(i).toString
    case DoubleType => (r: Row, i: Int) => r.getDouble(i).toString
    case DecimalType() => (r: Row, i: Int) => r.getDecimal(i).toString

    case DateType =>
      (r: Row, i: Int) => r.getAs[Date](i).toString

    case TimestampType => (r: Row, i: Int) => r.getAs[Timestamp](i).toString

    case BinaryType => (r: Row, i: Int) =>
      new String(r.getAs[Array[Byte]](i), StandardCharsets.UTF_8)

    case udt: UserDefinedType[_] => makeConverter(udt.sqlType)
    case _ => (row: Row, ordinal: Int) => row.get(ordinal).toString
  }


}
