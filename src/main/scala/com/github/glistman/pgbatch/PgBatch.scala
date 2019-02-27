package com.github.glistman.pgbatch

import java.sql.{Connection, DriverManager}

import scala.io.Source

object PgBatch {

  def main(args: Array[String]): Unit = {
    val batchOffset = sys.env.getOrElse("BATCH_OFFSET", "1").toInt
    val batchSize = sys.env.getOrElse("BATCH_SIZE", "10000").toInt
    val filePath = sys.env("SCRIPT_PATH")
    val tablesToVacuum = sys.env("TABLES_TO_VACUUM")
    val enableVacuum = sys.env("ENABLE_VACUUM").toBoolean
    val executeVacuumEveryNBatches = sys.env("EXECUTE_VACUUM_EVERY_N_BATCHES").toInt
    val pgConnection = PGConnection(
      sys.env("DB_HOST"),
      sys.env("DB_PORT"),
      sys.env("DB_NAME"),
      sys.env("DB_USER"),
      sys.env("DB_PASSWORD")
    )
    val connection = pgConnection.createConnection
    try run(connection, filePath, batchSize, batchOffset, tablesToVacuum.split(",").toList, enableVacuum, executeVacuumEveryNBatches)
    finally connection.close()
  }

  def run(connection: Connection,
          filePath: String,
          batchSize: Int,
          batchOffset: Int,
          tablesToVacuum: List[String],
          enableVacuum: Boolean,
          executeVacuumEveryNBatches: Int): Unit = {
    val vacuums = tablesToVacuum.map(table => s"VACUUM ANALYZE $table")
    executeBatches(connection, filePath, batchSize, batchOffset, vacuums, enableVacuum, executeVacuumEveryNBatches)
  }

  protected def executeBatches(connection: Connection,
                               filePath: String,
                               batchSize: Int,
                               batchOffset: Int,
                               vacuums: List[String],
                               enableVacuum: Boolean,
                               executeVacuumEveryNBatches: Int): Unit = {
    if (!connection.getAutoCommit) {
      throw new RuntimeException("There is already a transaction in progress")
    }
    val statements = Source.fromFile(filePath).bufferedReader()
    var currentBatch = 1
    var statement = 1
    var sql = statements.readLine()
    connection.setAutoCommit(false)
    val stm = connection.createStatement()

    while (sql != null) {
      stm.addBatch(sql)
      sql = statements.readLine()

      if (statement % batchSize == 0 || sql == null) {
        if (currentBatch >= batchOffset) {
          val result = stm.executeBatch()
          connection.commit()
          if (enableVacuum && currentBatch % executeVacuumEveryNBatches == 0) {
            println("VACUUM")
            executeVacuum(connection, vacuums)
          }
          println(s"Execute Batch:$currentBatch>> result[${result.size}]")
        } else {
          stm.clearBatch()
          println(s"Skip Batch:$currentBatch")
        }
        currentBatch += 1
      }

      statement += 1
    }
    connection.setAutoCommit(true)
  }

  protected def executeVacuum(connection: Connection, vacuums: List[String]): Unit = {
    if (vacuums.nonEmpty) {
      connection.setAutoCommit(true)
      val stm = connection.createStatement()
      try vacuums.foreach(stm.execute)
      finally stm.close()
      connection.setAutoCommit(false)
    }
  }

  protected def executeBatch(connection: Connection, batch: Seq[String]): Unit = {
    connection.setAutoCommit(false)
    val stm = connection.createStatement()
    try {
      batch.foreach(stm.addBatch)
      stm.executeBatch()
      connection.commit()
    } finally stm.close()
  }

}

case class PGConnection(host: String, port: String, database: String, user: String, password: String) {
  def createConnection: Connection = DriverManager.getConnection(s"jdbc:postgresql://$host:$port/$database", user, password)
}