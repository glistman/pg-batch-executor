package com.github.glistman.pgbatch

import java.sql.Connection

import org.scalatest.FunSuite

class PgBatchTest extends FunSuite {

  test("run a million inserts in batch") {
    val batchOffset = 1
    val batchSize = 10000
    val filePath = sys.props("user.dir") + "/src/test/scala/resources/inserts.sql"
    val pgConnection = PGConnection("localhost", "5432", "test_db", "postgres", "postgres")
    val connection = pgConnection.createConnection
    try {
      createTestTable(connection)
      PgBatch.run(connection, filePath, batchSize, batchOffset, List())
      val ps = connection.prepareStatement("SELECT count(*) FROM test_table")
      try {
        val rs = ps.executeQuery()
        try assert(rs.next() && rs.getInt("count") == 1000000)
        finally rs.close()
      } finally ps.close()
    } finally connection.close()
  }

  test("run a million inserts in batch with offset 100") {
    val batchOffset = 100
    val batchSize = 10000
    val filePath = sys.props("user.dir") + "/src/test/scala/resources/inserts.sql"
    val pgConnection = PGConnection("localhost", "5432", "test_db", "postgres", "postgres")
    val connection = pgConnection.createConnection
    try {
      createTestTable(connection)
      PgBatch.run(connection, filePath, batchSize, batchOffset, List())
      val ps = connection.prepareStatement("SELECT count(*) FROM test_table")
      try {
        val rs = ps.executeQuery()
        try assert(rs.next() && rs.getInt("count") == 10000)
        finally rs.close()
      } finally ps.close()
    } finally connection.close()
  }

  test("run a million inserts and a million updates") {
    val insertBatchOffset = 1
    val insertBatchSize = 10000
    val updateBatchOffset = 1
    val updateBatchSize = 1000
    val filePathInserts = sys.props("user.dir") + "/src/test/scala/resources/inserts.sql"
    val filePathUpdates = sys.props("user.dir") + "/src/test/scala/resources/updates.sql"
    val pgConnection = PGConnection("localhost", "5432", "test_db", "postgres", "postgres")
    val connection = pgConnection.createConnection
    try {
      createTestTable(connection)
      PgBatch.run(connection, filePathInserts, insertBatchSize, insertBatchOffset, List())
      PgBatch.run(connection, filePathUpdates, updateBatchSize, updateBatchOffset, List())
      val ps = connection.prepareStatement("SELECT count(*) FROM test_table WHERE name = 'update'")
      try {
        val rs = ps.executeQuery()
        try assert(rs.next() && rs.getInt("count") == 1000000)
        finally rs.close()
      } finally ps.close()
    } finally connection.close()
  }

  test("run a million inserts and a million updates with vacuum") {
    val insertBatchOffset = 1
    val insertBatchSize = 10000
    val updateBatchOffset = 1
    val updateBatchSize = 1000
    val filePathInserts = sys.props("user.dir") + "/src/test/scala/resources/inserts.sql"
    val filePathUpdates = sys.props("user.dir") + "/src/test/scala/resources/updates.sql"
    val pgConnection = PGConnection("localhost", "5432", "test_db", "postgres", "postgres")
    val connection = pgConnection.createConnection
    try {
      createTestTable(connection)
      PgBatch.run(connection, filePathInserts, insertBatchSize, insertBatchOffset, List())
      PgBatch.run(connection, filePathUpdates, updateBatchSize, updateBatchOffset, List("test_table"))
      val ps = connection.prepareStatement("SELECT count(*) FROM test_table WHERE name = 'update'")
      try {
        val rs = ps.executeQuery()
        try assert(rs.next() && rs.getInt("count") == 1000000)
        finally rs.close()
      } finally ps.close()
    } finally connection.close()
  }

  def createTestTable(connection: Connection): Unit = {
    val stm = connection.createStatement()
    try {
      stm.execute("DROP TABLE IF EXISTS test_table")
      stm.execute("CREATE TABLE test_table(id integer PRIMARY KEY , name varchar(255))")
    } finally stm.close()
  }

}