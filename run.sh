./sbt "set test in assembly := {}" clean assembly
java -jar target/scala-2.12/pg-batch-executor-assembly-0.1.jar