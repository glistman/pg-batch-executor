# pg-batch-executor
Allows to execute a very large file of insert update and delete in small batch, does not handle a transaction per file if not by batch, the size of the batch is defined by the user, allowing to start manually in the last batch executed correctly.

* Config
  ```
  export DB_HOST=localhost
  export DB_PORT=5432
  export DB_NAME=test
  export DB_USER=postgres
  export DB_PASSWORD=postgres
  export BATCH_OFFSET=1
  export BATCH_SIZE=10000
  export SCRIPT_PATH=/home/ubuntu/update_users.sql
  export TABLES_TO_VACUUM=users,addresses
  ```
  
 the parameter BATCH_OFFSET is used in case of failure of a batch to be able to correct the file and start in the batch that failed

* Run
```
./sbt run
```