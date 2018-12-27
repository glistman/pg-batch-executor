# pg-batch-executor
Allows to execute a very large file of insert update and delete in small batch, does not handle a transaction per file if not by batch, the size of the batch is defined by the user, allowing to start manually in the last batch executed correctly.
