# Databricks notebook source
# MAGIC %md ##Set the task value using `taskValues subutility`
# MAGIC * Use `dbutils.jobs.taskValues.set(key=<some_key>, value=<some_value>)`
# MAGIC * https://docs.databricks.com/en/workflows/jobs/share-task-context.html
# MAGIC * https://docs.databricks.com/en/dev-tools/databricks-utils.html#dbutils-jobs-taskvalues

# COMMAND ----------

dbutils.widgets.removeAll()
dbutils.widgets.text("task_name", "")
task_name = dbutils.widgets.get('task_name')
print(task_name)

# COMMAND ----------

dbutils.jobs.taskValues.set(key = "task1_key1", value = 'task1_key1_Value1')

# COMMAND ----------

dbutils.jobs.taskValues.set(key = task_name+"_key2", value = 'task1_key1_Value2')

# COMMAND ----------

dbutils.jobs.taskValues.set(key = task_name+"_key3", value = 10)
