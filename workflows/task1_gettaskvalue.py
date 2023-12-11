# Databricks notebook source
# MAGIC %md ## Option 1
# MAGIC * https://docs.databricks.com/en/workflows/jobs/share-task-context.html

# COMMAND ----------

value1 = dbutils.jobs.taskValues.get(taskKey = "task1", key = "task1_key1", default = 'default_valu1', debugValue = 'debug_value1')
print(value1)

# COMMAND ----------

value2 = dbutils.jobs.taskValues.get(taskKey = "task1", key = "task1_key2", default = 'default_value2', debugValue = 'debug_value2')
print(value2)

# COMMAND ----------

value3 = dbutils.jobs.taskValues.get(taskKey = "task1", key = "task1_key3", default = 'default_value3', debugValue = 'debug_value3')
print(value3)

# COMMAND ----------

# MAGIC %md ##Option 2
# MAGIC * This is `recommended` option by Databricks
# MAGIC * `Dynamic Value References` https://docs.databricks.com/en/workflows/jobs/parameter-value-references.html

# COMMAND ----------

dbutils.widgets.removeAll()
dbutils.widgets.text("task_name", "")
dbutils.widgets.text("task1_key1", "")
dbutils.widgets.text("task1_key2", "")
dbutils.widgets.text("task1_key3", "")

task_name      = dbutils.widgets.get('task_name')
task1_key1_val = dbutils.widgets.get('task1_key1')
task1_key2_val = dbutils.widgets.get('task1_key2')
task1_key3_val = dbutils.widgets.get('task1_key3')

print(task_name)
print('Value of task1_key1: ', task1_key1_val)
print('Value of task1_key2: ', task1_key2_val)
print('Value of task1_key3: ', task1_key3_val)
