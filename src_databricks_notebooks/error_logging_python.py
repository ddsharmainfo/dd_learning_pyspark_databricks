# Databricks notebook source
# MAGIC %md ## Useful URLs
# MAGIC * Python Reference Docs: https://docs.python.org/3/howto/logging-cookbook.html
# MAGIC * https://sparkbyexamples.com/spark/spark-stop-info-and-debug-logging-console/

# COMMAND ----------

# MAGIC %sh mount | grep dbfs

# COMMAND ----------

import sys
import logging

logger1 = logging.getLogger()
logger2 = logging.getLogger(__name__)
logger3 = logging.getLogger("py4j")
print('logger1: ', logger1)
print('logger2: ', logger2)
print('logger3: ', logger3)

## 'application' code
# logger.info('info message')
# logger.warning('warn message')
# logger.error('error message')
# logger.debug('debug message')
# logger.critical('critical message')

# COMMAND ----------

# MAGIC %md ##Option 1: Set Logger on console

# COMMAND ----------

# DBTITLE 1,Logs in console/driver stderr
import logging

#logger_stderr = logging.getLogger(__name__)
logger_console = logging.getLogger('custom_console_logger')
print('Starting of logger_console: ', logger_console)

formatter = logging.Formatter('%(asctime)s : %(levelname)s : %(name)s:%(filename)s : %(funcName)s:%(lineno)s : %(message)s')

logger_console.setLevel(logging.INFO)

## Stream handler or console handler
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
ch.setFormatter(formatter)

## add console handlet
logger_console.addHandler(ch)

logger_console.info('----- Custom console logger...logger.INFO')
logger_console.warning('----- Custom console logger...logger.WARNING')
logger_console.error('----- Custom console logger...logger.ERROR')

print('End of logger_console: ', logger_console)

# COMMAND ----------

# MAGIC %md ##Option 2: Set Logger in file

# COMMAND ----------

# DBTITLE 1,Logs in provided file
import logging

#logger_stderr = logging.getLogger(__name__)
logger_file = logging.getLogger('custom_file_logger')
print('Starting of logger_file: ', logger_file)

filename = '/local_disk0/tmp/custom_logger_file.log'

formatter = logging.Formatter('%(asctime)s : %(levelname)s : %(name)s:%(filename)s : %(funcName)s:%(lineno)s : %(message)s')

logger_file.setLevel(logging.INFO)

## File Handler
fh = logging.FileHandler(filename)
fh.setLevel(logging.INFO)
fh.setFormatter(formatter)

## add handler
logger_file.addHandler(fh)

logger_file.info('----- Custom file logger...logger.INFO')
logger_file.warning('----- Custom file logger...logger.WARNING')
logger_file.error('----- Custom file logger...logger.ERROR')

print('End of logger_file: ', logger_file)

# COMMAND ----------

# MAGIC %sh tail /local_disk0/tmp/custom_logger_file.log
# MAGIC 
# MAGIC ## To view or store the logs over dbfs or S3 or any other storage mount point we need to Copy or move from local_disk0/tmp
# MAGIC #cp /local_disk0/tmp/custom_logger_file.log /dbfs/FileStore/custom_logger_file.log

# COMMAND ----------

# MAGIC %md ##Option 3: Set Logger in stdout

# COMMAND ----------

# DBTITLE 1,Logs in driver stdout
import sys
import logging

logger_stdout = logging.getLogger(__name__)
print('logger_stdout: ', logger_stdout)

logger_stdout.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s : %(levelname)s : %(name)s:%(filename)s : %(funcName)s:%(lineno)s : %(message)s')
handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(formatter)
logger_stdout.addHandler(handler)

logger_stdout.info('----- Custom Logger Test in stdout...logger.INFO')
logger_stdout.warning('----- Custom Logger Test in stdout...logger.WARNING')
logger_stdout.error('----- Custom Logger Test in stdout...logger.ERROR')

# COMMAND ----------

# MAGIC %md ##Option 4: Set Logger in stderr

# COMMAND ----------

# DBTITLE 1,Logs in driver stderr
import sys
import logging

logger_stderr = logging.getLogger(__name__)
print('logger_stderr: ', logger_stderr)

logger_stderr.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s : %(levelname)s : %(name)s:%(filename)s : %(funcName)s:%(lineno)s : %(message)s')
handler = logging.StreamHandler(sys.stderr)
handler.setFormatter(formatter)
logger_stderr.addHandler(handler)

logger_stderr.info('----- Custom Logger Test in stderr...logger.INFO')
logger_stderr.warning('----- Custom Logger Test in stderr...logger.WARNING')
logger_stderr.error('----- Custom Logger Test in stderr...logger.ERROR')

# COMMAND ----------

# MAGIC %md ##Option 5: Set Logger using `main Spark log4j logger`

# COMMAND ----------

# DBTITLE 1,Logs in driver log4j
### Using main Spark log4j logger. This will be available in log4j outout
import logging

#sc._jvm.org.apache.log4j or spark._jvm.org.apache.log4j.Logger
logger_spark = spark.sparkContext._jvm.org.apache.log4j 
logger_sc = logger_spark.LogManager.getLogger(__name__) 
print(logger_sc)

logger_sc.info('----- Custom Logger Test in spark log4j logger...logger.INFO')
logger_sc.error('----- Custom Logger Test in spark log4j logger...logger.ERROR')
logger_sc.error('----- Custom Logger Test in spark log4j logger...logger.ERROR')

# logger_sc.error("Testing ERROR using Spark Context Logger")
# logger_sc.info('Spark Test Result : ' + str(lst))
# logger_sc.info('Spark Session Object : ' + str(spark))

# COMMAND ----------

# MAGIC %md ## Uisng `py4j` logger

# COMMAND ----------

# DBTITLE 1,Logs in driver stderr
import logging

#logger_custom = logging.getLogger("py4j")
logger_custom = logging.getLogger("py4j.java_gateway")
print('Starting of logger_custom: ', logger_custom)

formatter = logging.Formatter('%(asctime)s : %(levelname)s : %(name)s:%(filename)s : %(funcName)s:%(lineno)s : %(message)s')

logger_custom.setLevel(logging.INFO)

## Stream handler or console handler
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
ch.setFormatter(formatter)

## add console handlet
logger_custom.addHandler(ch)

logger_custom.info('----- Custom py4j logger...logger.INFO')
logger_custom.warning('----- Custom py4j logger...logger.WARNING')
logger_custom.error('----- Custom py4j logger...logger.ERROR')

print('End of logger_custom: ', logger_custom)

# COMMAND ----------

#Initiate Logger
Logger = spark._jvm.org.apache.log4j.Logger
mylogger = Logger.getLogger(__name__)

print(mylogger)

#notebookName="RM27"

# cellHeader="Reading_NMI_Data"
# documentTypeLogging="ServicePointData"
# collectionLogging="colNMIData"
# processNameLogging="Reading_NMI_Data"
# logFile=f"{notebookName} | Cell Header - {cellHeader} | Document Type - {documentTypeLogging} | Collection - {collectionLogging} < Process Name - {processNameLogging} | Process ID - {processID} | Description - "

# spark.sparkContext.setLogLevel("DEBUG")
# mylogger.debug(f"{logFile}Executing = {notebookName} >")
# mylogger.debug(f"{logFile}documentType = {documentTypeLogging} >")
# mylogger.debug(f"{logFile}collection = {collectionLogging} >")
# spark.sparkContext.setLogLevel("FATAL")
