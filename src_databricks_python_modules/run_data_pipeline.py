from databricks.sdk.runtime import *

from src_databricks_python_modules.get_spark_session_singleton import SparkSessionSingleton
from src_databricks_python_modules.config.configuration import file_path, output_path
from src_databricks_python_modules.data_pre_processing.read_data import read_parquet
from src_databricks_python_modules.model_building.create_model import get_new_product_id
from src_databricks_python_modules.data_post_processing.write_data import write_to_delta


def validate_data(spark, output_path):
    return spark.read.format('delta').load(output_path).count()


if __name__ == "__main__":
    try:
        ## Get SparkSession
        spark = SparkSessionSingleton.get_spark_session()
        
        ## Call read datasource
        df      = read_parquet(spark, file_path)
        
        ## Call model building process
        df_new  = get_new_product_id(spark, df)
        print('No of rows before write: ', df_new.count())

        ## Call write to delta          
        write_to_delta(df_new)

        ## Call validate data
        records_count = validate_data(spark, output_path)
        print('No of rows after write: ', records_count)

        print('Process Completed Successfully')
    except e:
        print('Error:', e)