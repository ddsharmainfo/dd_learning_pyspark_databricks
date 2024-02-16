from databricks.sdk.runtime import *

from src_databricks_python_modules.get_spark_session_singleton import SparkSessionSingleton
from src_databricks_python_modules.config.configuration import file_path, output_path
from src_databricks_python_modules.data_pre_processing.read_data import read_parquet
from src_databricks_python_modules.model_building.create_model import get_new_product_id


def write_to_delta(df):
    df.write.format('delta').mode('append').save(output_path)


# if __name__ == "__main__":
#     try:
#         spark = SparkSessionSingleton.get_spark_session()
#         df      = read_parquet(spark, file_path)
#         df_new  = get_new_product_id(spark, df)
#         #df_new.show()
#         write_to_delta(df_new)
#         print('Process Completed Successfully')
#     except e:
#         print('Error:', e)