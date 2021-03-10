from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StringType
import datetime

# this row only for IDE
spark = SparkSession.builder.appName("mmingalov_spark").getOrCreate()

#these 2 rows only for terminal
export SPARK_KAFKA_VERSION=0.10
/spark2.4/bin/pyspark --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5 --driver-memory 512m --num-executors 1 --executor-memory 512m --master local[1]

from pyspark.sql.functions import udf
from pyspark.sql.functions import lit

#loading all CSV files in spark dataframes
drill_df = spark.read.format("csv")\
            .option("header", "false")\
            .schema("hole_id STRING, depth_from float, depth_to float, sample_id STRING")\
            .load("final_project/DRILL/*.csv")
lab_df = spark.read.format("csv")\
            .option("header", "false")\
            .schema("sample_id STRING, result FLOAT")\
            .load("final_project/LAB/*.csv")

#JOIN AND SORTING

join_df = drill_df\
            .join(lab_df,lab_df.sample_id == drill_df.sample_id,"left")

sorted_df = join_df\
            .sort("hole_id","depth_from")

sorted_df.show()

Cb = 0.37
Mr = 2

def f_step1(s):
    if s < Cb:
        result = 0  #'П'
    else:
        result = 1  #'Р?'
    return result

#assigning UDF function
udf1 = udf(f_step1)
df1 = sorted_df \
    .withColumn("step1", udf1(join_df["result"])) \
    .show()





# Create step2 function
def f_step2(s):

    return result
#assigning UDF function
udf2 = udf(f_step2)

# Create step3 function
def f_step3(s):
    return result

#assigning UDF function
udf3 = udf(f_step3)

# Create step4 function
def f_step4(s):
    return result
#assigning UDF function
udf4 = udf(f_step4)

    # for i in range(1, df.index.stop - 1, 1):
    #
    # if (df["Au"][i] >= Cb) and (
    #         (
    #                 ((df["To_Depth"][i] - df["From_Depth"][i - 1]) >= Mr) and (df["Au"][i - 1] >= Cb)
    #         )
    #         or
    #         (
    #                 ((df["To_Depth"][i + 1] - df["From_Depth"][i]) >= Mr) and (df["Au"][i + 1] >= Cb)
    #         )
    # ):
    #     df["step2"][i] = 'К'
    # else:
    #     df["step2"][i] = 'П'

# Register UDF
#spark.udf.register("udf1", f_step1) -- DOES NOT WORK



df2 = df1 \
    .withColumn("step2", udf2(df1["result"])) \
    .show()

df3 = df2 \
    .withColumn("step3", udf3(df2["result"])) \
    .show()

df4 = df3 \
    .withColumn("step1", udf4(df3["result"])) \
    .show()

# # Create udf create python lambda
# from pyspark.sql.functions import udf
#
# classes = [8, 18, 38]
# udf1 = udf(lambda e: 'A' if e < classes[0] else 'B' if e < classes[1] else 'C' if e < classes[2] else 'D')
# udf2 = udf(lambda e: 1 if e < classes[0] else 2 if e < classes[1] else 3 if e < classes[2] else 4)
#
# df = df.withColumn("rating_class_str", udf1(df["rating"]))





#--------THERE IS PROTOTYPE OF COMPOSITING CALCULATION ------------
# import pandas as pd
#
# EXPORT_FILE = "D:\\Cloud\\Git\\geekbrains-final-project\\composite.xlsx"
# INPUT_FILE = "D:\\Cloud\\Git\\geekbrains-final-project\\final_project_data_mmingalov.xlsx"
# SHEET_NAME = "example"
#
# df_example = pd.read_excel(INPUT_FILE,
#                            sheet_name=SHEET_NAME, header=1, usecols="A:J",
#                            dtype={'Hole_ID': str, 'From_Depth': float, 'To_Depth': float, 'Sample': str, 'Au': float})
# Cb = 0.37
# Mr = 2
#
# df_example.head(15)
#
# # df_example.dtypes
#
# df = df_example.copy()
# df["step1"] = 'П'
# df["step2"] = 'П'
# df["step3"] = 'П'
# df["composite"] = ''
#
# # step1
# for i in range(0, df.index.stop, 1):
#
#     if df["Au"][i] < Cb:
#         df["step1"][i] = 'П'
#     else:
#         df["step1"][i] = 'Р?'
#
# # step2
# for i in range(1, df.index.stop - 1, 1):
#
#     if (df["Au"][i] >= Cb) and (
#             (
#                     ((df["To_Depth"][i] - df["From_Depth"][i - 1]) >= Mr) and (df["Au"][i - 1] >= Cb)
#             )
#             or
#             (
#                     ((df["To_Depth"][i + 1] - df["From_Depth"][i]) >= Mr) and (df["Au"][i + 1] >= Cb)
#             )
#     ):
#         df["step2"][i] = 'К'
#     else:
#         df["step2"][i] = 'П'
#
# # step3
# for i in range(1, df.index.stop - 1, 1):
#
#     if (df["Au"][i] < Cb) and (df["Au"][i - 1] >= Cb) and (df["Au"][i + 1] >= Cb):
#         df["step3"][i] = 'К'
#     else:
#         df["step3"][i] = df["step2"][i]
#
# # step4 = composite
# for i in range(1, df.index.stop - 1, 1):
#
#     if (
#             (
#                     (df["step1"][i] == 'Р?') or (df["step3"][i] == 'К')
#             )
#             and
#             (
#                     (df["step3"][i - 1] != 'П')
#                     or (df["step3"][i + 1] != 'П')
#                     or ((df["step1"][i - 1] == 'Р?') and (df["step1"][i + 1] == 'Р?') and (
#                     (df["To_Depth"][i + 1] - df["From_Depth"][i - 1]) >= Mr))
#             )
#     ):
#         df["composite"][i] = 'К'
#
# df.head(100)
#
# writer = pd.ExcelWriter(EXPORT_FILE)
# df.to_excel(writer, 'df_composite', index=False)
# writer.save()
