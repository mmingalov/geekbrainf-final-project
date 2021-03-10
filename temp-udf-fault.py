from pyspark.sql import SparkSession, DataFrame
# from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StringType, IntegerType
# from pyspark.sql.functions import lit
# import datetime

# this row only for IDE
spark = SparkSession.builder.appName("mmingalov_spark").getOrCreate()

#these 2 rows only for terminal
export SPARK_KAFKA_VERSION=0.10
/spark2.4/bin/pyspark --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5 --driver-memory 512m --num-executors 1 --executor-memory 512m --master local[1]

from pyspark.sql.functions import udf


#loading all CSV files in spark dataframes
drill_df = spark.read.format("csv")\
            .option("header", "false")\
            .schema("hole_id STRING, depth_from float, depth_to float, sample_id STRING")\
            .load("final_project/DRILL/*.csv")
lab_df = spark.read.format("csv")\
            .option("header", "false")\
            .schema("sample_id STRING, result FLOAT")\
            .load("final_project/LAB/*.csv")

drill_df.printSchema()

drill_df.show()

#JOIN AND SORTING
join_df = drill_df.alias("a")\
            .join(lab_df.alias("b"),lab_df.sample_id == drill_df.sample_id,"inner")\
            .select("a.*","b.result")

sorted_df = join_df\
            .sort("hole_id","depth_from")

sorted_df.printSchema()
sorted_df.show()

Cb = 0.37
Mr = 2

# hol = "RC001"
# f = 4
# get_prev_depth_from(df,hol, f)
def get_RDD_value(R):
    return R.collect()[0].__getitem__("depth_from")

def get_prev_depth_from(df,hol, f):
    try:
        result = df.filter((df["depth_to"] == f) & (df["hole_id"] == hol)).select(["depth_from"]).collect()[0].__getitem__("depth_from")
    except:
        result = -1
    return result
udf_gpdf = udf(get_prev_depth_from, IntegerType())

def get_next_depth_to(df, hol,t):
    try:
        result = df.filter((df["depth_from"] == t) & (df["hole_id"] == hol)).select(["depth_to"]).collect()[0].__getitem__("depth_to")
    except:
        result = -1
    return result
udf_gndt = udf(get_next_depth_to)

def get_prev_result():
    return
udf_gpr = udf(get_prev_result())

def get_next_result():
    return
udf_gnr = udf(get_next_result)

def calc_step1(res):
    if res < Cb:
        result = 0  #'П'
    else:
        result = 1  #'Р?'
    return result
udf_step1 = udf(calc_step1)

def calc_step2(s):
    result = ''
    return result
udf_step2 = udf(calc_step2)


#STEPS EXECUTING
df1 = sorted_df \
    .withColumn("step1", udf_step1(join_df["result"]))

#TEST
hol = "RC001"
f = 4
get_prev_depth_from(df1,hol, f)

def get_prev_depth_from(hol, f):
    try:
        result = df1.filter((df1["depth_to"] == f) & (df1["hole_id"] == hol)).select(["depth_from"]).collect()[0].__getitem__("depth_from")
    except:
        result = -1
    return result
udf_gpdf = udf(get_prev_depth_from, IntegerType())


temp_df = df1 \
    .withColumn("prev_depth_from", udf_gpdf(df1, df1["hole_id"],df1["depth_from"])) \
    .withColumn("next_depth_to", udf_gndt(df1, df1["hole_id"],df1["depth_to"])) \
    .show()



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
