from pyspark.sql import SparkSession, DataFrame
# from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StringType, IntegerType

# import datetime

# this row only for IDE
spark = SparkSession.builder.appName("mmingalov_spark").getOrCreate()

#these 2 rows only for terminal
export SPARK_KAFKA_VERSION=0.10
/spark2.4/bin/pyspark --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5 --driver-memory 512m --num-executors 1 --executor-memory 512m --master local[1]




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
#STEPS EXECUTING

#step1
from pyspark.sql.functions import udf
def step1(r):
    if r >= Cb:
        result = 1
    else:
        result = 0
    return result
udf_step1 = udf(step1)

from pyspark.sql.functions import lag,lead,min
from pyspark.sql.window import Window
window = Window.partitionBy("hole_id").orderBy("depth_from")

df = sorted_df \
    .withColumn("first_depth_from", min("depth_from").over(window))\
    .withColumn("prev_depth_from", lag("depth_from", 1).over(window))\
    .withColumn("next_depth_to", lead("depth_to", 1).over(window))\
    .withColumn("prev_result", lag("result", 1).over(window))\
    .withColumn("next_result", lead("result", 1).over(window))\
    .withColumn("step1", udf_step1(join_df["result"]))\
    .sort("hole_id","depth_from").sort("hole_id","depth_from")

df.createOrReplaceTempView("df1")
spark.sql("select * from df1").show()


#step2
sql2 = "select df1.*" \
       ", case when (df1.depth_from <> df1.first_depth_from) " \
       "and (df1.step1 = 1) and " \
       "((((df1.depth_to - df1.prev_depth_from)>=" + str(Mr) +  ") " \
       "and (df1.prev_result >= " + str(Cb) +  ")) " \
       "or (((df1.next_depth_to - df1.depth_from)>=" + str(Mr) +  ") " \
       "and (df1.next_result >= " + str(Cb) +  "))) " \
       "then 1 else 0 end as step2 from df1 order by df1.hole_id, df1.depth_from"

spark.sql(sql2).createOrReplaceTempView("df2")
spark.sql("select * from df2").show()

#step3
sql3 = "select df2.*, " \
       "case when " \
       "(df2.depth_from <> df2.first_depth_from) " \
       "and df2.result < " + str(Cb) +  " " \
       "and df2.prev_result>=" + str(Cb) +  " " \
       "and df2.next_result>=" + str(Cb) +  " " \
       "then 1 else step2 end step3 from df2 order by df2.hole_id, df2.depth_from"

#before final step we need to add other columns
df_temp = spark.sql(sql3)
df_temp.show()

df_temp = df_temp\
    .withColumn("prev_step1", lag("step1", 1).over(window))\
    .withColumn("next_step1", lead("step1", 1).over(window))\
    .withColumn("prev_step3", lag("step3", 1).over(window))\
    .withColumn("next_step3", lead("step3", 1).over(window))
# df_temp.show()

df_temp.createOrReplaceTempView("df3")
spark.sql("select * from df3").show()

# step4
sql4 = "select df3.*, " \
       "case when " \
       "(df3.depth_from <> df3.first_depth_from) " \
       "and ((df3.step1 = 1) or (df3.step3 = 1)) " \
       "and ( (df3.prev_step3<>0) " \
       "or (df3.next_step3<>0) " \
       "or ( (df3.prev_step1<>0) " \
       "and (df3.next_step1<>0) " \
       "and ( df3.next_depth_to - df3.prev_depth_from ) >= " + str(Mr) + ")) " \
       "then 1 else 0 end composite from df3 order by df3.hole_id, df3.depth_from"

spark.sql(sql4).createOrReplaceTempView("df4")
df_composite = spark.sql("select * from df4 order by hole_id, depth_from")
df_composite.show()

#SAVING
df_composite.select("hole_id", "depth_from", "depth_to", "sample_id", "result", "composite")\
    .write\
    .format("csv")\
    .save("final_project/composite.csv")

#checking a loading our CSV
df = spark.read.format("csv")\
            .option("header", "false")\
            .schema("hole_id STRING, depth_from float, depth_to float, sample_id STRING, result float, composite integer")\
            .load("final_project/composite.csv")\
            .sort("hole_id","depth_from")



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
