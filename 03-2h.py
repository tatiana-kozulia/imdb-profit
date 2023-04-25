import sys
from pyspark import SparkConf
from pyspark.sql import SparkSession,Window
import pyspark.sql.types as t
import pyspark.sql.functions as f

def th():
    path="D:\\IPAM_ProfIT\\project\\data\\title.basics.tsv.gz"
    spark_s=(SparkSession.builder
             .master("local")
             .appName("task app")
             .config(conf=SparkConf())
             .getOrCreate())
    sch=t.StructType([
        t.StructField("tconst",t.StringType(),True),
        t.StructField("titleType",t.StringType(),True),
        t.StructField("primaryTitle",t.StringType(),True),
        t.StructField("originalTitle",t.StringType(),True),
        t.StructField("isAdult",t.BooleanType(),True),
        t.StructField("startYear",t.DateType(),True),
        t.StructField("endYear",t.DateType(),True),
        t.StructField("runtimeMinutes",t.IntegerType(),True),
        t.StructField("genres",t.StringType(),True)])
    df=spark_s.read.options(delimiter='\t').csv(path,header=True,schema=sch)
##    df_2h=(df
##            .select("primaryTitle","originalTitle","titleType","runtimeMinutes")
##            .where((f.col("titleType") == "movie")
##                   & (f.col("runtimeMinutes") > 120)))
    df_2h=(df
            .where((f.col("titleType") == "movie")
                   & (f.col("runtimeMinutes") > 120))
            .select("originalTitle"))
    df_2h.show()
    po="D:\\IPAM_ProfIT\\project\\res\\2h"
    df_2h.write.mode("overwrite").csv(po,header=True)

if __name__ == "__main__":
    th()
