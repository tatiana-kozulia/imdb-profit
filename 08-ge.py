import sys
from pyspark import SparkConf
from pyspark.sql import SparkSession,Window
import pyspark.sql.types as t
import pyspark.sql.functions as f

def abc(e):
    def wer(s1):
        s=s1.split(',')
        try:
            ans=s[e]
        except IndexError:
            ans="abc"
        return ans
    return wer

def ge():
    path_t="D:\\IPAM_ProfIT\\project\\data\\title.basics.tsv.gz"
    path_p="D:\\IPAM_ProfIT\\project\\data\\title.ratings.tsv.gz"
    spark_s=(SparkSession.builder
             .master("local")
             .appName("task app")
             .config(conf=SparkConf())
             .getOrCreate())
    sch_t=t.StructType([
        t.StructField("tconst",t.StringType(),True),
        t.StructField("titleType",t.StringType(),True),
        t.StructField("primaryTitle",t.StringType(),True),
        t.StructField("originalTitle",t.StringType(),True),
        t.StructField("isAdult",t.IntegerType(),True),
        t.StructField("startYear",t.IntegerType(),True),
        t.StructField("endYear",t.IntegerType(),True),
        t.StructField("runtimeMinutes",t.IntegerType(),True),
        t.StructField("genres",t.StringType(),True)])
    sch_p=t.StructType([
        t.StructField("tconst",t.StringType(),True),
        t.StructField("averageRating",t.FloatType(),True),
        t.StructField("numVotes",t.IntegerType(),True)])
    df_t=spark_s.read.options(delimiter='\t').csv(path_t,header=True,schema=sch_t)
    df_p=spark_s.read.options(delimiter='\t').csv(path_p,header=True,schema=sch_p)
    df_t=df_t.select("tconst","originalTitle","genres")
    df_t=df_t.withColumn("genres",f.split(f.col("genres"),","))
    df_t=df_t.withColumn("genres",f.explode("genres"))
    df_p=df_p.select("tconst","averageRating")
    df_u=df_t.join(df_p,on="tconst",how='left')
    w=Window.partitionBy("genres").orderBy(f.col("averageRating").desc())
    df_u=df_u.withColumn("tconst",f.row_number().over(w))
    df_u=df_u.where(f.col("tconst")<11).select("genres","originalTitle","averageRating")
    df_u.show(100)
    po="D:\\IPAM_ProfIT\\project\\res\\ge"
    df_u.write.mode("overwrite").csv(po,header=True)

if __name__ == "__main__":
    ge()
