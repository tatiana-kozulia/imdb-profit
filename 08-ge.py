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
##    w0=abc(0)
##    df_t=df_t.withColumn("g0",f.col("genres").split(','))
##    w1=abc(1)
##    df_t=df_t.withColumn("g1",w1(f.col("genres")))
##    w2=abc(2)
##    df_t=df_t.withColumn("g2",w2(f.col("genres")))
##    df_t.show()
    df_g=df_t.select(f.split(f.col("genres"),",")).toDF("genres")
    dfu=df_t.select(f.split(f.col("genres"),","),"tconst","originalTitle").toDF("genres","tconst","originalTitle").join(df_p,on="tconst",how='left')
    df_g=df_g.withColumn("g0",f.get(f.col("genres"),0)).withColumn("g1",f.get(f.col("genres"),1)).withColumn("g2",f.get(f.col("genres"),2))
    df_g=df_g.select("g0").union(df_g.select("g1").toDF("g0")).union(df_g.select("g2").toDF("g0")).groupBy("g0").count()
    df_g.show(40)
    dfu.show()
    df_list=[]
    for row in (df_g.collect()):
        if(row["g0"] in [None,"\\N"]):
            continue
        df_top=dfu.where(f.array_contains(f.col("genres"),row["g0"]))
##        df_top=df_top.join(df_p,on="tconst",how='left')
        df_top=df_top.orderBy("averageRating",ascending=False).limit(10).select("originalTitle","genres","averageRating")
##        df_top=df_top.join(df_t,on="tconst",how='left').join(df_tv,on="parentTconst",how='left').select("count","parentTconst","originalTitle","averageRating")
        df_list.append(df_top)
    po="D:\\IPAM_ProfIT\\project\\res\\ge"
    df_res=df_list[0]
    if (len(df_list)>1):
        for horse in df_list[1:]:
            df_res=df_res.union(horse)
    df_res.show(300)
    df_res.write.mode("overwrite").csv(po,header=True)
##    for i in df_list:
##        i.show()
##        i.write.mode("overwrite").csv(po,header=True)

if __name__ == "__main__":
    ge()
