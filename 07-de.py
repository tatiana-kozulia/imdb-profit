import sys
from pyspark import SparkConf
from pyspark.sql import SparkSession,Window
import pyspark.sql.types as t
import pyspark.sql.functions as f

def sr():
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
    df_list=[]
    flag=True
    hat=2030
    shoe=2019
    while(flag):
        df_top=df_t.where((f.col("startYear")>shoe) & (f.col("startYear")<hat))
        if(df_top.isEmpty()):
            flag=False
            break
        df_top=df_top.join(df_p,on="tconst",how='left')
        df_top=df_top.orderBy("averageRating",ascending=False).limit(10).select("startYear","originalTitle","averageRating")
        df_list.append(df_top)
        hat=hat-10
        shoe=shoe-10
    po="D:\\IPAM_ProfIT\\project\\res\\sr"
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
    sr()
