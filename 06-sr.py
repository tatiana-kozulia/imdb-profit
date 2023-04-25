import sys
from pyspark import SparkConf
from pyspark.sql import SparkSession,Window
import pyspark.sql.types as t
import pyspark.sql.functions as f

def sr():
    path_t="D:\\IPAM_ProfIT\\project\\data\\title.basics.tsv.gz"
    path_e="D:\\IPAM_ProfIT\\project\\data\\title.episode.tsv.gz"
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
        t.StructField("startYear",t.DateType(),True),
        t.StructField("endYear",t.DateType(),True),
        t.StructField("runtimeMinutes",t.IntegerType(),True),
        t.StructField("genres",t.StringType(),True)])
    sch_e=t.StructType([
        t.StructField("tconst",t.StringType(),True),
        t.StructField("parentTconst",t.StringType(),True),
        t.StructField("seasonNumber",t.IntegerType(),True),
        t.StructField("episodeNumber",t.IntegerType(),True)])
    sch_p=t.StructType([
        t.StructField("tconst",t.StringType(),True),
        t.StructField("averageRating",t.FloatType(),True),
        t.StructField("numVotes",t.IntegerType(),True)])
    df_t=spark_s.read.options(delimiter='\t').csv(path_t,header=True,schema=sch_t)
    df_e=spark_s.read.options(delimiter='\t').csv(path_e,header=True,schema=sch_e)
    df_p=spark_s.read.options(delimiter='\t').csv(path_p,header=True,schema=sch_p)
    df_tv=df_e.groupBy("parentTconst").count().orderBy("count",ascending=False)
    df_tv.show()
    df_list=[]
    for row in (df_tv.collect()):
##        df_top=df_e.where(f.col("parentTconst")==(row["parentTconst"]))
##        df_top=df_top.join(df_p,on="tconst",how='left')
##        df_top=df_top.orderBy("averageRating",ascending=False).limit(5).select("parentTconst","tconst","averageRating")
##        df_top=df_top.join(df_t,on="tconst",how='left').join(df_tv,on="parentTconst",how='left').select("count","parentTconst","originalTitle","averageRating")
        df_top=df_e.where(f.col("parentTconst")==(row["parentTconst"])).join(df_p,on="tconst",how='left').select("parentTconst","tconst","averageRating").join(df_t,on="tconst",how='left').join(df_tv,on="parentTconst",how='left').select("count","parentTconst","originalTitle","averageRating").orderBy("averageRating",ascending=False).limit(50)
        df_list.append(df_top)
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
