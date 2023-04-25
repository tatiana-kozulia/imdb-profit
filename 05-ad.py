import sys
from pyspark import SparkConf
from pyspark.sql import SparkSession,Window
import pyspark.sql.types as t
import pyspark.sql.functions as f

def ad():
    path_t="D:\\IPAM_ProfIT\\project\\data\\title.basics.tsv.gz"
    path_r="D:\\IPAM_ProfIT\\project\\data\\title.akas.tsv.gz"
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
    sch_r=t.StructType([
        t.StructField("tconst",t.StringType(),True),
        t.StructField("ordering",t.IntegerType(),True),
        t.StructField("title",t.StringType(),True),
        t.StructField("region",t.StringType(),True),
        t.StructField("language",t.StringType(),True),
        t.StructField("types",t.StringType(),True),
        t.StructField("attributes",t.StringType(),True),
        t.StructField("isOriginalTitle",t.BooleanType(),True)])
    sch_p=t.StructType([
        t.StructField("tconst",t.StringType(),True),
        t.StructField("averageRating",t.FloatType(),True),
        t.StructField("numVotes",t.IntegerType(),True)])
    df_t=spark_s.read.options(delimiter='\t').csv(path_t,header=True,schema=sch_t)
    df_r=spark_s.read.options(delimiter='\t').csv(path_r,header=True,schema=sch_r,nullValue='ZZZ')
    df_p=spark_s.read.options(delimiter='\t').csv(path_p,header=True,schema=sch_p)
    df_t=df_t.where(f.col("isAdult") == 1)
    print(type(df_r))
    df_r=df_r.fillna("ZZZ",subset=["region"])
##    df_r=df_r.withColumn("region",f.when(f.col("region")=="null","ZZZ").otherwize(f.col("region")))
##    df_r=df_r.withColumn("region",f.when(f.col("region")=="\\N","YYY").otherwize(f.col("region")))
    df_r.show()
    print(type(df_r))
    dfu=df_t.join(df_r,on="tconst",how='left')
##    dfu=dfu.na.fill({"region":"empty"})
    df_rc=dfu.groupBy("region").count().orderBy("count",ascending=False)
    df_rc.show()
    df_list=[]
    for row in (df_rc.collect()):
##        print(row["region"])
        df_top=dfu.where(f.col("region")==(row["region"]))
        df_top=df_top.join(df_p,on="tconst",how='left')
        df_top=df_top.orderBy("averageRating",ascending=False).limit(100).select("title","region","averageRating")
        df_list.append(df_top)
    po="D:\\IPAM_ProfIT\\project\\res\\ad"
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
    ad()
