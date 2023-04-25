import sys
from pyspark import SparkConf
from pyspark.sql import SparkSession,Window
import pyspark.sql.types as t
import pyspark.sql.functions as f

def ch():
    path_t="D:\\IPAM_ProfIT\\project\\data\\title.basics.tsv.gz"
    path_n="D:\\IPAM_ProfIT\\project\\data\\name.basics.tsv.gz"
    path_c="D:\\IPAM_ProfIT\\project\\data\\title.principals.tsv.gz"
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
        t.StructField("isAdult",t.BooleanType(),True),
        t.StructField("startYear",t.DateType(),True),
        t.StructField("endYear",t.DateType(),True),
        t.StructField("runtimeMinutes",t.IntegerType(),True),
        t.StructField("genres",t.StringType(),True)])
    sch_n=t.StructType([
        t.StructField("nconst",t.StringType(),True),
        t.StructField("primaryName",t.StringType(),True),
        t.StructField("birthYear",t.DateType(),True),
        t.StructField("deathYear",t.DateType(),True),
        t.StructField("primaryProfession",t.StringType(),True),
        t.StructField("knownForTitles",t.StringType(),True)])
    sch_c=t.StructType([
        t.StructField("tconst",t.StringType(),True),
        t.StructField("ordering",t.IntegerType(),True),
        t.StructField("nconst",t.StringType(),True),
        t.StructField("category",t.StringType(),True),
        t.StructField("job",t.StringType(),True),
        t.StructField("characters",t.StringType(),True)])
    df_t=spark_s.read.options(delimiter='\t').csv(path_t,header=True,schema=sch_t)
    df_n=spark_s.read.options(delimiter='\t').csv(path_n,header=True,schema=sch_n)
    df_c=spark_s.read.options(delimiter='\t').csv(path_c,header=True,schema=sch_c)
    df_c=df_c.where(f.col("characters") != '\\N')
    df_c=df_c.limit(10)
    dfu=df_c.join(df_n,on="nconst",how='left')
    dfu=dfu.join(df_t,on="tconst",how='left')
##    df_ch=(dfu
##            .select("primaryName","originalTitle","characters")
##            .where(f.col("characters") != '\\N'))
    df_ch=dfu.select("primaryName","originalTitle","characters")
    df_ch.show()
    po="D:\\IPAM_ProfIT\\project\\res\\ch"
    #df_ch.write.mode("overwrite").csv(po,header=True)

if __name__ == "__main__":
    ch()
