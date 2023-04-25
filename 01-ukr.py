import sys
from pyspark import SparkConf
from pyspark.sql import SparkSession,Window
import pyspark.sql.types as t
import pyspark.sql.functions as f

def ukr():
    path="D:\\IPAM_ProfIT\\project\\data\\title.akas.tsv.gz"
    spark_s=(SparkSession.builder
             .master("local")
             .appName("task app")
             .config(conf=SparkConf())
             .getOrCreate())
    sch=t.StructType([
        t.StructField("titleId",t.StringType(),True),
        t.StructField("ordering",t.IntegerType(),True),
        t.StructField("title",t.StringType(),True),
        t.StructField("region",t.StringType(),True),
        t.StructField("language",t.StringType(),True),
        t.StructField("types",t.StringType(),True),
        t.StructField("attributes",t.StringType(),True),
        t.StructField("isOriginalTitle",t.BooleanType(),True)])
    df=spark_s.read.options(delimiter='\t').csv(path,header=True,schema=sch)
##    dff=df.select("title","region","language").where(f.col("region") == "UA").where(f.col("language") != "\\N")
##    df_uk=df.select("title","region","language").where(f.col("language") == "uk")
##    df_uk.show()
##    df_ua=df.select("title","region","language").where(f.col("region") == "UA").where((f.col("language") == "\\N") | (f.col("language") == "uk"))
##    df_ua.show()
##    df_uk=df.where(f.col("language") == "uk").select("title")
##    df_uk.show()
##    df_ua=df.where(f.col("region") == "UA").where((f.col("language") == "\\N") | (f.col("language") == "uk")).select("title")
    df_ua=df.where((f.col("language") == "uk")|((f.col("region") == "UA")&(f.col("language") == "\\N")))
    df_ua.show(300)
    po="D:\\IPAM_ProfIT\\project\\res\\ukr"
##    df_uk.write.mode("overwrite").csv(po,header=True)
    df_ua.write.mode("overwrite").csv(po,header=True)

if __name__ == "__main__":
    ukr()
