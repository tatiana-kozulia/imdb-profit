import sys
from pyspark import SparkConf
from pyspark.sql import SparkSession,Window
import pyspark.sql.types as t
import pyspark.sql.functions as f

def xix():
    path="D:\\IPAM_ProfIT\\project\\data\\name.basics.tsv.gz"
    spark_s=(SparkSession.builder
             .master("local")
             .appName("task app")
             .config(conf=SparkConf())
             .getOrCreate())
    sch=t.StructType([
        t.StructField("nconst",t.StringType(),True),
        t.StructField("primaryName",t.StringType(),True),
        t.StructField("birthYear",t.DateType(),True),
        t.StructField("deathYear",t.DateType(),True),
        t.StructField("primaryProfession",t.StringType(),True),
        t.StructField("knownForTitles",t.StringType(),True)])
    df=spark_s.read.options(delimiter='\t').csv(path,header=True,schema=sch)
##    df_xix=(df
##            .select("primaryName","birthYear")
##            .where((f.to_date(f.col("birthYear"),'YYYY') > "1799")
##                   & (f.to_date(f.col("birthYear"),'YYYY') < "1900")))
    df_xix=(df
            .where((f.to_date(f.col("birthYear"),'YYYY') > "1799")
                   & (f.to_date(f.col("birthYear"),'YYYY') < "1900"))
            .select("primaryName"))
    df_xix.show()
    po="D:\\IPAM_ProfIT\\project\\res\\xix"
    df_xix.write.mode("overwrite").csv(po,header=True)

if __name__ == "__main__":
    xix()
