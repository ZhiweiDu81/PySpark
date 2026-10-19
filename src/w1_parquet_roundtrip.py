from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, StringType, MapType

path = "/Users/zhiwei/PySparkDemo/data/01_standard_nested_samples.json"
out_path = "/Users/zhiwei/PySparkDemo/output/example_parquet"


spark = (SparkSession.builder
          .master("local[*]")
         .appName("FirstTry")
         .getOrCreate())
                             
df=spark.read.option("multiLine",True).json(path)

#df.select(F.col("design.applicationDate")).show(3, truncate=False)
#df.select(F.to_date(F.col("design.applicationDate"))).show(3)

#withColum是创建一个新的列


df3 = (df.withColumn("office_code",F.col("design.registrationOfficeCode"))
       .withColumn("application_date",F.to_date(F.col("design.applicationDate")))
       .drop("himmpat_figure"))

 #4
#输出parquet
(df3.write
    .mode("overwrite")
    .partitionBy("office_code")
    .parquet(out_path))
#5
df_back=spark.read.parquet(out_path)



#6 assert这个用法比较有意思
assert df3.count()==df_back.count(),"行数不一致"
def cols(d):
    return sorted((f.name, f.dataType.simpleString()) for f in d.schema.fields)

assert cols(df3) ==cols(df_back),"schema不一致"
print("检验通过")
spark.stop()

 #@df = spark.createDataFrame([
#@    (1,2.,date(2026,1,1),datetime(2026,1,1,12,10,10)),
#@    (3,3.,date(2026,2,1),datetime(2026,1,1,12,10,10)),
#@    (4,4.,date(2026,3,1),datetime(2026,1,1,12,10,10))
#@, schema ='a long, b double, c date, d timestamp')
#@
#@df.show(1)
#@df.printSchema()