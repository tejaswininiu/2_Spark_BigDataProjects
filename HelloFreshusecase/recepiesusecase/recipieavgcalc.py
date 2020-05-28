from pyspark import SparkContext, SparkConf
# import logging
# logging.basicConfig(filename='test.log',level=logging.INFO)
from pyspark.sql import SparkSession
if __name__ == '__main__':
    conf = SparkConf().setAppName("Spark recipie_avg_time")
    sc = SparkContext(conf=conf)
    log4jLogger = sc._jvm.org.apache.log4j
    log = log4jLogger.LogManager.getLogger(__name__)
    spark = SparkSession.builder.master("local").appName("Hello Fresh Useccase").getOrCreate()
    #sc.setLogLevel("WARN")

###code starts here.............
###reading data
log.warn("reading data from source")
recipie_df=spark.read.format("json").load("E:/TEJA/interview&prep/stemopttraining/recepiesusecase/Resources/data/2020/may/17may/recipes.json")
log.warn("cache the data")
recipie_df.cache()


###filtering out null values
from pyspark.sql.functions import expr, col, column
log.warn("filtering out null values: null values means invalid data")
filter_df=recipie_df.filter((col("cookTime")!='') & (col("prepTime")!=''))

#further using filtered df
#converting isodate duration to totalseconds and sending in int format

from pyspark.sql.types import IntegerType
import isodate
from pyspark.sql.functions import udf
def durudf(s):
  duration=isodate.parse_duration(s)
  return int(duration.total_seconds())

duration_udf = udf(durudf, IntegerType())
#adding columns with converted cookTime and PrepTime
log.warn("converting the time formats from iso duration format to seconds")
convert_df = filter_df.withColumn("cookdur", duration_udf("cookTime")).withColumn("prepdur",duration_udf("prepTime"))

#adding column with total time calculation
log.warn("calculating total time taken to prepare")
totaldur_df=convert_df.withColumn("Totaldurmin",(expr("cookdur")+expr("prepdur"))/60)
#uncomment below code if need to look at the new dataframe

#new column with categories
from pyspark.sql import functions as F
log.warn("categorizing the recipies based on total time taken in preparing them")
category_df = totaldur_df.withColumn("categ",(F.when(totaldur_df['Totaldurmin']<= 30,"easy").when(totaldur_df['Totaldurmin']<=60,"Medium").otherwise("Hard")))


#******facing issue in the below part*****************
#grouping by categories and finding average in each category
from pyspark.sql.functions import count, expr,avg
result_df= category_df.groupBy("categ").agg(avg("Totaldurmin").alias("avg_time"),expr("count(categ)").alias("countofrecipiesincategory"))
#storing results in the same project results folder. can store in local as the result df is not huge
result_df.coalesce(1).write.format("csv").save("results",mode="overwrite")
