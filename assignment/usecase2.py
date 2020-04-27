# Databricks notebook source
# Instruction 1: Access to the cluster--
ACCESS_KEY  = 'AKIASE7ZJRTXHT3RCVY5'
SECRET_KEY  = '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'
BUCKET_NAME = 'data-engineer-training'
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3n.awsAccessKeyId", ACCESS_KEY)
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3n.awsSecretAccessKey", SECRET_KEY)

# COMMAND ----------

#Instruction 2: Access the data location:
#2a:read the location details file
jsonstring=sc.wholeTextFiles("s3://data-engineer-training/data/auto_loan.json")
import json
#2b:using json module to read the contents of the file which are in json format
rdd1=jsonstring.values().map(lambda x: json.loads(x))

# COMMAND ----------

#instruction 3:
#reading data file details from configdetails file.
#input_loc contains input location path, output_loc contains output location path...
#....delimter contains delimter details to use while reading the data
details=rdd1.collect()
input_loc,output_loc,delimter=details[0].values()

# COMMAND ----------

#instruction 4:
#4areading the data from csv file
#4b:Skip the header while reading the date
from itertools import islice
rawdataRDD = sc.textFile(input_loc)
#print(rawdataRDD.take(5))
#**partition 1 contains the header, so the skip the first record if the index is 0
rawdataRdd1=rawdataRDD.mapPartitionsWithIndex(lambda idx,iter : islice(iter,1,None) if idx==0 else iter)
#print(rawdataRdd1.take(5))

# COMMAND ----------

#instruction 5:
#converting the csv file to json format for easy access
from datetime import date
#read the record by splitting using delimter.
#respective data after splitting is read into app_id, cus_id,car_price,car_model,cus_loc,req_date,loan_status
def getjsonformat(input_record):
  app_id,cus_id,car_price,car_model,cus_loc,req_date,loan_status = input_record.split(delimter)
  year,month,day=req_date.split("-")
  c_date=date(int(year),int(month),int(day))
  return ({"app_id":app_id, "cus_id":cus_id, "car_price":car_price, "car_model":car_model,"cus_loc":cus_loc,"req_date":c_date,"status":loan_status})
jsondata = rawdataRdd1.map(getjsonformat)
#jsondata is the new rdd which contains data in json format and used further for processing

# COMMAND ----------

#instruction 6:
#persist this newly created raw data rdd for further processing. 
from pyspark.storagelevel import StorageLevel
jsondata.persist(StorageLevel.MEMORY_ONLY)

# COMMAND ----------

#instruction 7:
#****Question 1.The month in which maximum loan requests were submitted in the last one year [2019-04-01 to 2020-03-31]****
#
#filter for last one year data
#
oneyeardata = jsondata.filter(lambda x : date(2019,4,1)<= x["req_date"] <= date(2020,3,31))
# 
#creating rdd with just month 
#
monthrdd=oneyeardata.map(lambda x: (x["req_date"].month,1))
#
#counting number of application per each month-- logic: counting number of times each month appeared.
#
maxmonthrdd=monthrdd.reduceByKey(lambda x,y:x+y)
#
# Info on months numbers:
# 1- January, 2-Febrarury, 3- March, 4- April, 5- May, 6-June, 7- July, 8- august, 9- September,10- October, 11- November, 12- December
# April through December are for year 2019, and January through March are for year 2020
#
#
#The result rdd is reversed, where "count" is the key and the "month" is the value:
#
countmonthrdd=maxmonthrdd.map(lambda x : (x[1],x[0]))
#
# The resultant rdd is Sorted ByKey--
#--thus the rdd values are present in order per the count--
#---where the month with max count lies on the top.
#
result1 = countmonthrdd.sortByKey().top(1)
for e, value in result1:
  print("Month with max application is-- {} with count--> {}".format(value,e))

# COMMAND ----------

#instruction 8:
#
#***** Question 2.Max, Min and Average number of applications submitted per customer id******
#
#function which accepts an rdd and returns the max, min, avg values:
#
def maxminavgval(inputrdd):
  #rdd with customerid's
  customerrdd=inputrdd.map(lambda x: (x["cus_id"],1))
  #counting number of applications per each customer id-- logic: counting number of times each customer id appeared.
  countcustrdd=customerrdd.reduceByKey(lambda x,y:x+y)
  #rdd of count values
  valuesrdd =countcustrdd.values()
  #returns max, min, avg values
  return valuesrdd.max(),valuesrdd.min(),valuesrdd.mean()
#
## accessing the function: for the over all data: use jsondata rdd
#
print("For Over all data:")
amaxval,aminval,aavgval= maxminavgval(jsondata)
print("Maximum number of applications submitted per customer_id in general-->",amaxval)
print("Minimum number of applications submitted per customer_id in general-->",aminval)
print("Average number of applications submitted per customer_id in general-->", round(aavgval,2))
#
## accessing the function: for just one year data: use oneyeardata rdd
#
print("For past one year data:")
maxval,minval,avgval = maxminavgval(oneyeardata)
print("Maximum number of applications submitted per customer_id in general-->",maxval)
print("Minimum number of applications submitted per customer_id in general-->",minval)
print("Average number of applications submitted per customer_id in general-->", round(avgval,2))


# COMMAND ----------

#instruction 9:
#
#***Question 3. Top 10 highest car price against which applications got approved****
#function returns: integer format of the carprice from the record.
def getpricestatusdata(input_record):
  return (int(input_record["car_price"]))
#***for over_all data: used jsondata rdd
#filter data for records where loan_status is approved.
statusfilterrdd=jsondata.filter(lambda x: x["status"]=="approved")
#create rdd with car_price
pricestatusrdd=statusfilterrdd.map(getpricestatusdata)
print("************for over all data*************")
#getting top 10 elements from the prices sorted in descending order.
print("Top 10 Highest car Prices against which applications got approved:",pricestatusrdd.takeOrdered(10,key=lambda s:-1*s))
#***for past one year data: used oneyeardata rdd
print("*********::for past one year data::********")
y_statusfilterrdd=oneyeardata.filter(lambda x: x["status"]=="approved")
y_pricestatusrdd=y_statusfilterrdd.map(getpricestatusdata)
print("Top 10 Highest car Prices against which applications got approved:",y_pricestatusrdd.takeOrdered(10,key=lambda s:-1*s))


# COMMAND ----------

#instruction 10:
#****Question 4.   For each customer location, top 5 car models which have most loan applications in the last month***
# filter rdd for for last month: considering last month as 2020-03-01 to 2020-03-31
onemonthrdd=oneyeardata.filter(lambda x : date(2020,3,1)<=x["req_date"]<=date(2020,3,31))
#create rdd:
#key value pair rdd of customer (location, car model), 1, where 1 is the value to represent it has occured once in that record.
custcarmodelrdd=onemonthrdd.map(lambda x :((x["cus_loc"],x["car_model"]),1))
#performing summation to find number of applications for each key: per state, per car model:total applications
count_custmodelrdd=custcarmodelrdd.reduceByKey(lambda x,y:x+y)
#re -ordering the key value pair- key is (customer_location,count of applications) and value is caar model
rc_custmodelrdd=count_custmodelrdd.map( lambda x : ((x[0][0],x[1]),x[0][1]))
#key function: to use in the function repartitionAndSortWithinPartitions.
#Key function says to perform ordering based on customer_location(ascending order) and then based on count of applications(Descending order)
def key_func(x):
  return x[0],x[1]*-1
from pyspark.rdd import portable_hash
#performing patitioning based on the customer_location
#
re_rdd=rc_custmodelrdd.repartitionAndSortWithinPartitions(1,lambda x: portable_hash(x[0])%1, keyfunc=key_func)
#
#re-ordering the key value pair back: customer location,(count,carmodel)
#performing groupby key operation, which groups all the values based on key i.e customer_location
#get the top 5 car models for each location # logic is getting the first 5 elements from the list.
#
result4 = re_rdd.map(lambda x : (x[0][0],(x[0][1],x[1]))).groupByKey()
for e, valuelist in result4.collect():
  print("{}-->{}".format(e,list(valuelist)[:5]))
#savinf to a file:
result4.mapValues(lambda x: list(x)[:5]).saveAsTextFile("result4")

# COMMAND ----------

##test
#saving to file
newRDD=sc.textFile("result4")
print(newRDD.take(5))
