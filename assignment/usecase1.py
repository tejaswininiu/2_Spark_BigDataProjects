# Databricks notebook source
#UseCase1******Card_transaction

# COMMAND ----------

#Access keys to the Filesystem where the file is stored
ACCESS_KEY  = 'AKIASE7ZJRTXHT3RCVY5'
SECRET_KEY  = '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'
BUCKET_NAME = 'data-engineer-training'
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3n.awsAccessKeyId", ACCESS_KEY)
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3n.awsSecretAccessKey", SECRET_KEY)

# COMMAND ----------

#** Instruction:1
#Now read the file and create rdd.
#filter the rdd for Febrauray month data
#put the filtered rdd in cache, to persist throught out in the book for all other operations.
jsonstring= sc.textFile('s3://data-engineer-training/data/card_transactions.json')
import json
#read json format
rawdata= jsonstring.map(lambda textline:json.loads(textline))
#filter
febdatardd=rawdata.filter(lambda x: 1580515200 <= x["ts"] < 1583020800)
#persist
febdatardd.cache()

# COMMAND ----------

#instruction :2
#Few utility functions
#sum of amounts
def sumamt(amt1,amt2):
  return amt1+amt2

# COMMAND ----------

#*****1.total amount spent by each user*****
#instruction:3
#key value pair rdd for user_id and amount
userrdd=febdatardd.map(lambda x:(x["user_id"],x["amount"]))
#perform summation on amount
#printing the total amount per user
for e,value in userrdd.reduceByKey(sumamt).collect():
  print("{}-->${}".format(e,value))

# COMMAND ----------

#****2.Get the total amount spent by each user for each of their cards******
#instruction:4
#key value pair rdd for (user_id, card number) and amount
usrcardrdd=febdatardd.map(lambda x:((x["user_id"],x["card_num"]),x["amount"]))
#perform summation on amount
#printing the total amount per user, card_number combination
for e,value in usrcardrdd.reduceByKey(sumamt).sortByKey().collect():
  print("{} on {}-->${}".format(e[0],e[1],value))

# COMMAND ----------

#****3.Get the total amount spend by each user for each of their cards on each category****
#instruction:5
#key value pair rdd for (user_id, card number,category) and amount
usrcardcatrdd=febdatardd.map(lambda x:((x["user_id"],x["card_num"],x["category"]),x["amount"]))
#perform summation on amount
resultrdd=usrcardcatrdd.reduceByKey(sumamt)
#perform sort operation based on key
sortedlist=resultrdd.sortByKey().collect()
#printing the total amount per user, card_number,category combination
for e,value in sortedlist:
  print("{} with {} on {} ---->${}".format(e[0],e[1],e[2],value))

# COMMAND ----------

#*****4.Get the distinct list of categories in which the user has made expenditure*****
#instruction :6
#
####functions needed for combinebykey function
#
#intialize function:create combiner
def intialize(category):
    return set([category])
#merge value
def add(set_categories,category):
    set_categories.add(category)
    return set_categories
#merge combiners
def merge(set_categories1,set_categories2):
    set_categories1.update(set_categories2)
    return  set_categories1
#
#creating rdd
##key value pair rdd for user_id and category
usrcatrdd=febdatardd.map(lambda x:(x["user_id"],x["category"]))
#getting distinct list of categories based on key i.e user_id
distinct_cate=usrcatrdd.combineByKey(intialize,add,merge)
#printing the distinct list of categories the user had made expenditure for reach user.
for e, value in distinct_cate.collect():
  print("{}-->{}".format(e,value))

# COMMAND ----------

#******5.Get the category in which the user has made the maximum expenditure**********
#instruction :6
#*********************************case a: if maximum expenditure has to be the summation of the expenditure on a particular card.
##creating rdd
##key value pair rdd for (user_id,category) and amount
usrcatrdd=febdatardd.map(lambda x:((x["user_id"],x["category"]),x["amount"]))
#performing summation on the amount for each user_id, category combination
sum_amount=usrcatrdd.reduceByKey(lambda x,y:x+y)
#re-ordering the key value pair- (user_id, (category,amount))
sum_amountref=sum_amount.map(lambda x:(x[0][0],(x[0][1],x[1])))
#utility function to find max amount. returnning who value pair (category & amount) for element with maximum amount
def maxamount(element1,element2):
    if(element1[1]>element2[1]):
        return element1
    else:    
        return element2
#Finding category with maximum amount
max_amounta=sum_amountref.reduceByKey(maxamount)
print("category with max spend per user")
for e,value in max_amounta.collect():
  print("{}---->{}--(${})".format(e,value[0],value[1]))

# COMMAND ----------

#b. if maximun expenditure is  referring to a transaction*********(extracase)
# # rawdata5=febdatardd.map(lambda x:(x["user_id"],(x["category"],x["amount"])))
# # def maxamount(element1,element2):
# #     if(element1[1]>element2[1]):
# #         return element1
# #     else:
# #         return element2
# # max_amountb=rawdata5.reduceByKey(maxamount)
# # print("category in which user has spend maximum amount(transaction basis)")
# # for e,value in max_amountb.collect():
# #     print("{}---->{}--({})".format(e,value[0],value[1]))
