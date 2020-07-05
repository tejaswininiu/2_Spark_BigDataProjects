Use cases 1 &2 uses simple Spark Framework RDD's Concept. Coded in python(Pyspark).

Usecase1 requirement:
    1.Read the file from S3 (s3://data-engineer-training/data/card_transactions.json)
    File has json records
    Each record has fields:
        user_id
        card_num
        merchant
        category
        amount
        ts
    2.For the one month (1st Feb to 29th Feb 2020: 1580515200 <= ts < 1583020800), perform below analysis:
    Get the total amount spent by each user
    Get the total amount spent by each user for each of their cards
    Get the total amount spend by each user for each of their cards on each category
    Get the distinct list of categories in which the user has made expenditure
    Get the category in which the user has made the maximum expenditure



usecase2 requirement:

    1.   Configuration JSON file: (JSON file Consists of input file location, output file location, delimiter in the actual data file)
        The configuration file is at: s3://data-engineer-training/data/auto_loan.json
    2.   Auto Loan Datasets: It has following fields
        application_id
        customer_id
        car_price
        car_model
        customer_location
        request_date
        loan_status
  3.  The input location and delimiter is present in the configuration file (Hint: Use sc.wholeTextFiles to read the configuration file and then parse it using the json module)
 4.   The first line of the file is the header
Perform following analysis
 1.   The month in which maximum loan requests were submitted in the last one year [2019-04-01 to 2020-03-31]
 2.   Max, Min and Average number of applications submitted per customer id
 3.   Top 10 highest car price against which applications got approved
 4.   For each customer location, top 5 car models which have most loan applications in the last month
