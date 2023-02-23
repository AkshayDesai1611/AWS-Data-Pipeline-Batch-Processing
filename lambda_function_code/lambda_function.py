import json
import pymongo
import certifi
import logging
import os
import boto3
import datetime
import os
import requests
import os

#Certificate that helps to connect with mongo db dynamically
ca = certifi.where()

DATABASE_NAME = os.getenv("DATABASE_NAME")
COLLECTION_NAME = os.getenv("COLLECTION_NAME")
MONGODB_URL = os.getenv("MONGODB_URL")
BUCKET_NAME=os.getenv("BUCKET_NAME")

DATA_SOURCE_URL = f"https://www.consumerfinance.gov/data-research/consumer-complaints/search/api/v1/" \
                  f"?date_received_max=<todate>&date_received_min=<fromdate>" \
                  f"&field=all&format=json"

client = pymongo.MongoClient(MONGODB_URL,tlsCAFile=ca)

"""
from_date: whenever the date/time is given in the form of string
datetime.datetime.strptime is used to convert it into date format
strptime takes the variable and %Y - year %m-month %d-day
"""
def get_from_date_to_date():
    from_date = "2023-01-01"
    from_date = datetime.datetime.strptime(from_date,"%Y-%m-%d")

    if COLLECTION_NAME in client[DATABASE_NAME].list_collection_names():

        res = client[DATABASE_NAME][COLLECTION_NAME].find_one(sort=[("to_date",pymongo.DESCENDING)])
        if res is not None:
            from_date = res["to_date"]
    to_date = datetime.datetime.now() #current date
    """
    if the above from_date: 2011-01-01 does not exist in mongo db collection
    then the max to_date from the pymongo collection will be considered as from_date
    and current date will be considered as to_date. This is the meaning of above 4 
    lines of code. Response in the below format is returned.
    """

    response = {
        "from_date": from_date.strftime("%Y-%m-%d"),
        "to_date": to_date.strftime("%Y-%m-%d"),
        "from_date_obj": from_date,
        "to_date_obj": to_date
    }
    logging.info(f"From date and to date {response}")
    return response
"""
in the above response, from_date,to_date is typecasted back again into string format
and from_date_object and to_date_obj is created because we want to store it in mongo db
"""

"""
save_from_date_to_date: will save the data in the form of dict
into the mongo db collection
"""

def save_from_date_to_date(data,status=True):
    data.update({"status": status})
    logging.info(f"Saving from_date and to_date {data}")
    client[DATABASE_NAME][COLLECTION_NAME].insert_one(data)

def lamda_handler(event,context):
    print(event,context)
    from_date,to_date,from_date_obj,to_date_obj = get_from_date_to_date().values()
    #current_date = datetime.datetime.now().strftime("%Y-%m-%d")
    if to_date==from_date:
        return{
            'statusCode': 200,
            'body': json.dumps('Pipeline has already downloaded all the data upto yesterday')
        }
    url = DATA_SOURCE_URL.replace("<todate>", to_date).replace("<fromdate>", from_date)
    #replacing the from and to date in the url so that we can get the exact url for our data
    data = requests.get(url, params={'User-agent': f'your bot '})

    finance_complaint_data = list(map(lambda x: x["_source"],
                                      filter(lambda x: "_source" in x.keys(),
                                             json.loads(data.content)))
                                  )
    s3 = boto3.resource('s3')
    s3object = s3.Object(BUCKET_NAME,
                         f"inbox/{from_date.replace('-', '_')}_{to_date.replace('-', '_')}_finance_complaint.json")
    s3object.put(
        Body=(bytes(json.dumps(finance_complaint_data).encode('UTF-8')))
    )

    save_from_date_to_date({"from_date": from_date_obj, "to_date": to_date_obj})
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }



