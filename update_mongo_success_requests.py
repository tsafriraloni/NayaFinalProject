import pymongo
import datetime
from datetime import date

#### MongoDB Flights Requests Details ####
myquery_flights_requests = { "is_active": 1 }
myclient_flights_requests = pymongo.MongoClient("mongodb://localhost:27017/")
mydb_flights_requests = myclient_flights_requests["Flights_DB"]
mycol_flights_requests = mydb_flights_requests["Flights_Requests"]


#### MongoDB Flights Offered Details ####
myquery_flights_offered = { "is_active": 1 }
myclient_flights_offered = pymongo.MongoClient("mongodb://localhost:27017/")
mydb_flights_offered = myclient_flights_offered["Flights_DB"]
mycol_flights_offered = mydb_flights_offered["Flights_Offered"]


#### Generating Today's date ####
today = date.today()
current_date = today.strftime("%d_%m_%Y")

def format_time():
    t = datetime.datetime.now()
    s = t.strftime('%Y-%m-%d %H:%M:%S.%f')
    return s[:-3]

new_ts = format_time()
current_timestamp =  datetime.datetime.now()

#### Telegram Bot Message to User When Price Has Changed ####
def send_message_to_user(chat_id,user_first_name,new_price,origin_city, dest_city, dep_date, return_date,original_price_total):
  bot.send_message(chat_id, f'Hi {user_first_name}, Good news! The old price for flight from {origin_city} to {dest_city} between '
                             f'{dep_date} and {return_date} was {original_price_total}$, the new price is {new_price}$' )

#### Updating The Relevant Request In MongoDB As Not Active ####
def update_mongo_not_active(chat_id):
  myquery_flights_requests = {"chat_id": chat_id}
  newvalues = {"$set": {"is_active": 0}}
  mycol_flights_requests.update_one(myquery_flights_requests, newvalues)


#### Quering MongoDB For Flights Offered Success Results ####
myquery_flights_offered = { "is_active": 1 }
myclient_flights_offered = pymongo.MongoClient("mongodb://localhost:27017/")
mydb_flights_offered = myclient_flights_offered["Flights_DB"]
mycol_flights_offered = mydb_flights_offered["Flights_Offered"]

#### Pulling The Parameters Of The Active Requests In Order Update The MongoDb Flights Requests And To Send To Amadeus API ####
for x in mycol_flights_offered.find(myquery_flights_offered):
  chat_id = x["chat_id"]
  update_mongo_not_active(chat_id)


