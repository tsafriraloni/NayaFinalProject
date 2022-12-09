import pymongo
import json
from kafka import KafkaProducer
from time import sleep
import requests

global chat_id
global location_arrival
global date_departure
global price

location_arrival = "NYC"
date_departure = "2022-12-15"
price = 100

topic = 'kafka-flights-requests-topic'
brokers = ['cnt7-naya-cdh63:9092']

# First we set the producer.
producer = KafkaProducer(bootstrap_servers = brokers)

#### MongoDB Details ####
#myquery = { "is_active": 1 }
myclient = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = myclient["Flights_DB"]
mycol = mydb["Flights_Requests"]

#### Quering MongoDB For The Active Requests And Send To The Different Api`s ####

#x = mycol.delete_many({})

for x in mycol.find():
  request_id = x["_id"]
  chat_id = x["chat_id"]
  location_arrival = x["location_arrival"]
  date_departure = x["date_departure"]
  price = x["max_price"]
  print(chat_id)

#### Flights Api #########

  url = "https://priceline-com-provider.p.rapidapi.com/v1/flights/search"


  querystring = {"itinerary_type": "ROUND_TRIP", "class_type": "ECO", "location_arrival": location_arrival,"date_departure": date_departure, "location_departure": "TLV", "sort_order": "PRICE","price_max": int(float(price)), "number_of_passengers": "1", "duration_max": "2051", "price_min": "100","date_departure_return": "2022-12-31", "number_of_stops": "0"}


  headers = {
      "X-RapidAPI-Key": "9556b44064mshfe8d4aeba9c478fp1a1496jsn8b62c9d979be",
      "X-RapidAPI-Host": "priceline-com-provider.p.rapidapi.com"
  }

  response = requests.request("GET", url, headers=headers, params=querystring)

  json_object = json.loads(response.text)

  json_formatted_str = json.dumps(json_object, indent=2)

  print(json_formatted_str)

  try:

    for item in json_object["pricedItinerary"]:

        prepare_flight_json = '{"request_id":' + '"' + str(request_id) + '"' + ',"chat_id":' + '"' + chat_id + '"' + ',"first_name":"tsafrir","last_name":"aloni","class_type":"ECO","location_arrival":' + '"' + location_arrival + '"' + ',"date_departure":' + '"' + date_departure + '"' + ',"location_departure":"TLV","date_departure_return":"2022-12-31"' + ',' + '"price" :' + '"' + str(item["pricingInfo"].get('totalFare')) + '"' + ',' + '"currencyCode" :' + '"' + item["pricingInfo"].get('currencyCode') + '"' + ',' + '"carrier" :' + '"' + item["pricingInfo"].get('ticketingAirline') + '"' + ',' + '"totalTripDurationInHours" :' + '"' + str(item["totalTripDurationInHours"]) + '"' + '}'

        print(prepare_flight_json)
        producer.send(topic=topic, value=prepare_flight_json.encode('utf-8'))
        producer.flush()
        sleep(3)
        #print(prepare_flight_json)

  except Exception:

        None