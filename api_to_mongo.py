import requests
import json
from kafka import KafkaProducer
from time import sleep
import configuration as c
import random
from random import randint
import pymongo

global chat_id
global location_arrival
global date_departure
global price

class Flights():

    def find_flight(self, city):

        global df_list_airports
        global df_airports
        global df_airports_string


        myclient = pymongo.MongoClient("mongodb://localhost:27017/")
        mydb = myclient["travel_app"]
        mycol = mydb["Flights"]

        x = mycol.delete_many({})

        mycol.insert_one(list_json[i])


        return df_list_airports

app_flights = Flights().find_flight('New York')

topic = c.topic4
topic1 = c.topic3
brokers = c.bootstrapServers

price = 100000

date_departure = "2023-02-01"

location_arrival = "NYC"

date_departure_return = "2023-03-01"

list_of_cities = ["NYC","MAD","PAR","MOW","LON","AAL","IST","LCY","BCN","DUB","DOH","DFW","RIO","MIA","MNL","BOM","BFS","BKK","BOS","CAS","CHI","CPH","CUR","HAM","HKG","JNB","LAX","LCA","LED","LIS"]

list_of_carriers = ["UA","LFT","QTR","ELY","BAW","AFR","UAL","DAT","IB","LOT","RJA","EZY","RYR"]

list_of_class_type = ["ECO","BUS","FIR"]

# First we set the producer.
producer = KafkaProducer(bootstrap_servers = brokers)

#url = "https://priceline-com-provider.p.rapidapi.com/v1/flights/search"
url = c.flights_api

querystring = {"itinerary_type":"ROUND_TRIP","class_type":"ECO","location_arrival": "NYC","date_departure":"2023-02-01","location_departure":"TLV","sort_order":"PRICE","price_max":"1500","number_of_passengers":"1","duration_max":"2051","price_min":"100","date_departure_return":"2023-02-15","number_of_stops":"0"}

headers = {
	"X-RapidAPI-Key": "5f7df78670msh470977d57fc0e58p1bfd80jsnaa924711be5b",
	"X-RapidAPI-Host": "priceline-com-provider.p.rapidapi.com"
}

response = requests.request("GET", url, headers=headers, params=querystring)

json_object = json.loads(response.text)

json_formatted_str = json.dumps(json_object, indent=30)

for item in json_object["pricedItinerary"]:


    try:


        prepare_flight_json = '{"location_arrival":' + '"' + location_arrival + '"' + ',"date_departure":' + '"' + date_departure + '"' + ',"location_departure":"TLV","date_departure_return":' + '"' + date_departure_return + '"' + ',' + '"price" :' + '"' + str(item["pricingInfo"].get('totalFare')) + '"' + ',' + '"currencyCode" :' + '"' + item["pricingInfo"].get('currencyCode') + '"' + ',' + '"carrier" :' + '"' + item["pricingInfo"].get('ticketingAirline') + '"' + ',' + '"totalTripDurationInHours" :' + '"' + str(item["totalTripDurationInHours"]) + '"' + '}'

        for number in range(1000):

            random_city = random.choice(list_of_cities)

            random_carrier = random.choice(list_of_carriers)

            random_price = randint(500, 1500)

            random_totalTripDurationInHours = randint(8, 24)

            prepare_flight_json = '{"location_arrival":' + '"' + random_city + '"' + ',"date_departure":' + '"' + date_departure + '"' + ',"location_departure":"TLV","date_departure_return":' + '"' + date_departure_return + '"' + ',' + '"price" :' + '"' + str(random_price) + '"' + ',' + '"currencyCode" :' + '"' + item["pricingInfo"].get('currencyCode') + '"' + ',' + '"carrier" :' + '"' + random_carrier + '"' + ',' + '"totalTripDurationInHours" :' + '"' + str(random_totalTripDurationInHours) + '"' + '}'
            print(prepare_flight_json)

            #print('a')



            #mycol.insert_one(prepare_flight_json.encode('utf-8'))

            #producer.send(topic=topic, value=prepare_flight_json.encode('utf-8') )
            #producer.send(topic=topic1, value=prepare_flight_json.encode('utf-8'))

        #producer.flush()
        #sleep(1)
            #print(prepare_flight_json)

    except Exception:

        None