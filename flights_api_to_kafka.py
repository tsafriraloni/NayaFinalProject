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



class api_to_kafka_class():





    def api_to_kafka(self, selected_budget,selected_departure_date,selected_departure_return_date,user_email):

            topic = c.topic5
            topic_S3 = c.topic8

            brokers = c.bootstrapServers

            #print(user_email)

            date_departure = selected_departure_date

            location_arrival = "NYC"

            date_departure_return = "2023-03-01"

            list_of_cities = ["NEW YORK","MADRID","PARIS","MOSCOW","LONDON","ISTANBULE","BARCELONA","DUBLINE","RIO","MANILA","BELFAST","BANKOK","HAMBURG","HONG KONG","GENUA","LISBON"]

            list_of_carriers = ["UA","LFT","QTR","ELY","BAW","AFR","UAL","DAT","IB","LOT","RJA","EZY","RYR"]

            list_of_class_type = ["ECO","BUS","FIR"]

            # First we set the producer.
            producer = KafkaProducer(bootstrap_servers = brokers)


            url = c.flights_api

            querystring = {"itinerary_type": "ROUND_TRIP", "class_type": "ECO", "location_arrival": "NYC",
                           "date_departure": "2023-01-28", "location_departure": "TLV", "sort_order": "PRICE",
                           "number_of_stops": "1", "price_max": "600", "number_of_passengers": "1",
                           "duration_max": "2051", "price_min": "100", "date_departure_return": "2023-02-16"}

            headers = {
                "X-RapidAPI-Key": "9556b44064mshfe8d4aeba9c478fp1a1496jsn8b62c9d979be",
                "X-RapidAPI-Host": "priceline-com-provider.p.rapidapi.com"
            }

            response = requests.request("GET", url, headers=headers, params=querystring)

            json_object = json.loads(response.text)

            check_results = len(json_object["pricedItinerary"][0]["pricingInfo"])

            if check_results > 0:

                                    json_formatted_str = json.dumps(json_object, indent=30)

                                    for item in json_object["pricedItinerary"]:

                                        random_city = random.choice(list_of_cities)


                                        try:


                                            prepare_flight_json = '{"location_arrival":' + '"' + random_city + '"' + ',"date_departure":' + '"' + selected_departure_date + '"' + ',"location_departure":"TEL AVIV","date_departure_return":' + '"' + selected_departure_return_date + '"' + ',' + '"price" :' + '"' + str(item["pricingInfo"].get('totalFare')) + '"' + ',' + '"currencyCode" :' + '"' + item["pricingInfo"].get('currencyCode') + '"' + ',' + '"carrier" :' + '"' + item["pricingInfo"].get('ticketingAirline') + '"' + ',' + '"totalTripDurationInHours" :' + '"' + str(item["totalTripDurationInHours"]) + '"' + ',' + '"user_email" :' + '"' + user_email + '"' + '}'

                                            for number in range(100):

                                                random_city = random.choice(list_of_cities)

                                                random_carrier = random.choice(list_of_carriers)

                                                random_price_1 = randint(350, 600)

                                                random_number = randint(10, 100)

                                                random_price = (random_price_1 - random_number)

                                                random_totalTripDurationInHours = randint(8, 24)

                                                prepare_flight_json = '{"location_arrival":' + '"' + random_city + '"' + ',"date_departure":' + '"' + selected_departure_date + '"' + ',"location_departure":"TEL AVIV","date_departure_return":' + '"' + selected_departure_return_date + '"' + ',' + '"price" :' + '"' + str(random_price) + '"' + ',' + '"currencyCode" :' + '"' + item["pricingInfo"].get('currencyCode') + '"' + ',' + '"carrier" :' + '"' + random_carrier + '"' + ',' + '"totalTripDurationInHours" :' + '"' + str(random_totalTripDurationInHours) + '"' + ',' + '"user_email" :' + '"' + user_email + '"' + '}'
                                                print(prepare_flight_json)


                                                producer.send(topic=topic, value=prepare_flight_json.encode('utf-8') )
                                                producer.send(topic=topic_S3, value=prepare_flight_json.encode('utf-8'))

                                            producer.flush()
                                            sleep(1)
                                                #print(prepare_flight_json)

                                        except Exception:

                                            None
            else:
                None

myclient = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = myclient["travel_app"]
mycol_User_Requests = mydb["Users_Requests"]

for x in mycol_User_Requests.find():
    user_email = x["User_Email"]
    selected_budget = x["Budjet"]
    selected_departure_date = x["Departure_Date"]
    selected_departure_return_date = x["Departure_Return_Date"]

#api_to_kafka_class().api_to_kafka(selected_budget,selected_departure_date,selected_departure_return_date,user_email)