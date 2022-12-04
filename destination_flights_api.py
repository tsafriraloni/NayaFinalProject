import requests
import json

url = "https://priceline-com-provider.p.rapidapi.com/v1/flights/search"

querystring = {"itinerary_type":"ROUND_TRIP","class_type":"ECO","location_arrival":"NYC","date_departure":"2022-12-15","location_departure":"TLV","sort_order":"PRICE","price_max":"1500","number_of_passengers":"1","duration_max":"2051","price_min":"100","date_departure_return":"2022-12-31","number_of_stops":"0"}

headers = {
	"X-RapidAPI-Key": "9556b44064mshfe8d4aeba9c478fp1a1496jsn8b62c9d979be",
	"X-RapidAPI-Host": "priceline-com-provider.p.rapidapi.com"
}

response = requests.request("GET", url, headers=headers, params=querystring)

json_object = json.loads(response.text)

json_formatted_str = json.dumps(json_object, indent=2)

log_path = '/home/naya/flights_project/flight_json.json'
with open(log_path, "w") as file:

    for item in json_object["pricedItinerary"]:

        prepare_flight_json = '{"chat_id":"12345","first_name":"tsafrir","last_name":"aloni","class_type":"ECO","location_arrival":"NYC","date_departure":"2022-12-15","location_departure":"TLV","date_departure_return":"2022-12-31"' + ','  +  '"price" :' + '"' + str(item["pricingInfo"].get('totalFare')) + '"' + ',' + '"currencyCode" :'  + '"' +  item["pricingInfo"].get('currencyCode') + '"' + ',' + '"carrier" :' + '"' + item["pricingInfo"].get('ticketingAirline') + '"' + ',' + '"totalTripDurationInHours" :' + '"' + str(item["totalTripDurationInHours"]) + '"' + '}'
        print(prepare_flight_json)

        file.write(prepare_flight_json + "\n")


    file.close()





#print(json_formatted_str)