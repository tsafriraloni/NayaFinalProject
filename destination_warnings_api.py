import requests
import json
import codecs

url = "https://data.gov.il/api/3/action/datastore_search?resource_id=2a01d234-b2b0-4d46-baa0-cec05c401e7d&limit=10000"

response = requests.request("GET", url)

json_object = json.loads(response.text)

json_formatted_str = json.dumps(json_object, indent=2, ensure_ascii=False)

print(json_formatted_str)

log_path = '/home/naya/flights_project/destination_flight_warnings_json.json'
with open(log_path, "w") as file:


    for item in json_object["result"]["records"]:

        prepare_destination_flight_warnings_json = '{"chat_id":"12345"' + ','  +  '"continent" :' + '"' + str(item["continent"]).strip() + '"' + ','  +  '"country" :' + '"' + str(item["country"]).strip() + '"' + ','  +  '"recommendations" :' + '"' + str(item["recommendations"]).strip() + '"' + '}'
        print(prepare_destination_flight_warnings_json)

        file.write(prepare_destination_flight_warnings_json + "\n")


    file.close()


