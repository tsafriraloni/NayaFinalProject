import pymongo
import pandas as pd


class check_mongo_results():

    def check_mongo(self, selected_budget, selected_departure_date, selected_departure_return_date,selected_user_email):


                myclient_flights = pymongo.MongoClient("mongodb://localhost:27017/")
                mydb_flights = myclient_flights["travel_app"]
                mycol_User_flights = mydb_flights["Flights"]

                # This is a cursor instance
                cur = mycol_User_flights.find()

                results = list(cur)

                # Checking the cursor is empty
                # or not
                if len(results) == 0:
                        print("Empty Cursor")
                        return pd.DataFrame()
                else:


                        #data = pd.DataFrame(list(mycol_User_flights.find({"date_departure" : { "$in": [selected_departure_date] } },{"date_departure_return" : { "$in": [selected_departure_return_date] } })))
                        data = pd.DataFrame(list(mycol_User_flights.find({"date_departure": {"$in": [selected_departure_date] },"date_departure_return" : { "$in": [selected_departure_return_date] } })))

                        data = data.dropna()

                        len_data = len(data.index)

                        data = data.drop_duplicates()

                        data.columns = data.columns.str.replace(' ', '')

                        data = data.reindex(data.columns.tolist(), axis=1)


                        data = data.applymap(str)


                        data = data.loc[:,
                               ["location_arrival", "location_departure", "date_departure", "date_departure_return", "price"]]


                        data = data.loc[
                            data["price"].apply(pd.to_numeric) <= int(selected_budget)].dropna()

                        data.price = pd.to_numeric(data.price, errors='coerce')


                        data = data.drop_duplicates()

                        data = data.sort_values(by=['price'], ascending=True).head(3)

                        data = data.head(3)

                        print(data)

                        return data

#myclient = pymongo.MongoClient("mongodb://localhost:27017/")
#mydb = myclient["travel_app"]
#mycol_User_Requests = mydb["Users_Requests"]

#selected_user_email = 'tsafrir.aloni@gmail.com'

#for x in mycol_User_Requests.find({"User_Email" : { "$in": [selected_user_email] } }):
#    selected_user_email = x["User_Email"]
#    selected_budget = x["Budjet"]
#    selected_departure_date = x["Departure_Date"]
#    selected_departure_return_date = x["Departure_Return_Date"]

#print(user_email,selected_budget,selected_departure_date,selected_departure_return_date)

#check_mongo_results().check_mongo(selected_budget,selected_departure_date,selected_departure_return_date,selected_user_email)