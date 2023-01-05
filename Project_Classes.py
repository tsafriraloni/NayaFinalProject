import subprocess
import requests
import os
import signal
import math
import json
import pymongo
import functools
import re
import pandas as pd
import datetime as dt
import time
import streamlit as st
from PIL import Image
import random
import numpy as np
from random import sample
import base64
import os
from email.message import EmailMessage
import ssl
import smtplib
import string
import pretty_html_table
#import seaborn as sns
#from matplotlib import pyplot
from pretty_html_table import build_table
from smtplib import SMTP
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from collections import namedtuple
import altair as alt
import math
import time
import multiprocessing
from time import sleep
import configuration as c
import random
from random import randint
import subprocess
from flights_api_to_kafka import api_to_kafka_class
from hotels_api_to_kafka import api_to_kafka_class_hotels
from check_mongo_for_quick_results import check_mongo_results


class Process(multiprocessing.Process):
    def __init__(self, id):
        super(Process, self).__init__()
        self.id = id

    def run(self):
        time.sleep(1)


class TravelOptions:

    def __init__(self):

        self.get_user_input()

    import os, signal

    def kill_spark_process():

        # Ask user for the name of process
        name = 'SparkUI'
        try:

            # iterating through each instance of the process
            for line in os.popen("ps ax | grep " + name + " | grep -v grep"):
                fields = line.split()

                # extracting Process ID from the output
                pid = fields[0]

                # terminating process
                os.kill(int(pid), signal.SIGKILL)
            print("Process Successfully terminated")

        except:
            print("Error Encountered while running script")



    def empty_dataframe(my_df=None):
        if (my_df is None):
            my_df = pd.DataFrame()
        # stuff to do if it's not empty
        if (len(my_df) != 0):
            print(my_df)
        elif (len(my_df) == 0):
            print("Nothing")
        return my_df

    global default_empty_dataframe

    default_empty_dataframe = empty_dataframe(None)

    def get_user_input(self):
        global selected_city
        global selected_flight_budget
        global selected_hotel_budget
        global selected_restorants_budget
        global user_email
        # global df_list_airports
        # global df_airports
        # global app_flights
        global our_email

        global selected_budget
        global selected_departure_date
        global selected_departure_return_date


        ############################### set_background ################################

        def get_base64(bin_file):
            with open(bin_file, 'rb') as f:
                data = f.read()
            return base64.b64encode(data).decode()

        def set_background(png_file):
            bin_str = get_base64(png_file)
            page_bg_img = '''
                    <style>
                    .stApp {
                    background-image: url("data:image/png;base64,%s");
                    background-size: cover;
                    }
                    </style>
                    ''' % bin_str
            st.markdown(page_bg_img, unsafe_allow_html=True)

        set_background(r"vacation_travel_picture.png")



        st.markdown("<h1 style='text-align: center; color: green;'>User Requests</h1>", unsafe_allow_html=True)

        regex = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
        check_email = 'N'
        keys = random.sample(range(1000, 9999), 1)

        ################################### check email format #########################################################

        def check(email):

            email = email.strip()

            if (re.fullmatch(regex, email)):

                check_email = 'Y'

            else:

                check_email = 'N'

            return check_email

        global letters
        global letters_random

        global form_random_key

        letters = string.ascii_lowercase

        form_random_key = 'my_form' + letters



        form = st.form(key=f"{form_random_key}")



        selected_budget = form.text_input(label='Enter Your Budget')


        ############################### flight budget choose ########################

        global selected_flight_budget

        selected_flight_budget = '1500'

        selected_departure_date = form.text_input(label='Enter Your Departure Date')



        ########################### Hotel Budget Choose ######################################
        global selected_hotel_budget

        selected_hotel_budget = '150'

        selected_departure_return_date = form.text_input(label='Enter Your Departure Return Date')


        ########################### user email input + check user format #######################

        user_email = form.text_input(label='Enter Your Email', key=1)

        submit_counter = 0

        submit_button = form.form_submit_button(label='Submit')

        if submit_counter == 0:
            letters = string.ascii_lowercase
            form_random_key = 'my_form' + letters
            submit_counter = submit_counter + 1
        else:
            letters = string.ascii_lowercase
            letters_random = ''.join(random.choice(letters) for i in range(10))
            form_random_key = 'my_form' + letters_random
            form = st.form(key=f"{form_random_key}")

            # user_email = st.text_input('Please Enter Your Email :')
        for i in keys:
            if submit_button:

                letters = string.ascii_lowercase
                letters_random = ''.join(random.choice(letters) for i in range(10))
                form_random_key = 'my_form' + letters_random
                form = st.form(key=f"{form_random_key}")

                if check(user_email) == 'N':
                    st.write('The email is invalid please type again and press submit')
                    None
                else:
                    self.Process_User_Input()
                    #self.send_to_user_email(user_email)
                    form = st.empty()
                    submit_button = st.empty()

                    letters = string.ascii_lowercase
                    letters_random = ''.join(random.choice(letters) for i in range(10))
                    form_random_key = 'my_form' + letters_random
                    form = st.form(key=f"{form_random_key}")
                    break

    def Process_User_Input(self):

        with st.spinner('Your Travel Recommendations Are On there Way,Please Wait...'):

            app_flights = Flights().find_flight(selected_budget,selected_departure_date,selected_departure_return_date,user_email)
            #app_hotels = Hotels().find_hotels(selected_city)
            #app_restorants = Restorants().find_restorants(selected_city)
            time.sleep(5)

        st.success(
            'Done! Please Check Above For Your Travel Recommendations :')
        TravelOptions.kill_spark_process()
        #st.balloons()

    def send_to_user_email(self, user_email):

        email_sender = 'tsafrir.aloni@gmail.com'
        email_password = 'gwxovlcinbboegbu'
        email_receiver = user_email

        subject = 'Check out your travel recommendations'
        #flights_list = ' '.join(map(str, df_list_airports))

        if df_airports_for_email.empty:
            flights_output = 'no flights available'
        else:
            flights_output = build_table(df_airports_for_email, 'blue_light')

        #if df_hotels_for_email.empty:
        #    hotels_output = 'no hotels available'
        #else:
        #    hotels_output = build_table(df_hotels_for_email, 'blue_light')

        ##print('hotels_output :',hotels_output)

        #if df_restorants_for_email.empty:
            #restorants_output = 'no restorants available'

        #else:
            #restorants_output = build_table(df_restorants_for_email, 'blue_light')

        body = "\n\n\n Your Recommended Travel Information Are - \n\n\n" + "\n\n\n FLIGHTS \n\n\n " + flights_output #+ "\n\n HOTELS \n\n " + hotels_output
        body_content = body
        message = MIMEMultipart()
        message.attach(MIMEText(body_content, "html"))
        message["Subject"] = 'Check out your travel recommendations '
        msg_body = message.as_string()

        em = EmailMessage()
        em['From'] = email_sender
        em['To'] = email_receiver
        em['Subject'] = subject
        em.set_content(msg_body)

        context = ssl.create_default_context()

        with smtplib.SMTP_SSL('smtp.gmail.com', 465, context=context) as smtp:
            smtp.login(email_sender, email_password)
            smtp.sendmail(email_sender, email_receiver, msg_body)


class Flights():

    def find_flight(self, selected_budget,selected_departure_date,selected_departure_return_date,user_email):

        global df_list_airports
        global df_airports
        global df_airports_string

        global df_airports_for_email

        ################# insert user request into mongodb ############

        myclient = pymongo.MongoClient("mongodb://localhost:27017/")
        mydb = myclient["travel_app"]
        mycol_User_Requests = mydb["Users_Requests"]

        #x = mycol_User_Requests.delete_many({})

        mycol_User_Requests.insert_one({"Budjet": selected_budget, "Departure_Date": selected_departure_date,
                                        "Departure_Return_Date": selected_departure_return_date,
                                        "User_Email": user_email})

        for x in mycol_User_Requests.find():
            User_Email = x["User_Email"]
        #    Budjet = x["Budjet"]
        #    Departure_Date = x["Departure_Date"]
        #    Departure_Return_Date = x["Departure_Return_Date"]

        #    print(User_Email)

        df_airports_for_email = default_empty_dataframe


        ##################run kafka flights consumer mongo###################


        mogodb_client = pymongo.MongoClient('mongodb://localhost:27017/')
        mydb = mogodb_client["travel_app"]
        mycol = mydb["Flights"]
        #x = mycol.delete_many({})


        cmd = 'python kafka_to_spark_flights_offered_mongo.py'

        p = subprocess.Popen(cmd, shell=True,stdin=None, stdout=None, stderr=None, close_fds=True)

        ##################run kafka hotels consumer mongo###################

        cmd = 'python kafka_to_spark_hotels_offered_mongo.py'

        p = subprocess.Popen(cmd, shell=True, stdin=None, stdout=None, stderr=None, close_fds=True)

        ################## run kafka flights consumer S3 ###################

        cmd = 'python kafka_to_spark_flights_offered_S3.py'

        p = subprocess.Popen(cmd, shell=True, stdin=None, stdout=None, stderr=None, close_fds=True)

        ################## run flights api producer with user parameters ##############################

        try:
            check_mongo_df = check_mongo_results().check_mongo(selected_budget,selected_departure_date,selected_departure_return_date,user_email)
        except:
            check_mongo_df=pd.DataFrame()



        if check_mongo_df.empty:


                api_to_kafka_class().api_to_kafka(selected_budget,selected_departure_date,selected_departure_return_date,user_email)

                sleep(3)

                ################## run hotels api producer with user parameters ##############################

                api_to_kafka_class_hotels().api_to_kafka(selected_budget, selected_departure_date, selected_departure_return_date,user_email)

                sleep(3)

               ############################ find flights in mongodb that are in budjet ####################

                mogodb_client = pymongo.MongoClient('mongodb://localhost:27017/')
                mydb = mogodb_client["travel_app"]
                mycol = mydb["Flights"]

                user_email_param = user_email

                #data = pd.DataFrame(list(mycol.find({user_email: "tsafrir.aloni@gmail.com"})))

                #data = pd.DataFrame(list(mycol.find()))

                data = pd.DataFrame(list(mycol.find({"user_email" : { "$in": [user_email_param] }})))

                data = data.dropna()

                len_data = len(data.index)

                data = data.drop_duplicates()

                data.columns = data.columns.str.replace(' ', '')

                data = data.reindex(data.columns.tolist(), axis=1)

                #data = mycol.find({user_email: "tsafrir.aloni@gmail.com"}, {location_arrival: 1, date_departure: 1,date_departure_return:1,price:1,totalTripDurationInHours:1, _id: 0})

                data = data.applymap(str)

                #data.get("location_arrival", default="no_location_arrival")

                data = data.loc[:, ["location_arrival","price","location_departure","date_departure","date_departure_return"]]


                data = data.loc[
                    data["price"].apply(pd.to_numeric) <= int(selected_budget)].dropna()

                data.price = pd.to_numeric(data.price, errors='coerce')


                data = data.drop_duplicates()

                data= data.sort_values(by=['price'], ascending=True).head(3)

                data = data.head(3)

                print(data)

                minimum_flight_price = data['price'].min()

                def cell_colours(series):
                    red = 'background-color: red;'
                    yellow = 'background-color: yellow;'
                    turquoise = 'background-color: turquoise;'
                    default = ''

                    return [red if data == "failed" else yellow if data == "error" else green if data == "passed"
                    else turquoise for data in series]

                headers = {
                    'selector': 'th.col_heading',
                    'props': 'background-color: #000066; color: white;'
                }


                #st.table(data)

                p = subprocess.Popen(cmd, stdout=subprocess.PIPE, shell=True)
                p.kill()

                ############################ find hotels in mongodb that are in budjet ####################

                mogodb_client = pymongo.MongoClient('mongodb://localhost:27017/')
                mydb = mogodb_client["travel_app"]
                mycol = mydb["Hotels"]

                user_email_param = user_email


                data_hotel = pd.DataFrame(list(mycol.find({"user_email": {"$in": [user_email_param]}})))

                len_data = len(data_hotel.index)

                data_hotel = data_hotel.dropna()

                data_hotel = data_hotel.drop_duplicates()

                data_hotel.columns = data_hotel.columns.str.replace(' ', '')

                data_hotel = data_hotel.reindex(data_hotel.columns.tolist(), axis=1)


                data_hotel = data_hotel.applymap(str)


                data_hotel = data_hotel.loc[:,
                       ["hotel","price","date_departure","date_departure_return"]]



                data_hotel = data_hotel.loc[
                    data_hotel["price"].apply(pd.to_numeric) <= int(selected_budget)].dropna()

                data_hotel.price = pd.to_numeric(data_hotel.price, errors='coerce')


                data_hotel = data_hotel.drop_duplicates()

                data_hotel = data_hotel.sort_values(by=['price'], ascending=True).head(3)

                data_hotel = data_hotel.head(3)

                print(data_hotel)

                minimum_hotel_price = data_hotel['price'].min()

                def cell_colours(series):
                    red = 'background-color: red;'
                    yellow = 'background-color: yellow;'
                    turquoise = 'background-color: turquoise;'
                    default = ''

                    return [red if data == "failed" else yellow if data == "error" else green if data == "passed"
                    else turquoise for data in series]

                headers = {
                    'selector': 'th.col_heading',
                    'props': 'background-color: #000066; color: white;'
                }

                if math.isnan(minimum_flight_price):
                    st.success(
                        'No Flight Package for this budjet')

                elif int(minimum_flight_price) + int(minimum_hotel_price) <= int(selected_budget):
                    st.table(data)
                    st.table(data_hotel)
                else:
                    st.success(
                        'No Flight Package for this budjet')

                p = subprocess.Popen(cmd, stdout=subprocess.PIPE, shell=True)
                p.kill()

        else:
            st.table(check_mongo_df)
            p = subprocess.Popen(cmd, stdout=subprocess.PIPE, shell=True)
            p.kill()

        return User_Email


class Hotels():

    def find_hotels(self, city):

        global df_hotels_for_email
        global df_hotels_string

        df_hotels_for_email = default_empty_dataframe

        if city == 'New York':
            city = "60763"
        elif city == 'bangkok':
            city = "301643"
        elif city == 'Barcelona':
            city = "1465497"

        url = "https://travel-advisor.p.rapidapi.com/airports/search"

        myclient = pymongo.MongoClient("mongodb://localhost:27017/")
        mydb = myclient["travel_app"]
        mycol = mydb["Hotels"]

        x = mycol.delete_many({})

        client = pymongo.MongoClient("mongodb://localhost:27017/")

        url = "https://travel-advisor.p.rapidapi.com/hotels/list"

        querystring = {"location_id": city, "adults": "1", "rooms": "1", "nights": "2", "offset": "0",
                       "currency": "USD", "order": "asc", "limit": "10", "sort": "recommended", "lang": "en_US"}

        headers = {
            "X-RapidAPI-Key": "fe42155845mshc3b183a04c42514p1fc16djsnbb15492b29c4",
            "X-RapidAPI-Host": "travel-advisor.p.rapidapi.com"
        }

        response_hotels = requests.request("GET", url, headers=headers, params=querystring)

        dict_json = json.loads(response_hotels.text)

        try:

            # print(' dict json :',dict_json)

            json_length = len(dict_json)

            # print('hotels_list_length :', json_length)

            for list_json in dict_json.values():

                list_length = len(list_json)

                try:
                    for i in range(0, list_length):
                        try:
                            # print(f"The value for your request is {list_json[i]}")
                            None
                            var_price = random.randint(50, 150)
                            var_flight_number = random.randint(10000, 15000)

                            list_json[i]['price_in_dollars'] = var_price

                            mycol.insert_one(list_json[i])
                        except KeyError:
                            None
                            # print(f"There is no parameter with the '{list_json[i]}' key. ")
                except:
                    None

            df_hotels = pd.DataFrame(list(mycol.find()))

            # df_hotels = df_hotels.reset_index(inplace=True)

            additional_cols = ['price_in_dollars']

            df_hotels = df_hotels.reindex(df_hotels.columns.tolist(), axis=1)

            df_hotels = df_hotels.applymap(str)

            df_hotels = df_hotels.loc[:, ["name", "hotel_class", "price_in_dollars"]]

            df_hotels = df_hotels.loc[df_hotels["price_in_dollars"].apply(pd.to_numeric) <= int(selected_hotel_budget)]

            # df_hotels['hotel_class'] = df_hotels['hotel_class'].astype('int')

            df_hotels.sort_values(by=['hotel_class'], ascending=False).head(3)

            df_hotels = df_hotels.head(3)

            try:
                df_hotels_for_email = df_hotels
            except Exception:
                df_hotels_for_email = 'no hotels found'

            df_hotels_string = df_hotels.to_string()

            def cell_colours(series):
                red = 'background-color: red;'
                yellow = 'background-color: yellow;'
                turquoise = 'background-color: turquoise;'
                default = ''

                return [red if data == "failed" else yellow if data == "error" else green if data == "passed"
                else turquoise for data in series]

            headers = {
                'selector': 'th.col_heading',
                'props': 'background-color: #000066; color: white;'
            }
            df_hotels = df_hotels.style.set_table_styles([headers]) \
                .apply(cell_colours)

            st.table(df_hotels)

        except Exception:
            df_hotels_string = 'Hotels API didn`t return results'
            st.write('Hotels API didn`t return results')


class Restorants():

    def find_restorants(self, city):

        global df_restorants_for_email
        global df_restorants_string

        df_restorants_for_email = default_empty_dataframe

        if city == 'New York':
            city = "60763"
        elif city == 'bangkok':
            city = "301643"
        elif city == 'Barcelona':
            city = "1465497"

        myclient = pymongo.MongoClient("mongodb://localhost:27017/")
        mydb = myclient["travel_app"]
        mycol = mydb["Restorants"]

        x = mycol.delete_many({})

        client = pymongo.MongoClient("mongodb://localhost:27017/")

        url = "https://travel-advisor.p.rapidapi.com/restaurants/list"

        querystring = {"location_id": city, "restaurant_tagcategory": "10591",
                       "open_now": "false", "lang": "en_US"}

        headers = {
            "X-RapidAPI-Key": "fe42155845mshc3b183a04c42514p1fc16djsnbb15492b29c4",
            "X-RapidAPI-Host": "travel-advisor.p.rapidapi.com"
        }

        response_restorants = requests.request("GET", url, headers=headers, params=querystring)

        dict_json = json.loads(response_restorants.text)

        try:

            for list_json in dict_json.values():
                # print(list_json)
                list_length = len(list_json)

                # print(list_length)
                try:
                    for i in range(0, list_length):
                        try:
                            # print(f"The value for your request is {list_json[i]}")
                            None
                            var_price = random.randint(50, 150)
                            var_flight_number = random.randint(10000, 15000)
                            list_json[i]['price_in_dollars'] = var_price

                            mycol.insert_one(list_json[i])
                        except KeyError:
                            None
                            # print(f"There is no parameter with the '{list_json[i]}' key. ")
                except:
                    None

            df_restorants = pd.DataFrame(list(mycol.find()))

            df_restorants = df_restorants.applymap(str)

            df_restorants = df_restorants.loc[:, ["name", "rating", "price_in_dollars"]]

            df_restorants = df_restorants.loc[
                df_restorants["price_in_dollars"].apply(pd.to_numeric) <= int(selected_restorants_budget)]

            # df_restorants['rating'] = df_restorants['rating'].astype('int')

            df_restorants.sort_values(by=['rating'], ascending=False).head(3)

            df_restorants = df_restorants.head(3)

            try:
                df_restorants_for_email = df_restorants
            except Exception:
                df_restorants_for_email = 'no restorants found'

            df_restorants_string = df_restorants.to_string()

            # print(df_restorants)

            def cell_colours(series):
                red = 'background-color: red;'
                yellow = 'background-color: yellow;'
                turquoise = 'background-color: turquoise;'
                default = ''

                return [red if data == "failed" else yellow if data == "error" else green if data == "passed"
                else turquoise for data in series]

            headers = {
                'selector': 'th.col_heading',
                'props': 'background-color: #000066; color: white;'
            }
            df_restorants = df_restorants.style.set_table_styles([headers]) \
                .apply(cell_colours)

            st.table(df_restorants)

            df = pd.DataFrame(
                np.random.randn(1000, 2) / [50, 50] + [40.69, -74],
                columns=['lat', 'lon'])

            st.map(df)

        except Exception:
            df_restorants_string = 'Restorants API didn`t return Results'
            #TravelOptions().send_to_user_email(user_email)
            #st.write('Restorants API didn`t return Results')
