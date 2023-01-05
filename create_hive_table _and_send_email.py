from pyhive import hive
#import pyarrow as pa
import pandas as pd
from pretty_html_table import build_table
import ssl
import smtplib
import string
from smtplib import SMTP
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.message import EmailMessage
import ssl

hive_cnx = hive.Connection(host = 'cnt7-naya-cdh63',
                           port = 10000,
                           username = 'hdfs',
                           password = 'naya',
                           auth = 'CUSTOM')

# Creating the database
cursor = hive_cnx.cursor()
cursor.execute('''CREATE DATABASE IF NOT EXISTS flightdb''')
cursor.close()

# #==============================================================================================================================================#
cursor = hive_cnx.cursor()
cursor.execute('''DROP TABLE IF EXISTS flightdb.myflight''')
cursor.close()

#Creating the table============================================================================================================================#
cursor = hive_cnx.cursor()
cursor.execute('''CREATE TABLE flightdb.myflight (
location_arrival string,
date_departure string,
location_departure string,
date_departure_return string,
price string,
currencyCode string,
carrier String,
totalTripDurationInHours String,
FlightStartDay int,
FlightStartMonth int,
FlightStartYear int,
hourd int,
minuted int,
secondd int,
dayofweek int,
week_day_full String
)
stored as parquet'''
               )
cursor.close()


#LOAD the DATA INPATH====================================
cursor1 = hive_cnx.cursor()
cursor1.execute('''LOAD DATA INPATH 'hdfs://cnt7-naya-cdh63:8020//user/Flight_Project/Data/'
                          INTO TABLE flightdb.myflight''')

cursor1.close()



#We can see the actual data by querying the hive table
cursor = hive_cnx.cursor()
cursor.execute('select * from flightdb.myflight limit 10')
data = cursor.fetchall()
cursor.close()
print(len(data))

email_df = pd.read_sql("select location_arrival,count(*) as cnt from flightdb.myflight group by location_arrival order by cnt desc limit 3", hive_cnx)



print(email_df)

global user_email

user_email = 'tsafrir.aloni@gmail.com'

def send_to_user_email():
    email_sender = 'tsafrir.aloni@gmail.com'
    email_password = 'gwxovlcinbboegbu'
    email_receiver = user_email

    subject = 'Check out your flight stats  '
    #flights_list = ' '.join(map(str, df_list_airports))

    if email_df.empty:
        flights_output = 'no flights available'
    else:
        flights_output = build_table(email_df, 'blue_light')

    body = "\n\n\n Your Flights Stats Information Are - \n\n\n" + "\n\n\n FLIGHTS \n\n\n " + flights_output
    body_content = body
    message = MIMEMultipart()
    message.attach(MIMEText(body_content, "html"))
    message["Subject"] = 'Check out your flight stats  '
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

send_to_user_email()



