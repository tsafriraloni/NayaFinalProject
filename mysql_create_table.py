import mysql.connector as mc

#===========================connector to mysql====================================#
mysql_conn = mc.connect(
    user='naya',
    password='NayaPass1!',
    host='localhost',
    port=3306,
    autocommit=True,
    database="MyFlightdb"  )

#=======================# Creating the events table==============================#
mysql_cursor= mysql_conn.cursor()
mysql_create = '''create table if not exists flights (
                            location_arrival varchar(100),
                            date_departure date,
                            location_departure varchar(100),
                            date_departure_return date,
                            price double,
                            currencyCode varchar(100),
                            carrier varchar(100),
                            totalTripDurationInHours varchar(100),
                            FlightStartYear integer,
                            hourd integer,
                            minuted integer,
                            secondd integer,
                            dayofweek integer,
                            week_day_full varchar(100) )'''

mysql_cursor.execute(mysql_create)
mysql_cursor.close()




