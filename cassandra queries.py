#CONNECTIONS
from collections import Counter
import heapq
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
cloud_config= {
  'secure_connect_bundle': 'secure-connect-assignment.zip'
}
auth_provider = PlainTextAuthProvider('', '')
cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
session = cluster.connect()
row = session.execute("select release_version from system.local").one()
if row:
  print(row[0])
else:
  print("An error occurred.")
  session.set_keyspace('assignmentkey') 
row = cluster.metadata.keyspaces['assignmentkey'] 

#DATA CLEANING WITH PANDAS/NUMPY
import pandas as pd
import numpy as np

trip = pd.read_csv('taxi_trip_data.csv',encoding='ISO-8859-1')

zone = pd.read_csv('taxi_zone_geo.csv',encoding='ISO-8859-1')

trip.drop(['store_and_fwd_flag','rate_code','total_amount'], inplace=True, axis=1)

trip = trip.iloc[:15000]

trip[['pickup_date','pickup_time']] = trip.pickup_datetime.str.split(" ", expand = True)

trip[['dropoff_date','dropoff_time']] = trip.dropoff_datetime.str.split(" ", expand = True)

trip=trip.replace({'payment_type' : { 1:'credit card', 2:'cash', 3:'no charge', 4:'dispute',5:'unknown',6:'voided trip'}})

trip.drop(['vendor_id'], inplace=True, axis=1)

merged=trip.merge(zone, left_on='pickup_location_id', right_on='zone_id')

merged.drop(['pickup_datetime','dropoff_datetime','borough','zone_geom'], inplace=True, axis=1)

conditions = [
    (merged['dropoff_time'] >= '5:00:00') & (merged['dropoff_time'] < '12:00:00'),
 (merged['dropoff_time'] >= '12:00:00') & (merged['dropoff_time'] < '17:00:00'),    
 (merged['dropoff_time'] >= '17:00:00') & (merged['dropoff_time'] < '21:00:00'),
 ]
results = ['morning', 'afternoon', 'evening']
merged['dropoff_day_time'] = np.select(conditions, results)
          
merged['payment_type'] = merged['payment_type'].astype('str')
merged=merged.iloc[:10000]
merged['pickup_time'] = pd.to_datetime(merged['pickup_time'], errors='coerce') 
merged['dropoff_time'] = pd.to_datetime(merged['dropoff_time'], errors='coerce')
merged['duration'] = (merged['dropoff_time'] - merged['pickup_time']).dt.total_seconds() / 60
merged['total_cost']=merged['fare_amount']+merged['extra']+merged['mta_tax']+merged['tip_amount']+merged['tolls_amount']+merged['imp_surcharge']
merged['duration'] = merged['duration'].astype(int)

#created another index as it only inserted 260 records with the primary key i created before( pickup and dropoff id together)
merged=merged.reset_index()
merged=merged.rename(columns={"index":"new_id"})
merged["new_id"]=merged.index+90

#TABLE CREATIION IN CASSANDRA

session.execute("""
    create table if not exists assignmentkey.triptaxi (
         new_id int,
        passenger_count int,
        trip_distance float,
        payment_type text,
        fare_amount float,
        extra float,
        mta_tax float,
        tip_amount float,
        tolls_amount float,
        imp_surcharge float,
        pickup_location_id int,
        dropoff_location_id int,
        pickup_date timestamp,
        pickup_time timestamp,
        dropoff_date timestamp,
        dropoff_time timestamp,
        zone_id int,
        zone_name text,
       dropoff_day_time text,
       total_cost float,
       duration int,
       Primary key (new_id)
);
""")
i,j = merged.shape
for x in range(0, i):
  session.execute("""insert into assignmentkey.triptaxi(new_id,passenger_count,trip_distance,payment_type,fare_amount,extra,mta_tax,tip_amount,tolls_amount,imp_surcharge,pickup_location_id,dropoff_location_id,pickup_date,pickup_time,dropoff_date,dropoff_time,zone_id,zone_name,dropoff_day_time,duration,total_cost) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);""",(
   int(merged.loc[x].new_id),int(merged.loc[x].passenger_count), float(merged.loc[x].trip_distance),str(merged.loc[x].payment_type), float(merged.loc[x].fare_amount), float(merged.loc[x].extra), float(merged.loc[x].mta_tax), float(merged.loc[x].tip_amount), float(merged.loc[x].tolls_amount), float(merged.loc[x].imp_surcharge), int(merged.loc[x].pickup_location_id), int(merged.loc[x].dropoff_location_id), str(merged.loc[x].pickup_date), str(merged.loc[x].pickup_time), str(merged.loc[x].dropoff_date), str(merged.loc[x].dropoff_time), int(merged.loc[x].zone_id), str(merged.loc[x].zone_name),str(merged.loc[x].dropoff_day_time),int(merged.loc[x].duration),float(merged.loc[x].total_cost)))
  print(merged.loc[x].new_id, "inserted")


#average tips for each passenger count
passenger_count=merged.passenger_count.unique()
for i in range(len(passenger_count)):
 rows=session.execute(f"select Avg(tip_amount) as mycount From assignmentkey.triptaxi where passenger_count= {passenger_count[i]} ALLOW FILTERING;")
 for row in rows:
    print( "passenger count",passenger_count[i],"average tip amount",row.mycount)


  

#most common payment types for each time of day note: 0 means midnight but i forgot to rename it:)
payment_type=merged.payment_type.unique()
dropoff_day_time=merged.dropoff_day_time.unique()
evening=[]
midnight=[]
afternoon=[]
for j in range(len(dropoff_day_time)):
  for k in range(len(payment_type)):
   row=session.execute(f"select count(payment_type) as mycount from assignmentkey.triptaxi WHERE payment_type= '{payment_type[k]}' AND dropoff_day_time= '{dropoff_day_time[j]}' ALLOW FILTERING;")
 
   for roww in row:
   #print("count for ",payment_type[k],"in",dropoff_day_time[j],roww.mycount)
      if(dropoff_day_time[j]=='evening'):
       evening.append([payment_type[k],roww.mycount])
      elif(dropoff_day_time[j]=='0'):
       midnight.append([payment_type[k],roww.mycount])
      elif(dropoff_day_time[j]=='afternoon'):
       afternoon.append([payment_type[k],roww.mycount])
print( "evening",evening) 
print("midnight",midnight) 
print("afternoon",afternoon)
#the three times of the day have credit card as their most used payment type


#top 5 loations
pickup_location_id=merged.pickup_location_id.unique()
locations=[]
for x in range(len(pickup_location_id)):
  row2=session.execute(f"select count(pickup_location_id) as mycount from assignmentkey.triptaxi where pickup_location_id={pickup_location_id[x]}  ALLOW FILTERING;")
  for k in row2:
   #print( pickup_location_id[x], "has this amount of pickup requests",k.mycount)
   locations.append([pickup_location_id[x],k.mycount])
sorted_list = sorted(
    locations,
    key=lambda t: t[1],
    reverse=True
)   
sorted2=[]
for i in range(len(sorted_list)):
  sorted2.append(sorted_list[i][1])
print(*sorted_list,sep=",")  
print(heapq.nlargest(5, sorted2))
#the best 5 locations for pickups that are highly requested by customers are  location ids 237,230,236,186,48






 



  
 











      


   











        

              




             


  



