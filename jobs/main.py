import os
from quopri import encode
import random 
import uuid
from confluent_kafka import SerializingProducer
import simplejson as json
from datetime import datetime,timedelta
import time
LONDON_COORDINATES={
    "latitude":51.5074,
    "longitude":-0.1278
}

BIRMINGHAM_COORDINATES={
    "latitude":52.4862,
    "longitude":-1.8904
}

LATITUDE_INCREMENT = (
    BIRMINGHAM_COORDINATES["latitude"]
    - LONDON_COORDINATES["latitude"]
) / 100

LONGITUDE_INCREMENT = (
    BIRMINGHAM_COORDINATES["longitude"]
    - LONDON_COORDINATES["longitude"]
) / 100

KAFKA_BOOTSTRAP_SERVERS=os.getenv("KAFKA_BOOTSTRAP_SERVERS","localhost:9092")
VEHICLE_TOPIC=os.getenv("VEHICLE_TOPIC","VEHICLE_TOPIC")
GPS_TOPIC=os.getenv("GPS_TOPIC","GPS_TOPIC")
TRAFFIC_TOPIC=os.getenv("TRAFFIC_TOPIC","TRAFFIC_TOPIC")
WEATHER_TOPIC=os.getenv("WEATHER_TOPIC","WEATHER_TOPIC")
EMERGENCY_TOPIC=os.getenv("EMERGENCY_TOPIC","EMERGENCY_TOPIC")
random.seed(42)
start_time=datetime.now()
start_location=LONDON_COORDINATES.copy()

def get_next_time():
    global start_time
    start_time+=timedelta(seconds=random.randint(30,60))
    return start_time

def simulate_vehicle_movement():
    global start_location
    start_location["latitude"]+=LATITUDE_INCREMENT
    start_location["longitude"]+=LONGITUDE_INCREMENT

    start_location["latitude"]+=random.uniform(-0.0005,0.0005)      
    start_location["longitude"]+=random.uniform(-0.0005,0.0005)

    return start_location


def generate_vehicle_data(device_id):
    location=simulate_vehicle_movement()
    return {
        "id":uuid.uuid4(),
        "device_id":device_id,
        "timestamp":get_next_time().isoformat(),
        "location":(location["latitude"],location["longitude"]),
        "speed":random.uniform(10,40),
        "direction":"north-east",
        "make":"BMW",
        "model":"X5",
        "year":2020,
        "fuelType":"Hybrid" ,
        "Snapshot":"base64encodedstring"
    }



def generate_gps_data(device_id,timestamp,vehicle_type="private"):
    return{
        "id":uuid.uuid4(),
        "device_id":device_id,
        "timestamp":timestamp,
        "speed":random.uniform(0,40),
        "direction":"north-east",
        "vehicle_type":vehicle_type
    }

def generate_traffic_camera_data(device_id,timestamp,camera_id):
      return{
          "id":uuid.uuid4(),
          "device_id":device_id,
          "camera_id":camera_id,
          "timestamp":timestamp,
          "snapshot":"base64encodedstring"
      }

def generate_weather_data(device_id,timestamp,location):
    return{
        "id":uuid.uuid4(),
        "device_id":device_id,
        "location":location,
        "temperature":random.uniform(-5,26),
        "weather_condition":random.choice(["sunny","cloudy","rainy","snowy"]),
        "timestamp":timestamp,
        "precipitation":random.uniform(0,25),
        "wind_speed":random.uniform(0,100),
        "humidity":random.uniform(0,100),
        "air_quality_index":random.uniform(0,500)
    }

def generate_emergency_incident_data(device_id,timestamp,location):
    return{
        "id":uuid.uuid4(),
        "device_id":device_id,
        "timestamp":timestamp,
        "location":location,
        "type":random.choice(["accident","fire","medical","poice","None"]),
        "incident_id":uuid.uuid4(),
        "status":random.choice(["active","resolved"]),
        "description":"A description of the incident"
    }

def json_serializer(obj):
    if isinstance(obj,uuid.UUID):
        return str(obj)
    raise TypeError("This is not serializable")

def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failer : {err}")
    else: 
        print(f"message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

def produce_data_to_kafka(producer,topic,data):
    producer.produce(topic=topic,value=json.dumps(data,default=json_serializer).encode("utf-8"),
                                            on_delivery=delivery_report)
    producer.flush()
def simulate_journey(producer,device_id):
    location=simulate_vehicle_movement()

    while True:
        vehicle_data=generate_vehicle_data(device_id)
        gps_data=generate_gps_data(device_id,vehicle_data["timestamp"])
        traffic_data=generate_traffic_camera_data(device_id,vehicle_data["timestamp"],"canon a-series")
        weather_data=generate_weather_data(device_id,vehicle_data["timestamp"],vehicle_data["location"])
        emergency_incident_data=generate_emergency_incident_data(device_id,vehicle_data["timestamp"],vehicle_data["location"] ) 
        if(vehicle_data["location"][0]>=BIRMINGHAM_COORDINATES["latitude"] and vehicle_data["location"][1]<=BIRMINGHAM_COORDINATES["longitude"]):
            print("Vehicle has reached Birmingham, ending simulation.")
            break
        produce_data_to_kafka(producer,VEHICLE_TOPIC,vehicle_data)
        produce_data_to_kafka(producer,GPS_TOPIC,gps_data)
        produce_data_to_kafka(producer,TRAFFIC_TOPIC,traffic_data)
        produce_data_to_kafka(producer,WEATHER_TOPIC,weather_data)
        produce_data_to_kafka(producer,EMERGENCY_TOPIC,emergency_incident_data)
        time.sleep(5)


if __name__ =="__main__":
    producer_config={
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'error_cb':lambda err: print(f"Kafka error: {err}"),
    }

    producer=SerializingProducer(producer_config)

    try:
        simulate_journey(producer,"i20")

    except KeyboardInterrupt:
        print("Simulation ended by the user")
    
    except Exception as e:
        print(f"Error during simulation: {e}")