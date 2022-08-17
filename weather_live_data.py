from kafka import KafkaProducer
from datetime import datetime
import time
from json import dumps
import requests

TOPIC_NAME_CONS = "topic-weather"
BOOTSTRAP_SERVERS_CONS = 'localhost:9092'

def get_data(url):

    response_data = requests.get(url=url)
    data = response_data.json()
    weather = {}
    event_datetime = datetime.now()
    weather["event_datetime"] 		= event_datetime.strftime("%Y-%m-%d %H:%M")
    weather["humidity"] 			= data["main"]["humidity"]
    weather["pressure"] 			= data["main"]["pressure"]
    weather["temperature"] 			= data["main"]["temp"]
    weather["temperature_max"] 		= data["main"]["temp_max"]
    weather["temperature_min"] 		= data["main"]["temp_min"]
    weather["temperature_feels"] 	= data["main"]["feels_like"]
    weather["city_name"] 			= data["name"]
    weather["wind_deg"] 			= data["wind"]["deg"]
    weather["wind_speed"] 			= data["wind"]["speed"]

    return weather


if __name__ == "__main__":
    print("Open Weather API Data | Kafka Producer Application Started ... ")

    kafka_producer_obj = KafkaProducer(bootstrap_servers=BOOTSTRAP_SERVERS_CONS,
                             value_serializer=lambda x: dumps(x).encode('utf-8'))

    city_names = ["Hamburg","Berlin","Frankfurt am Main","Stuttgart","Munich","Düsseldorf","Köln","Dresden","Leipzig"]
    
    i = 0
    while True:
        try:

            for city in city_names:
                api_key = "API KEY"
                url = "https://api.openweathermap.org/data/2.5/weather?q={0}&appid={1}".format(city,api_key)
                weather_data = get_data(url)
                i = i + 1
                print("<<<<<<<<<<  Live Straeming Data >>>>>>>>>>")
                print("Printing message id: " + str(i))
                print(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
                print("Sending message to Kafka topic: " + TOPIC_NAME_CONS)
                print("Message to be sent: ", weather_data)
                kafka_producer_obj.send(TOPIC_NAME_CONS, weather_data)

        except Exception as ex:
            print("Event Message Construction Failed. ")
            print(ex)

        time.sleep(10)

        


