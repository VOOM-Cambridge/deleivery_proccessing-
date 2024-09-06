from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
import math
import time
from datetime import datetime, timezone, timedelta
import json
import paho.mqtt.client as mqtt
import tomli, os
import pytz

class SupplyChainTracker:

    def __init__(self, config_file):
        self.config = self.load_config(config_file)
        self.clientIn = InfluxDBClient(
            url=self.config["influxdb"]["url"], 
            token=self.config["influxdb"]["token"], 
            org=self.config["influxdb"]["org"]
        )
        self.mqtt_address = self.config["constants"]["mqtt_address"]
        self.name = self.config["constants"]["current_name"]
        self.order = self.config["constants"]["order"]
        self.trolleyList = self.config["constants"]["trolleyList"]

        self.locations = {"Robot_Lab" :[52.209222464816634, 0.08702698588458352],
                         "3D_Printing":[52.20963378973377, 0.0876479255503501],
                         "Design_Studio":[52.20925064994319, 0.08727564922295765],
                         "Manual_Assembly":[52.20925064994319, 0.08727564922295765],
                         "Supplier":[52.209504315277606, 0.08767811011743598]}

        self.journey_time = {"Robot_Lab" : {"3D_Printing": 200, "Design_Studio": 100, "Manual_Assembly": 200, "Supplier": 180},
                         "3D_Printing": {"Robot_Lab": 200, "Design_Studio": 200, "Manual_Assembly": 200, "Supplier": 200},
                         "Design Studio": {"3D_Printing": 200, "Robot_Lab": 100, "Manual_Assembly": 50, "Supplier": 180},
                         "Manual Assembly": {"Design_Studio": 50,"3D_Printing": 200, "Robot_Lab": 100, "Supplier": 180} ,
                         "Supplier":{"Design_Studio": 180,"3D_Printing": 200, "Robot_Lab": 180, "Manual_Assembly": 180} }
        self.locationDict = {"loc4": "Robot_Lab",
                            "loc2": "3D_Printing", 
                            "loc5": "Design_Studio",
                            "loc3": "Manual_Assembly", 
                            "loc1": "Supplier"}
    def load_config(self, file_name):
        file_path = os.path.join(os.path.dirname(__file__), file_name)
        with open(file_path, 'rb') as config_file:
            config = tomli.load(config_file)
        return config

    def load_data_from_influxdb(self, timeBack):
        query = '''
        from(bucket: "supplychain_data")
        |> range(start: -''' + str(timeBack) + '''s)
        |> filter(fn: (r) => r["_measurement"] == "coordinate")
        |> filter(fn: (r) => r["_field"] == "latitude" or r["_field"] == "longitude" or r["_field"] == "name" or r["_field"] == "vehicle")
        |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
        |> map(fn: (r) => ({ r with _value: r.latitude}))
        |> group()
        |>last()   
        '''
        query_api = self.clientIn.query_api()
        result = query_api.query(query)
        out = result.to_values(columns=['_time', 'latitude', 'longitude', 'vehicle', 'start', 'destination'])[0]
        #print(out)
        if out != None and out != []:
            new = {'_time':out[0], 'latitude':out[1], 'longitude':out[2], 'vehicle':out[3], 'start':out[4], 'destination':out[5]}
        else:
            new = None
        return new


    @staticmethod
    def haversine(coord1, coord2):
        R = 6371000
        lat1, lon1 = math.radians(coord1[0]), math.radians(coord1[1])
        lat2, lon2 = math.radians(coord2[0]), math.radians(coord2[1])
        dlat = lat2 - lat1
        dlon = lon2 - lon1
        a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
        distance = R * c
        return distance

    def findJourneyTime(self, startLoc, endLoc):
        return self.journey_time[startLoc][endLoc]

    def sendMess(self, remaining_time, start, destination, percent_comp, orderIn, arrival, vehicle):
        now = datetime.now(timezone.utc)
        delayed_time = now + timedelta(seconds=remaining_time)
        formatted_timestamp = delayed_time.isoformat()
        timestamp_str = delayed_time.strftime("%Y-%m-%dT%H:%M:%S")

        client = mqtt.Client()

        mess_delay = {
            "remaining_time": remaining_time, 
            "supplier": start,
            "customer": destination,
            "percentage": percent_comp,
            "timestamp": formatted_timestamp,
            "vehicle": vehicle,
            "order": orderIn
        }
        if not arrival: # messeage about order arrival or at location
            mess_MES = {
                "name": orderIn,
                "customer": destination,
                "due": timestamp_str,
            }
            topicMES = f"MES/purchase/{self.name}/update/"
        else:
            mess_MES = {
                "name": orderIn,
                "customer": destination,
                "due": timestamp_str,
                "status": "completed"
            }
            topicMES = f"MES/purchase/{self.name}/update/"

        json_message_delay = json.dumps(mess_delay)
        json_message_mes = json.dumps(mess_MES)
        
        topicD = "Tracking/delivery/delays/"

        try:
            client.connect(self.mqtt_address, 1883, 60)
            time.sleep(0.1)
            client.publish(topicD, json_message_delay, qos=1)
            time.sleep(0.1)
            client.publish(topicMES, json_message_mes, qos=1)
            time.sleep(0.1)
            print("Message sent!" + orderIn)
        except:
            print("Connection failed to broker")

        client.disconnect()

    def checkOrderOnTrolley(self, trolly, timeback):
        query = f'''from(bucket: "tracking_data_comp")
                    |> range(start: -30d )
                    |> filter(fn: (r) => r["_measurement"] == "Tracking_comp")
                    |> filter(fn: (r) => r["_field"] == "child")
                    |>filter(fn: (r) => r["parent"] == "{trolly}")
                    |> group()
                    |> unique()'''
        query_api = self.clientIn.query_api()
        result = query_api.query(query)
        output = result.to_values(columns=['_value'])
        print(output)

        if output == [] or output == None:
            query = f'''from(bucket: "tracking_data_comp")
                        |> range(start: -30d )
                        |> filter(fn: (r) => r["_measurement"] == "Tracking_comp")
                        |> filter(fn: (r) => r["_field"] == "parent")
                        |> filter(fn: (r) => r["child"] == "{self.order}")
                        |> group()
                        |> unique()'''
            table = query_api.query(query)

            data = []
            output = table.to_values(columns=['_value'])
            out = [x[0] for x in output]
        else:
            out = [x[0] for x in output]
        return out


    def findStartOrder(self, order):
        query = '''from(bucket: "supplychain_data")
                    |> range(start: -2d)
                    |> filter(fn: (r) => r["_measurement"] == "delviery")
                    |> filter(fn: (r) => r["order"] == "''' + order + '''")
                    |>group()
                    |>last()'''
        query_api = self.clientIn.query_api()
        result = query_api.query(query)
        output = result.to_values(columns=["customer", "supplier"])
        return output

    def findTrolleyActive(self, trolly, timeback):
        query = '''from(bucket: "supplychain_data")
                    |> range(start: ''' + timeback + ''')
                    |> filter(fn: (r) => r["_measurement"] == "arrival")
                    |> filter(fn: (r) => r["_value"] == "''' + trolly + '''")
                    |>group()
                    |> map(fn: (r) => ({ r with _value: r.state }))
                    |> last()'''
        
        query_api = self.clientIn.query_api()
        result = query_api.query(query)
        output = result.to_values(columns=['_time','_value', 'location'])
        if output == None or output == [] or output == None:
            return None, output
        elif output[0][1] == "out":
            return output[0][1] , output
        else:
            return output[0][1] , output
        #return not bool(output)

    def run(self):
        timeStart = datetime.now()
        sent = 0
        while True: 
            timeNow = datetime.now()
            if (timeNow - timeStart).total_seconds() > 10:
                print("checking")
                timeStart = datetime.now()
                for trolly in self.trolleyList:
                    activeState, output = self.findTrolleyActive(trolly, "-1d")
                    print(trolly)
                    print(activeState)
                    print(output)
                    if output != None and output != [] and activeState != None and activeState != []:
                        timeStarRun = output[0][0] #.astimezone(london_timezone)
                        locationLeft = output[0][2]
                        london_timezone = pytz.timezone('Europe/London')
                        current_utc_time = datetime.now(london_timezone)
                        time_difference = current_utc_time - timeStarRun
                        seconds_difference = round(time_difference.total_seconds())
                        orders = self.checkOrderOnTrolley(trolly, (seconds_difference + 7200))
                        #print("orders:  ")
                        #print(orders)
                        if activeState == "out": # trolley is active and moving between locations
                            print("trolly is active: ", trolly)
                            data = self.load_data_from_influxdb(seconds_difference)
                            #print(data)
                            sent = 0
                            if data != None:
                                coords = [data["latitude"], data["longitude"]]
                                start = str(locationLeft) #data["start"]
                                destination = str(data["destination"])
                                #location = self.findTrollyLocation(trolly, self.clientIn.query_api())
                                dissToStart = self.haversine(self.locations[start], coords)
                                dissToEnd = self.haversine(self.locations[destination], coords)
                                percent_left = dissToEnd/(dissToEnd+dissToStart)
                                journey_time = self.findJourneyTime(start,destination)
                                remaining_time = journey_time*percent_left
                                percent_comp = 1- percent_left
                                # find remaining time in journey
                                for ord in orders:
                                    # send messeages to localhost for MES and tracking
                                    self.sendMess(remaining_time, start, destination, percent_comp, ord, False, trolly)
                    elif activeState == "in": # trolley is at a fixed location point start 
                        print("trolley at: " + locationLeft)
                        destination = str(locationLeft)
                        for ord in orders:
                            start = self.findStartOrder(ord)
                            if start == None or start == []:
                                # send messeages to localhost for MES and tracking
                                if sent < 5:
                                    self.sendMess(0, "", destination, 1, ord, True, trolly)
                                    sent = sent + 1
                            else:
                                # send messeages to localhost for MES and tracking
                                if start[0][1] == "null" or start[0][1] == None:
                                    star = ""
                                else:
                                    star = start[0][1]
                                if sent < 5:
                                    self.sendMess(0, star, destination, 1, ord, True, trolly)
                                    sent = sent + 1
                    else:
                        print("no data for trolley")



                 
                else:
                    print("no trolley " + trolly + " active")
                    

if __name__ == "__main__":
    supplyChain = SupplyChainTracker("/app/config/config.toml")
    supplyChain.run()

