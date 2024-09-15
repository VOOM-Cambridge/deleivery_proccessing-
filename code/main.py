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
        self.london_timezone = pytz.timezone("Europe/London")
        self.lastMess = {}
        self.lastMessSent ={}

        self.locations = {"Robot_Lab" :[52.209222464816634, 0.08702698588458352],
                          "Robot_lab" :[52.209222464816634, 0.08702698588458352],
                         "3D_Printing":[52.20963378973377, 0.0876479255503501],
                         "Design_Studio":[52.20925064994319, 0.08727564922295765],
                         "Manual_Assembly":[52.20925064994319, 0.08727564922295765],
                         "Supplier":[52.209504315277606, 0.08767811011743598]}

        self.journey_time = {"Robot_Lab" : {"3D_Printing": 200, "Design_Studio": 100, "Manual_Assembly": 200, "Supplier": 180, "Robot_Lab": 0},
                             "Robot_lab" : {"3D_Printing": 200, "Design_Studio": 100, "Manual_Assembly": 200, "Supplier": 180, "Robot_Lab": 0},
                         "3D_Printing": {"Robot_Lab": 200, "Design_Studio": 200, "Manual_Assembly": 200, "Supplier": 200, "3D_Printing":0},
                         "Design_Studio": {"3D_Printing": 200, "Robot_Lab": 100, "Manual_Assembly": 50, "Supplier": 180, "Design_Studio": 0},
                         "Manual_Assembly": {"Design_Studio": 50,"3D_Printing": 200, "Robot_Lab": 100, "Supplier": 180, "Manual_Assembly": 0} ,
                         "Supplier":{"Design_Studio": 180,"3D_Printing": 200, "Robot_Lab": 180, "Manual_Assembly": 180, "Supplier": 0} }
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
        |>sort(columns: ["_time"])
        |>last()   
        '''
        query_api = self.clientIn.query_api()
        result = query_api.query(query)
        try:
            out = result.to_values(columns=['_time', 'latitude', 'longitude', 'vehicle', 'start', 'destination'])[0]
        except:
            out = None
        #print(out)
        if out != None and out != []:
            try:
                new = {'_time':out[0], 'latitude':out[1], 'longitude':out[2], 'vehicle':out[3], 'start':out[4], 'destination':out[5]}
            except:
                None
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

        percent_comp = round( percent_comp, 3)
        remaining_time = round(remaining_time)
        try:
            client = mqtt.Client()

            mess_check = {"remaining_time": remaining_time, 
                "supplier": start, "customer": destination,
                "percentage": percent_comp, "vehicle": vehicle}
            
            if orderIn not in self.lastMess:
                self.lastMess[orderIn] = mess_check
                self.lastMessSent[orderIn] = 0

            if self.lastMess[orderIn] == mess_check:
                self.lastMessSent[orderIn] = self.lastMessSent[orderIn] + 1
            else:
                self.lastMess[orderIn] = mess_check
                self.lastMessSent[orderIn] = 0

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
                #topicMES = f"MES/purchase/{self.name}/update/"
            else:
                mess_MES = {
                    "name": orderIn,
                    "customer": destination,
                    "due": timestamp_str,
                    "status": "completed"
                }
                topicMESLocal = f"MES/purchase/{self.name}/update/"
                topicMESCustomer = destination+"/MES/purchase/"+ self.name + "/update/"

            json_message_delay = json.dumps(mess_delay)
            json_message_mes = json.dumps(mess_MES)
            
            topicLocal = "Tracking/delivery/delays/"
            topicCustomer = destination+"/Tracking/delivery/delays/"

            if self.lastMessSent[orderIn] < 5:
                try:
                    client.connect(self.mqtt_address, 1883, 60)
                    time.sleep(0.1)
                    client.publish(topicLocal, json_message_delay, qos=1)
                    time.sleep(0.1)
                    client.publish(topicCustomer, json_message_delay, qos=1)
                    time.sleep(0.1)
                    client.publish(topicMESLocal, json_message_mes, qos=1)
                    time.sleep(0.1)
                    client.publish(topicMESCustomer, json_message_mes, qos=1)
                    time.sleep(0.1)
                    print("Message sent!" + orderIn)
                except:
                    print("Connection failed to broker")
            else:
                print("Message already sent 5 times!")
            client.disconnect()
        except:
            print("mqtt not connected")
            client = ""

    def checkOrderOnTrolley(self, trolly, time_start,time_back):
        if time_start == 0:
            query = '''from(bucket: "tracking_data_comp")
                        |> range(start: -''' + str(time_back) + '''s )
                        |> filter(fn: (r) => r["_measurement"] == "Tracking_comp")
                        |> filter(fn: (r) => r["_field"] == "child")
                        |> filter(fn: (r) => r["parent"] == "'''+ trolly +'''")
                        |> group()
                        |> unique()'''
        else:
            query = '''from(bucket: "tracking_data_comp")
                        |> range(start: -''' + str(time_back) + '''s ,  stop: -''' + str(time_start) + '''s )
                        |> filter(fn: (r) => r["_measurement"] == "Tracking_comp")
                        |> filter(fn: (r) => r["_field"] == "child")
                        |> filter(fn: (r) => r["parent"] == "'''+ trolly +'''")
                        |> group()
                        |> unique()'''
        query_api = self.clientIn.query_api()
        result = query_api.query(query)
        output = result.to_values(columns=['_value'])

        if output == [] or output == None:
            query = '''from(bucket: "tracking_data_comp")
                        |> range(start: -''' + str(time_back) + '''s )
                        |> filter(fn: (r) => r["_measurement"] == "Tracking_comp")
                        |> filter(fn: (r) => r["_field"] == "parent")
                        |> filter(fn: (r) => r["child"] == "'''+ trolly +'''")
                        |> group()
                        |> unique()'''
            table = query_api.query(query)

            data = []
            output = table.to_values(columns=['_value'])
            out = [x[0] for x in output]
        else:
            out = [x[0] for x in output]
        return out


    def checkOrderOnTrolleyDelivery(self, trolly, time_start,time_back):
        query = '''from(bucket: "supplychain_data")
                    |> range(start: -''' + str(time_back) + '''s )
                    |> filter(fn: (r) => r["_measurement"] == "delviery")
                    |> filter(fn: (r) => r["vehicle"] == "''' + trolly + '''")
                    |> filter(fn: (r) => r["customer"] == "''' + self.name + '''")
                    |>group(columns: ["order"])
                    |>sort(columns: ["_time"])
                    |>last()
                     |> map(fn: (r) => ({ r with _value: r.order}))
                     |>group()'''
        query_api = self.clientIn.query_api()
        result = query_api.query(query)
        output = result.to_values(columns=["supplier","_value"])
        try:
            ordersOut = [x[1] for x in output]
            supplier = [x[0] for x in output]
        except:
            ordersOut = None
            supplier = None
        return supplier, ordersOut

    def findTrolleyActive(self, trolly, timeback):
        query = '''from(bucket: "supplychain_data")
                    |> range(start: ''' + str(timeback) + ''')
                    |> filter(fn: (r) => r["_measurement"] == "arrival")
                    |> filter(fn: (r) => r["_value"] == "''' + trolly + '''")
                    |>group()
                    |>sort(columns: ["_time"])
                    |>last()
                    |> map(fn: (r) => ({ r with _value: r.state }))'''
        
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

    def findTimeOutlast(self, trolleyIn):
        query = '''from(bucket: "supplychain_data")
                    |> range(start: -1d)
                    |> filter(fn: (r) => r["_measurement"] == "arrival")
                    |> filter(fn: (r) => r["_value"] == "''' + trolleyIn + '''")
                    |> filter(fn: (r) => r["state"] == "out")
                    |>group()
                    |>sort(columns: ["_time"])
                    |>last()
                    |> map(fn: (r) => ({ r with _value: r.state }))'''
        
        query_api = self.clientIn.query_api()
        result = query_api.query(query)
        output = result.to_values(columns=['_time', 'location'])
        if output == None or output == [] or output == None:
            location = None
            timeOut = 1000
        else:
            location = output[0][1]
            if location == None or location == "null":
                location = ""
            timeTo = output[0][0]
            current_utc_time = datetime.now(self.london_timezone)
            time_back = current_utc_time - timeTo
            timeOut = round(time_back.total_seconds())
        
        return timeOut, location

    def run(self):
        timeStart = datetime.now()
        sent = 0
        while True: 
            timeNow = datetime.now()
            if (timeNow - timeStart).total_seconds() > 5:
                print("checking")
                timeStart = datetime.now()
                try:
                    for trolly in self.trolleyList:
                        
                        activeState, output = self.findTrolleyActive(trolly, "-1d")
                        print(trolly)
                        print(activeState)
                        print(output)
                        if output != None and output != [] and activeState != None and activeState != []:
                            #print("into parts")
                            timeStarRun = output[0][0] #.astimezone(london_timezone)
                            locationLeft = output[0][2]
                            if locationLeft in self.locations:
                                # print("orders:  ")
                                # print(orders)
                                if activeState == "out": # trolley is active and moving between locations
                                    print("trolly is active: ", trolly)
                                    
                                    current_utc_time = datetime.now(self.london_timezone)
                                    time_back = current_utc_time - timeStarRun
                                    seconds_difference = round(time_back.total_seconds())
                                    print(seconds_difference)
                                    data = self.load_data_from_influxdb(seconds_difference)
                                    print(data)
                                    #print(data)
                                    if data != None:
                                        coords = [data["latitude"], data["longitude"]]
                                        start = str(data["start"])
                                        destination = str(data["destination"])
                                        dissToStart = self.haversine(self.locations[start], coords)
                                        dissToEnd = self.haversine(self.locations[destination], coords)
                                        percent_left = dissToEnd/(dissToEnd+dissToStart)
                                        journey_time = self.findJourneyTime(start,destination)
                                        remaining_time = journey_time*percent_left
                                        percent_comp = 1 - percent_left
                                        if start == self.name:
                                            print("Checking local orders sent from " + str(locationLeft))
                                            orders = self.checkOrderOnTrolley(trolly, 0, (seconds_difference  + 750))
                                            print(orders)
                                            for ord in orders:
                                                print(start)
                                                print(destination)
                                                self.sendMess(remaining_time, start, destination, percent_comp, ord, False, trolly)
                                        elif destination == self.name:
                                            print("Checking orders arriving at" + str(destination))
                                            #location = self.findTrollyLocation(trolly, self.clientIn.query_api())
                                            supplier, orders = self.checkOrderOnTrolleyDelivery(trolly, 0,(seconds_difference  + 100))
                                            # find remaining time in journey
                                            print(orders)
                                            if orders != None:
                                                for ord in orders:
                                                    print(start)
                                                    print(destination)
                                                    self.sendMess(remaining_time, start, destination, percent_comp, ord, True, trolly)
                                                
                                    # else:
                                    #     # out signal order left but no trolly function
                                    #     # sendMess(self, remaining_time, start, destination, percent_comp, orderIn, arrival, vehicle)
                                    #     for ord in orders:
                                    #         if locationLeft == self.name:
                                    #             self.sendMess(200,locationLeft, "", 0, ord, False, trolly)
                                elif activeState == "in": # trolley is at a fixed location point start 
                                    print("trolley at: " + locationLeft)
                                    destination = str(locationLeft)
                                    # find last orders on trolly now deposited in destination
                                    timeStartRun ,start = self.findTimeOutlast(trolly)
                                    print(timeStartRun)
                                    if destination == self.name:
                                        print("orders triggerred by in at" + str(destination))
                                        # new delivery for this lab from supplier
                                        supplier, orders = self.checkOrderOnTrolleyDelivery(trolly, 0,(timeStartRun))
                                        # check if desitnation is current location 
                                        if orders != None or orders != []:
                                            for i in range(len(orders)):  
                                                ord = orders[i]
                                                print(ord)
                                                print(supplier[i])
                                                print(destination)
                                                self.sendMess(0, supplier[i], destination, 1, ord, True, trolly)
                                    else: # desitination is not current location
                                        #orders = self.checkOrderOnTrolley(trolly, timeStartRun, (timeStartRun + 750))#
                                        if start == self.name:
                                            print("orders triggerred by in at customer starting at" + str(start))
                                            # delivery to customer form this lab send message to confirm arrival
                                            for ord in orders:  
                                                print(ord)
                                                print(start)
                                                print(destination)
                                                self.sendMess(0, start, destination, 1, ord, True, trolly)
                                else:
                                    print("no data for trolley")
                        else:
                            print("no trolley " + trolly + " active")
                
                except Exception as e:
                    print(e)    

if __name__ == "__main__":
    #supplyChain = SupplyChainTracker("/app/config/config.toml")
    supplyChain = SupplyChainTracker("./config_local.toml")
    supplyChain.run()

