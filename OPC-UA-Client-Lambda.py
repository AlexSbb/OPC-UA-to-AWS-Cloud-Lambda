import asyncio
import logging
from asyncua import Client, ua
import greengrasssdk
import json
##############################
######### Settings ###########
URL = "opc.tcp://Sbb-Book:48020"
POLLING_INTERVAL = 10 
##############################

logging.basicConfig(level=logging.WARN)
logger = logging.getLogger('asyncua')

############################## 1. GreenGras part
# Creating a greengrass core sdk client

GGclient = greengrasssdk.client("iot-data")

def greengrass_send_MQTT_to_Server(opcData : dict):
    '''
    Convert data to JSON and publish it
    '''
    opcDataJSON=json.dumps(opcData)
    print(opcDataJSON)
    try:       
        GGclient.publish(
            topic="FromOPC",
            queueFullPolicy="AllOrException",
            # payload="Value ={}".format(var)
            payload = opcDataJSON
        )
    except Exception as e:
        logger.error("Failed to publish message: " + repr(e))


############################## 2. OPC-UA part
class SubHandler(object):

    """
    Subscription Handler. To receive events from OPC server for a subscription
    """

    def datachange_notification(self, node, val, data):
        opcData = {
                "node":str(node),
                "value": val,
                "Status": data.monitored_item.Value.StatusCode.name,
                "TimeStamp": data.monitored_item.Value.ServerTimestamp.isoformat()
            }
        # value = val
        # statusCode = data.monitored_item.Value.StatusCode
        # timeStamp = data.monitored_item.Value.ServerTimestamp
        # print("Python: New data change event", opcData)       
        greengrass_send_MQTT_to_Server(opcData)
        
    def event_notification(self, event):
        # print("Python: New event", event)
        pass


async def read_opc_data_from_tag(tag):
    '''
    Read data from OPC tag and create a dictionary. 
    Than send the dictionary with data to MQTT  Server
    '''
    data_val=await tag.read_data_value()
    opcData = {
        "node":str(tag),
        "value": data_val.Value.Value,
        "Status": data_val.StatusCode.name,
        "TimeStamp": data_val.SourceTimestamp.isoformat()
        }
    greengrass_send_MQTT_to_Server(opcData)


async def main():
    async with Client(url=URL) as client:
        
        tag1 = client.get_node("ns=4;s=Demo.Dynamic.Scalar.Float")       
        tag2 = client.get_node("ns=4;s=Demo.Static.Scalar.Float")
       
        handler = SubHandler()
        sub = await client.create_subscription(500, handler)
        handle1 = await sub.subscribe_data_change(tag1)
        handle2 = await sub.subscribe_data_change(tag2)

        while True:          
            # val1=await tag1.read_value()
            # val2=await tag2.read_value()
            # print(f"tag1 is: {tag1} with value {val1} ")
            # print(f"tag2 is: {tag2} with value {val2} ")
            await read_opc_data_from_tag(tag1)
            await read_opc_data_from_tag(tag2)
            await asyncio.sleep(POLLING_INTERVAL)

        await sub.unsubscribe(handle1)
        await sub.unsubscribe(handle2)
        await sub.delete()


asyncio.run(main())

def function_handler(event, context):
    return