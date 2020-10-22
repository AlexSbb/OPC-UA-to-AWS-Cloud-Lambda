import asyncio
import logging
from asyncua import Client, ua
import greengrasssdk
##############################
######### Settings ###########
URL = "opc.tcp://Sbb-Book:48020"
POLLING_INTERVAL = 5 

##############################

logging.basicConfig(level=logging.WARN)
logger = logging.getLogger('asyncua')

# 1. GreenGras part
# Creating a greengrass core sdk client

GGclient = greengrasssdk.client("iot-data")

def greengrass_send_MQTT_to_Server(var):
    try:       
        GGclient.publish(
            topic="#",
            queueFullPolicy="AllOrException",
            payload="Value ={}".format(var)
        )
    except Exception as e:
        logger.error("Failed to publish message: " + repr(e))



# 2. OPC-UA part
class SubHandler(object):

    """
    Subscription Handler. To receive events from server for a subscription
    """

    def datachange_notification(self, node, val, data):
        print("Python: New data change event", node, val)
        greengrass_send_MQTT_to_Server(val)
        
    def event_notification(self, event):
        print("Python: New event", event)


async def main():
    async with Client(url=URL) as client:
        
        tag1 = client.get_node("ns=4;s=Demo.Dynamic.Scalar.Float")       
        tag2 = client.get_node("ns=4;s=Demo.Static.Scalar.Float")
       
        handler = SubHandler()
        sub = await client.create_subscription(500, handler)
        handle1 = await sub.subscribe_data_change(tag1)
        handle2 = await sub.subscribe_data_change(tag2)

      
        # await sub.unsubscribe(handle1)
        # await sub.unsubscribe(handle2)
        # await sub.delete()

        while True:
            
            val1=await tag1.read_value()
            val2=await tag2.read_value()
            
            print(f"tag1 is: {tag1} with value {val1} ")
            print(f"tag2 is: {tag2} with value {val2} ")
            greengrass_send_MQTT_to_Server(val1)
            greengrass_send_MQTT_to_Server(val2)
            await asyncio.sleep(POLLING_INTERVAL)


if __name__ == "__main__":
    asyncio.run(main())