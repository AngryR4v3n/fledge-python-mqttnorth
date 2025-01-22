import copy
import os
import sys

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(SCRIPT_DIR))
from fledge.common import logger
import asyncio
from aiomqtt import Client
import json

# Fledge documentation - Developing north plugins.
# https://fledge-iot.readthedocs.io/en/latest/plugin_developers_guide/04_north_plugins.html

__author__ = "Francisco Molina"
__license__ = "Apache 2.0"
__version__ = "${VERSION}"

_LOGGER = logger.setup(__name__)

_CONFIG_CATEGORY_NAME = "MQTT"
_CONFIG_CATEGORY_DESCRIPTION = "MQTT Python North Plugin"
_DEFAULT_CONFIG = {
    "plugin": {
        "description": "Python module name of the plugin to load",
        "type": "string",
        "default": "mqttnorth",
        "readonly": "true",
    },
    "host": {
        "description": "IP of MQTT server to connect to",
        "type": "string",
        "default": "127.0.0.1",
        "order": "1",
        "displayName": "MQTT Broker IP Addresss",
        "mandatory": "true",
    },
    "port": {
        "description": "Port to connect to",
        "type": "integer",
        "default": "1883",
        "order": "2",
        "displayName": "MQTT Port",
        "mandatory": "true",
    },
    "prefixToRemove": {
        "description": "If your asset name comes with an undesired prefix, define it.",
        "type": "string",
        "default": "",
        "order": "3",
        "displayName": "Asset name prefix to be removed",
    },
    "prefix": {
        "description": "Topic where processed information will be published",
        "type": "string",
        "default": "Fledge",
        "order": "4",
        "displayName": "Topic Prefix",
        "mandatory": "true",
    },
    "qos": {
        "description": "QoS to use when publishing",
        "type": "integer",
        "default": "0",
        "order": "5",
        "displayName": "QoS",
    }
}


def plugin_info():
    return {
        "name": "MQTT North Connector",
        "version": "1.0.0",
        "type": "north",
        "interface": "1.0",
        "config": _DEFAULT_CONFIG,
    }


def plugin_init(config):
    config_data = copy.deepcopy(config)
    config_data["client"] = AsyncMqtt(config_data)
    return config_data


async def plugin_send(handle, payload, stream_id):
    """Used to send the readings block to the configured destination
    Args:
        handle: An object which is returned by plugin_init
        payload: A list of readings block
        stream_id: An integer that uniquely identifies the connection from Fledge instance to the destination system
    Returns:
          Tuple which consists of
          - A Boolean that indicates if any data has been sent
          - The object id of the last reading which has been sent
          - Total number of readings which has been sent to the configured destination
    """
    try:
        client = handle["client"]
        is_data_sent, new_last_object_id, num_sent = await client.publish_messages(
            payload
        )
    except asyncio.CancelledError:
        pass

    return is_data_sent, new_last_object_id, num_sent


def plugin_shutdown(handle):
    client = handle["client"]
    client.disconnect()
    handle["client"] = None


def plugin_reconfigure():
    pass


class AsyncMqtt:
    def __init__(self, config):
        self.client = None
        self.event_loop = asyncio.get_event_loop()
        self.host = config["host"]["value"]
        self.port = int(config["port"]["value"])
        if config["prefixToRemove"]["value"] == "":
            self.prefix_to_remove = None
        else:
            self.prefix_to_remove = config["prefixToRemove"]["value"]

        self.topic_prefix = config["prefix"]["value"].replace("/", "")

        self.qos = int(config["qos"]["value"])

    async def publish_messages(self, payloads):
        is_data_sent, last_object_id, num_sent = False, 0, 0
        _LOGGER.error("Received payload shape: {}".format(payloads[0]))
        try:

            async with Client(self.host, self.port) as client:
                for payload in payloads:
                    try:
                        target_topic = payload.get("asset_code").replace(self.prefix_to_remove, "")
                    except TypeError:
                        target_topic = payload.get("asset_code")
                    await client.publish(
                        f"{self.topic_prefix}/{target_topic}", json.dumps(payload), qos=self.qos
                    )
                is_data_sent = True
                num_sent += len(payloads)
                last_object_id = payloads[-1]["id"]
                _LOGGER.debug(is_data_sent, last_object_id, num_sent)
                return is_data_sent, last_object_id, num_sent
        except Exception as e:
            _LOGGER.error("Unable to publish to MQTT broker error -> {}".format(e))
            

    def disconnect(self):
        if self.client is not None:
            self.event_loop.run_until_complete(self.client.disconnect())
