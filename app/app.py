import asyncio
import socket
import netifaces as ni
import zmq.asyncio
import time
import json
import logging

############################
# CONFIG
############################
def load_config(config_file):
    with open(config_file, 'r') as f:
        config = json.load(f)
    return config

def get_log_level( level):
    levels = {
        'DEBUG': logging.DEBUG,
        'INFO': logging.INFO,
        'WARNING': logging.WARNING,
        'ERROR': logging.ERROR,
        'CRITICAL': logging.CRITICAL
    }
    return levels.get(level.upper(), logging.INFO)

config = load_config('config/config.json')

logging.basicConfig(level=get_log_level(config['debug']['log_level']))

# Get hostname
hostname = socket.gethostname()

# Get IP
ip = ni.ifaddresses('wlan0')[ni.AF_INET][0]['addr']


########################
# ZMQ
########################
ctx = zmq.asyncio.Context()
# Publish to the player app
pub_socket = ctx.socket(zmq.PUB)
pub_socket.bind(f"tcp://{config['zmq']['ip_bind']}:{config['zmq']['port_synker_pub']}")  # Publish to the player app

# Ports to listen on
udp_port = config['synker']['udp_port']

# Dict of other nodes and the last time we heard from them
nodes = {}

async def udp_server():
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind((ip, udp_port))
    sock.setblocking(False)

    while True:
        data, addr = await loop.sock_recv(sock, 1024)
        data = data.decode()

        if "heartbeat" in data:
            node_hostname, node_ip = data.split()[1:]
            nodes[node_ip] = {"hostname": node_hostname, "last_heard": time.time()}
            print(f"Heard from {node_hostname} ({node_ip})")

async def udp_heartbeat():
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setblocking(False)

    while True:
        heartbeat_msg = f"heartbeat {hostname} {ip}".encode()
        await loop.sock_sendto(sock, heartbeat_msg, ("<broadcast>", udp_port))
        await asyncio.sleep(1)

async def udp_cleanup():
    while True:
        current_time = time.time()
        nodes_copy = nodes.copy()

        for node_ip, node_info in nodes_copy.items():
            if current_time - node_info["last_heard"] > 10:
                del nodes[node_ip]
                print(f"Lost {node_info['hostname']} ({node_ip})")

        await asyncio.sleep(1)

async def zmq_publisher():
    context = zmq.asyncio.Context()
    pub = context.socket(zmq.PUB)
    pub.bind(f"tcp://*:{zmq_port}")

    while True:
        known_nodes = [f"{info['hostname']} ({node_ip})" for node_ip, info in nodes.items()]
        await pub.send_string(f"nodes: {known_nodes}")
        await asyncio.sleep(5)

async def main():
    await asyncio.gather(
        udp_server(),
        udp_heartbeat(),
        udp_cleanup(),
        zmq_publisher()
    )

loop = asyncio.get_event_loop()
loop.run_until_complete(main())

