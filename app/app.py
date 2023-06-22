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
    try:
        with open(config_file, 'r') as f:
            config = json.load(f)
        return config
    except Exception as e:
        logging.error("Error while loading configuration: " + str(e))
        exit()

def get_log_level( level):
    levels = {
        'DEBUG': logging.DEBUG,
        'INFO': logging.INFO,
        'WARNING': logging.WARNING,
        'ERROR': logging.ERROR,
        'CRITICAL': logging.CRITICAL
    }
    return levels.get(level.upper(), logging.INFO)

def setup_logging(config):
    log_format = "%(asctime)s - %(levelname)s - %(message)s"
    logging.basicConfig(level=get_log_level(config['debug']['log_level']), format=log_format)


config = load_config('config/config.json')
setup_logging(config)
polling_period_s = int(config['synker']['polling_period_s'])

# Get hostname
hostname = socket.gethostname()

# Get IP
ip = ni.ifaddresses('wlan0')[ni.AF_INET][0]['addr']


########################
# ZMQ
########################
#  ctx = zmq.asyncio.Context()
#  # Publish to the player app
#  pub_socket = ctx.socket(zmq.PUB)
#  pub_socket.bind(f"tcp://{config['zmq']['ip_bind']}:{config['zmq']['port_synker_pub']}")  # Publish to the player app

# Ports to listen on
udp_port = int(config['synker']['udp_port'])

# Dict of other nodes and the last time we heard from them
nodes = {}

async def udp_server():
    loop = asyncio.get_running_loop()
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind(("0.0.0.0", udp_port))
    sock.setblocking(False)

    while True:
        data = await loop.sock_recv(sock, 1024)
        data = data.decode()

        if "heartbeat" in data:
            node_hostname, node_ip = data.split()[1:]
            nodes[node_ip] = {"hostname": node_hostname, "last_heard": time.time()}
            if node_ip not in nodes:
                logging.info(f"Found {node_hostname} ({node_ip})")
        await asyncio.sleep(0.1)

async def udp_heartbeat():
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
    sock.setblocking(False)

    while True:
        heartbeat_msg = f"heartbeat {hostname} {ip}".encode()
        #  logging.debug(f"Sending heartbeat: {heartbeat_msg}")
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, sock.sendto, heartbeat_msg, ("<broadcast>", udp_port))
        await asyncio.sleep(polling_period_s)

async def udp_cleanup():
    while True:
        current_time = time.time()
        nodes_copy = nodes.copy()

        for node_ip, node_info in nodes_copy.items():
            if current_time - node_info["last_heard"] > polling_period_s*5:
                del nodes[node_ip]
                print(f"Lost {node_info['hostname']} ({node_ip})")

        await asyncio.sleep(polling_period_s)

ctx = zmq.asyncio.Context()
async def zmq_publisher():
# Publish to the player app
    global ctx
    pub_socket = ctx.socket(zmq.PUB)
    pub_socket.bind(f"tcp://{config['zmq']['ip_bind']}:{config['zmq']['port_synker_pub']}")  # Publish to the player app

    while True:
        known_nodes = [f"{info['hostname']} ({node_ip})" for node_ip, info in nodes.items()]
        await pub_socket.send_string(f"nodes: {known_nodes}")
        logging.debug(f"Sending nodes: {known_nodes}")
        await asyncio.sleep(polling_period_s)

async def main():
    await asyncio.gather(
        udp_server(),
        udp_heartbeat(),
        udp_cleanup(),
        zmq_publisher()
    )

if __name__ == "__main__":
    asyncio.run(main())
