import asyncio
import socket
import netifaces as ni
import zmq.asyncio
import time
import json
import logging
import random
import os

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
last_sync_time = time.time()  # Maintain the last sync time.
sync_interval = int(config['synker']['sync_interval'])  # Sync interval in seconds.
random_delay = random.randint(0, 600)  # Random delay between 0 and 10min

# Get hostname
hostname = socket.gethostname()

# get available interfaces
available_interfaces = ni.interfaces()
logging.info(available_interfaces)
# Get IP
ip = None
if 'wlan0' in available_interfaces:
    ip = ni.ifaddresses('wlan0')[ni.AF_INET][0]['addr']
else:
    print("wlan0 is not available")


########################
# ZMQ
########################
ctx = zmq.asyncio.Context()
#  # Publish to the player app
pub_socket = ctx.socket(zmq.PUB)
pub_socket.bind(f"tcp://{config['zmq']['ip_bind']}:{config['zmq']['port_synker_pub']}")  # Publish to the player app

# Ports to listen on
udp_port = int(config['synker']['udp_port'])

# Dict of other nodes and the last time we heard from them
nodes = {}

video_dir = config['video_dir']

def get_filename_from_playlist():
    global video_dir
  #open playlist json file, pick a random item, look for "filepath" key
    playlist_path = os.path.join(video_dir, "playlist.json")
    playlist = []
    with open(playlist_path, 'r') as f:
        playlist = json.load(f)
    random_item = random.choice(playlist['playlist'])
    logging.debug(f"Random item: {random_item}")
    return random_item['filepath']

async def udp_server():
  loop = asyncio.get_running_loop()
  sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
  sock.bind(("0.0.0.0", udp_port))
  sock.setblocking(False)

  while True:
    data = await loop.sock_recv(sock, 1024)
    data = data.decode()

    # Handling sync isn't separate anymore, but it might be necessary to
    # detect and treat these messages differently in the future
    if "sync" in data:
      filename = data.split()[1]  # Assuming the filename is the second word in the message.
      logging.debug(f"Sync received for filename: {filename}")
      await pub_socket.send_string(f"sync {filename}")
      
    elif "heartbeat" in data:
      node_hostname, node_ip = data.split()[1:]
      nodes[node_ip] = {"hostname": node_hostname, "last_heard": time.time()}
      if node_ip not in nodes:
        logging.info(f"Found {node_hostname} ({node_ip})")

    await asyncio.sleep(0.1)

def check_re_sync_time():
  global last_sync_time
  global random_delay
  # Check if the sync interval + random delay have passed since the last sync.
  if time.time() - last_sync_time >= (sync_interval + random_delay):
    last_sync_time = time.time()  # Reset the sync time.
    random_delay = random.randint(0, 60)  # Generate a new random delay for next time.
    logging.debug("Time to sync!")
    return True
  logging.debug("Not time to sync yet.")
  return False

async def udp_heartbeat():
  sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
  sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)

  while True:
    # Check if it's time to re-sync before sending heartbeat.
    if check_re_sync_time():
      # If yes, get a filename from the playlist.
      filename = get_filename_from_playlist()  # Insert your playlist file-choosing logic here.
      logging.info(f"Syncing {filename}")
      await pub_socket.send_string(f"sync {filename}")  # Send the filename to the player via ZMQ.
      logging.debug(f"Sync sent with filename: {filename}")

    # Send regular heartbeat.
    heartbeat_msg = f"heartbeat {hostname} {ip}".encode()
    logging.debug(f"Sending heartbeat: {heartbeat_msg}")
    sock.sendto(heartbeat_msg, ("<broadcast>", udp_port))
    await asyncio.sleep(polling_period_s)

async def udp_cleanup():
    while True:
        current_time = time.time()
        nodes_copy = nodes.copy()

        for node_ip, node_info in nodes_copy.items():
            if current_time - node_info["last_heard"] > polling_period_s*5:
                del nodes[node_ip]
                logging.info("Lost {{node_info['hostname']}} ({{node_ip}})")

        await asyncio.sleep(polling_period_s)

ctx = zmq.asyncio.Context()
async def zmq_publisher():
# Publish to the player app
    #  global ctx
    #  pub_socket = ctx.socket(zmq.PUB)
    #  pub_socket.bind(f"tcp://{config['zmq']['ip_bind']}:{config['zmq']['port_synker_pub']}")  # Publish to the player app
    global pub_socket

    while True:
        known_nodes = [f"{info['hostname']} ({node_ip})" for node_ip, info in nodes.items()]
        await pub_socket.send_string(f"nodes: {known_nodes}")
        #  logging.debug(f"Sending nodes: {known_nodes}")
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
