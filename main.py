import atexit
import grpc
import json
import logging
import os
import time

from typing import Dict
from typing import List
from typing import Tuple


import user_pb2
import user_pb2_grpc


name: str = 'CLOCK'
v: str = 'v2'

env_json_file: str = os.path.abspath('./env.json')

lingo = None
log = None

env = {}
env_list: List[str] = [
    'USER_PORT',
    'USER_IP',
    'SLEEP'
]


def get(key: str) -> str:
    global env
    return env[key]


def init_env() -> None:
    for e in env_list:
        if e in env:
            msg: str = 'Found env var "%s" in file with default value "%s"'
            log.info(msg, e, get(e))
        else:
            env[e] = os.environ[e]
            log.info('Found env var "%s" with value "%s"', e, env[e])


def init_atexit() -> None:
    def end():
        logging.info('bye')

    atexit.register(end)


def init_log() -> None:
    global log
    global name
    global v

    logging.basicConfig(
        format=f'[{v}] [{name}] %(asctime)s %(levelname)-8s %(message)s',
        level=logging.DEBUG,
        datefmt='%Y-%m-%d %H:%M:%S')

    log = logging.getLogger(name)
    log.info('hi')


def init_json() -> None:
    global env

    try:
        json_file = open(env_json_file, 'r')
        env = json.load(json_file)
    except FileNotFoundError as fe:
        log.warning('Did not find env json file - using env vars')


def init() -> None:
    init_log()
    init_json()
    init_env()
    init_atexit()


def trigger() -> None:
    ip: str = get('USER_IP')
    port: str = get('USER_PORT')
    addr: str = f'{ip}:{port}'

    log.info('Triggering USER service')
    channel = grpc.insecure_channel(addr)
    stub = user_pb2_grpc.PingStub(channel)
    stub.Trigger(user_pb2.Ack(msg=True))
    log.info('USER triggered')


def sleep() -> None:
    log.info('Sleeping for %s', get('SLEEP'))
    time.sleep(int(get('SLEEP')))


def main() -> None:
    init()

    while True:
        trigger()
        sleep()


if __name__ == '__main__':
    main()
