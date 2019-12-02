# -*- coding: utf-8 -*-
"""
Created on Thursday Nov  14 10:30:00 2019

@author: Steffen Guenther
"""

""" 
Skript zur asynchronen Abfrage von Exchange-APIs. Die Abfrage wird aus dem Hauptprogramm ausgelagert seperat durchgeführt,
die Responses dem Hauptprogramm an geeigneter Schnittstelle zugefügt und wie gewohnt in die Datenbank persistiert.

Ziel: eine asynchrone Abfrage von singulären Exchange-API-Methoden, genauer den Tickern. Die Durchlaufzeit der Abfragen soll hierbei minimiert werden,
      wobei die Verarbeitungszeit der Datenbankpersistierung lediglich eine sekundäre Rolle spielt.
"""

import time
import yaml
import requests
from database import Database
import asyncio
from aiohttp import ClientSession
import queue
import json
import api_scheduling


def yaml_loader(exchange):
    with open('exchanges/' + exchange + '.yaml', 'r') as f:
        data = yaml.load(f, Loader=yaml.FullLoader)
        return data


class Request:

    def __init__(self, yaml_file):
        self.exchange = yaml_file['name']
        self.column_mapping = None
        self.api_url = yaml_file['api_url']
        self.response = None


    def execute(self):
        result = requests.get(self.api_url)
        return result

    def set_response(self, result):
        self.response = result



def fill_queue():

    import os
    exchanges_list = os.listdir('exchanges')
    exchanges = [x.split(".")[0] for x in exchanges_list if ".yaml" in x]
    exchanges.sort()
    for exchange in exchanges:
        exchange_name = exchange
        exchange_file = yaml_loader(exchange)
        request_objects[exchange_name] = Request(exchange_file)
        Job_Queue.put(request_objects[exchange_name])
    return Job_Queue


async def fetch(job, session):
    async with session.get(job.api_url) as response:
        print(job.api_url)
        request_objects[job.exchange].response = response
        Request_Queue.put(request_objects[job.exchange])
        return await response.read()  # , request_objects, Request_Queue


async def run():
    jobs = fill_queue()
    tasks = []

    # Fetch all responses within one Client session,
    # keep connection alive for all requests.
    session: ClientSession
    async with ClientSession() as session:
        for i in range(9):
       # while Job_Queue.qsize() > 1:
            job = jobs.get()
            print(Job_Queue.qsize())
            task = asyncio.ensure_future(fetch(job, session))
            tasks.append(task)

        responses = await asyncio.gather(*tasks)
        #print(Request_Queue.qsize())
        #
        for i in range(Request_Queue.qsize()):
            Test = Request_Queue.get().response
            #print(Test)
        return responses

# def print_responses(result):
#     print(result)

if __name__ == "__main__":
    Job_Queue = queue.Queue()
    request_objects = dict()
    Request_Queue = queue.Queue()

    loop = asyncio.get_event_loop()
    future = asyncio.ensure_future(run())
    responses = loop.run_until_complete(future)

    for i in responses:
        try:
            i = json.loads(i.decode('utf-8'))
            print(i)
        except:
            pass



