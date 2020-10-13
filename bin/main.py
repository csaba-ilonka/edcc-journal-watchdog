import json
import ntpath
import os
from concurrent.futures.thread import ThreadPoolExecutor
from datetime import datetime, timedelta
from pathlib import Path

import requests
from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer


class EDCCMainframeClient:

    def __init__(self, host: str = 'localhost', port: int = 8080):
        self.__url = f"http://{host}:{port}"

    def journalEvent(self, fileName: str, payload):
        print(f"Journal Event -> {fileName}")
        return self.__post(f"journal/?name={fileName}", json.dumps(payload))

    def cargoEvent(self, payload):
        return self.__post('journal/cargo', payload)

    def marketEvent(self, payload):
        return self.__post('journal/market', payload)

    def modulesEvent(self, payload):
        return self.__post('journal/modules', payload)

    def routeEvent(self, payload):
        return self.__post('journal/route', payload)

    def outfittingEvent(self, payload):
        return self.__post('journal/outfitting', payload)

    def shipyardEvent(self, payload):
        return self.__post('journal/shipyard', payload)

    def statusEvent(self, payload):
        return self.__post('journal/status', payload)

    def __post(self, endpoint: str, payload):
        requests.post(f"{self.__url}/{endpoint}", data=payload, headers={'Content-type': 'application/json', 'Accept': 'application/json'})


class JournalEventHandler(FileSystemEventHandler):

    def __init__(self, client: EDCCMainframeClient, threadPoolSize: int = 4, tolerance: int = 100):
        self.threadPool = ThreadPoolExecutor(max_workers=threadPoolSize)
        self.tolerance = timedelta(milliseconds=tolerance)
        self.lastOnCreatedEvent = datetime.now()
        self.lastOnModifiedEvent = datetime.now()
        self.client = client

    def on_created(self, event):
        if self.shouldProcessEvent(event, self.lastOnCreatedEvent):
            self.threadPool.submit(self.processEvent, event.src_path)
        self.lastOnCreatedEvent = datetime.now()

    def on_modified(self, event):
        if self.shouldProcessEvent(event, self.lastOnModifiedEvent):
            self.threadPool.submit(self.processEvent, event.src_path)
        self.lastOnModifiedEvent = datetime.now()

    def on_deleted(self, event):
        print(f"Deleted {event.src_path}")

    def shouldProcessEvent(self, event, lastProcessed: datetime):
        if datetime.now() - lastProcessed > self.tolerance:
            return False
        if not Path(event.src_path).exists():
            return False
        if os.stat(event.src_path).st_size == 0:
            return False
        return True

    def processEvent(self, filePath: str):
        filePath = str(Path(filePath).absolute())  # Unify potentially mixed, platform dependent path separators
        fileName = ntpath.basename(filePath)
        try:
            payload = Path(filePath).read_text()
            if fileName.startswith('Journal') and fileName.endswith('.log'):
                self.client.journalEvent(fileName, payload)
            elif fileName == 'Cargo.json':
                self.client.cargoEvent(json.loads(json.dumps(payload)))
            elif fileName == 'Market.json':
                self.client.marketEvent(json.loads(json.dumps(payload)))
            elif fileName == 'ModulesInfo.json':
                self.client.modulesEvent(json.loads(json.dumps(payload)))
            elif fileName == 'NavRoute.json':
                self.client.routeEvent(json.loads(json.dumps(payload)))
            elif fileName == 'Outfitting.json':
                self.client.outfittingEvent(json.loads(json.dumps(payload)))
            elif fileName == 'Shipyard.json':
                self.client.shipyardEvent(json.loads(json.dumps(payload)))
            elif fileName == 'Status.json':
                self.client.statusEvent(json.loads(json.dumps(payload)))
            else:
                print(f"Unhandled file: {fileName}")
        except Exception as e:
            print(e)

def main():

    # Workaround so that the wachdog can be launched from PyCharm as well as a standalone script.
    configPath = Path('config.json') if Path('config.json').exists() else Path('../config.json')
    config = json.loads(configPath.read_text())
    print(json.dumps(config, indent=2))

    host = config['client']['host']
    port = config['client']['port']
    directory = config['watchdog']['directory']
    threadPoolSize = config['watchdog']['threadPoolSize']
    tolerance = config['watchdog']['tolerance']

    client = EDCCMainframeClient(host=host, port=port)
    handler = JournalEventHandler(client, threadPoolSize=threadPoolSize, tolerance=tolerance)

    observer = Observer()
    observer.schedule(handler, path=ntpath.expandvars(directory), recursive=False)

    observer.start()
    print('WatchDog started')
    input('Press Enter to exit...')

    observer.stop()
    print('WatchDog stopped')

    observer.join(timeout=2)

if __name__ == "__main__":
    main()