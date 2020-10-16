import json
import os
import threading
import time
from concurrent.futures.thread import ThreadPoolExecutor
from datetime import datetime, timedelta
from pathlib import Path

import requests
from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer

import app


class EDCCMainframeClient:

    def __init__(self, host: str = 'localhost', port: int = 8080):
        self.url = f"http://{host}:{port}"

    def journalEvent(self, fileName: str, payload):
        return self.__post(f"journal/?name={fileName}", payload)

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

    def __post(self, endpoint: str, payload: str):
        url = f"{self.url}/{endpoint}"
        print(f"api :: {datetime.now()} -> {url}")
        try:
            requests.post(url, data=payload, headers={'Content-type': 'application/json', 'Accept': 'application/json'})
        except Exception as ex:
            print(f"Failed to publish payload to '{url}' -> {ex}")

class JournalEventHandler(FileSystemEventHandler):

    def __init__(self, client: EDCCMainframeClient):
        self.threadPool = ThreadPoolExecutor(max_workers=app.WATCHDOG_THREAD_POOL_SIZE)
        self.tolerance = timedelta(milliseconds=app.WATCHDOG_FS_EVENT_TOLERANCE)
        self.lastOnCreatedEvent = datetime.now()
        self.lastOnModifiedEvent = datetime.now()
        self.client = client

    def on_created(self, event):
        path = app.pathify(event.src_path)
        print(f"on_created :: {datetime.now()} -> {path}")
        if self.shouldProcessEvent(path, self.lastOnCreatedEvent):
            self.threadPool.submit(self.processEvent, path)
        self.lastOnCreatedEvent = datetime.now()

    def on_modified(self, event):
        path = app.pathify(event.src_path)
        print(f"on_modified :: {datetime.now()} -> {path}")
        if self.shouldProcessEvent(path, self.lastOnModifiedEvent):
            self.threadPool.submit(self.processEvent, path)
        self.lastOnModifiedEvent = datetime.now()

    def on_deleted(self, event):
        path = app.pathify(event.src_path)
        print(f"on_deleted :: {path}")

    def shouldProcessEvent(self, path: Path, lastProcessed: datetime):
        if datetime.now() - lastProcessed > self.tolerance:
            return False
        if not path.exists():
            return False
        if os.stat(path).st_size == 0:
            return False
        if path.name.startswith('Journal') and not path.name.endswith('.log'):
            return False
        return True

    def processEvent(self, file: Path):
        print(f"process_event :: {datetime.now()} -> {file}")
        fileName = file.name
        try:
            payload = file.read_text()
            if fileName.startswith('Journal.') and fileName.endswith('.log'):
                self.client.journalEvent(fileName, payload)
            else:
                validatedPayload = self.__validateJson(payload)
                if fileName == 'Status.json':
                    self.client.statusEvent(validatedPayload)
                elif fileName == 'Cargo.json':
                    self.client.cargoEvent(validatedPayload)
                elif fileName == 'Market.json':
                    self.client.marketEvent(validatedPayload)
                elif fileName == 'ModulesInfo.json':
                    self.client.modulesEvent(validatedPayload)
                elif fileName == 'NavRoute.json':
                    self.client.routeEvent(validatedPayload)
                elif fileName == 'Outfitting.json':
                    self.client.outfittingEvent(validatedPayload)
                elif fileName == 'Shipyard.json':
                    self.client.shipyardEvent(validatedPayload)
                else:
                    print(f"Unhandled file: {fileName}")
        except Exception as e:
            print(e)

    # noinspection PyMethodMayBeStatic
    def __validateJson(self, rawJson: str) -> str:
        """
        Make sure that journal event payload sent to the API is at least a valid JSON.
        """
        validObj = json.loads(rawJson)
        validJson = json.dumps(validObj)
        return validJson

class JournalDirectoryObserver(Observer):
    def __init__(self):
        super().__init__()
        api = EDCCMainframeClient(host=app.API_HOST, port=app.API_PORT)
        handler = JournalEventHandler(api)
        self.schedule(handler, path=app.WATCHDOG_JOURNAL_DIRECTORY, recursive=False)
        self.__refreshThread = threading.Thread(target=self.__refreshJournalDirectory)

    def start(self):
        super().start()
        self.__refreshThread.start()
        print('WatchDog started')

    def stop(self):
        super().stop()
        self.join(timeout=2)
        self.__refreshThread.join(timeout=2)
        print('WatchDog stopped')

    def __refreshJournalDirectory(self):
        cycle = app.WATCHDOG_REFRESH_PERIOD / 1000
        print('Journal refresh thread started')
        while self.is_alive():
            for x in os.scandir(path=app.WATCHDOG_JOURNAL_DIRECTORY):
                os.stat(x).st_size
            time.sleep(cycle)
        print('Journal refresh thread stopped')

def main():

    observer = JournalDirectoryObserver()
    observer.start()

    input('Press Enter to exit...\n')

    observer.stop()

if __name__ == "__main__":
    main()