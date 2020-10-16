import json
import ntpath
from pathlib import Path

# Workaround so that the wachdog can be launched from PyCharm as well as a standalone script.
configPath = Path('config.json') if Path('config.json').exists() else Path('../config.json')
config = json.loads(configPath.read_text())

print(json.dumps(config, indent=2))

API_HOST = config['api']['host']
API_PORT = config['api']['port']
API_ERROR_QUEUE_SIZE = config['api']['errorQueueSize']

def pathify(path: str) -> Path:
    """
    Unify potentially mixed, platform dependent path separators
    """
    return Path(path).absolute()

WATCHDOG_JOURNAL_DIRECTORY = ntpath.expandvars(config['watchdog']['directory'])
WATCHDOG_THREAD_POOL_SIZE = config['watchdog']['threadPoolSize']
WATCHDOG_FS_EVENT_TOLERANCE = config['watchdog']['tolerance']
WATCHDOG_REFRESH_PERIOD = config['watchdog']['refreshPeriod']

