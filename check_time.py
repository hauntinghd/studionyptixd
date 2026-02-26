import time
from datetime import datetime
import os

print(f"System Time (Unix): {time.time()}")
print(f"System Time (ISO): {datetime.now().isoformat()}")
print(f"Environment TZ: {os.environ.get('TZ', 'Not Set')}")
