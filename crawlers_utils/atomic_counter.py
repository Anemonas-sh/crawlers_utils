import threading

class AtomicCounter:

    def __init__(self, initial_value):
        self.value = initial_value
        self._lock = threading.Lock()

    def get(self):
        with self._lock:
            return self.value

    def increment(self, value=1):
        with self._lock:
            self.value += value