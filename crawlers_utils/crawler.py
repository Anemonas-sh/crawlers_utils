import threading
import posixpath
from queue import Empty
from time import time
from datetime import datetime, timedelta
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from .constants import date_format, date_time_format
from .utils import save_file, print_end_estimate, get_file_name

DELAY_TO_LOAD_FULL_PAGE = 3
QUERIES_UNTIL_DRIVER_RESET = 1 # prevents memory leak

default_chrome_options = Options()
# default_chrome_options.add_extension("../RequestBlocker/RequestBlocker.crx")
default_chrome_options.add_argument('headless')
default_chrome_options.add_argument('no-sandbox')
user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/85.0.4166.0 Safari/537.36 Edg/85.0.545.0"
default_chrome_options.add_argument(f'user-agent={user_agent}')
default_chrome_options.add_argument("log-level=3")
default_chrome_options.add_experimental_option("excludeSwitches", ["enable-logging"])

class Crawler:
    def __init__(self, query_queue, output_folder, atomic_counter, query_size, bucket, debug=False, estimate_level=2, chrome_options=Options(), save_on_root: bool = False):
        self.query_queue = query_queue
        self.output_folder = output_folder
        self.atomic_counter = atomic_counter
        self.query_size = query_size
        self.bucket = bucket

        self.chrome_options = default_chrome_options
        self.init_driver()

        self.debug = debug
        self.estimate_level = estimate_level
        self.save_on_root = save_on_root


    def init_driver(self):
        while True:
            try:
                self.driver = webdriver.Chrome(options=self.chrome_options)
                self.driver.implicitly_wait(DELAY_TO_LOAD_FULL_PAGE)
                self.query_counter = 0
                break
            except Exception as e:
                pass


    def close_driver(self):
        try:
            while len(self.driver.window_handles): # close all tabs
                self.driver.switch_to.window(self.driver.window_handles[0])
                self.driver.close()
        except Exception:
            self.driver.quit()


    def reset_driver(self):
        self.close_driver()
        self.init_driver()


    def get_data(self, current_day):
        raise NotImplementedError()


    def start_requests(self):
        script_date_time, script_start_time = datetime.now(), time()

        print("Starting thread:", threading.current_thread().ident)

        while True:
            try:
                current_day = self.query_queue.get(block=False)
            except Empty:
                break

            while True:
                try:
                    data = self.get_data(current_day)
                    break
                except Exception as e:
                    print("Couldn't get data at %s |" % current_day.strftime("%d-%m-%Y"), e)
                    pass


            query_file = posixpath.join(self.output_folder, get_file_name(script_date_time, current_day, self.save_on_root))
            save_file(query_file, data, bucket=self.bucket)

            self.atomic_counter.increment()
            print_end_estimate(script_start_time, self.atomic_counter.get(), self.query_size, script_date_time, 0, self.estimate_level)

            self.query_counter += 1
            if self.query_counter >= QUERIES_UNTIL_DRIVER_RESET:
                self.reset_driver()

        self.close_driver()
        print("Finished thread:", threading.current_thread().ident)
