import threading
class Logger(object):
    thread_safe_lock = None
    file_to_write_out = None
    global_text_to_write = None

    @staticmethod
    def initialize(file_name):
        self.thread_safe_lock = threading.Lock()
        self.file_to_write_out = file_name 
        self.global_text_to_write = ""

    @staticmethod
    def log(stringy):
        self.thread_safe_lock.acquire()
        self.global_text_to_write += stringy + "\n"
        self.thread_safe_lock.release()

    @staticmethod
    def write_log():
        f = open(self.file_to_write_out, "w+")
        f.write(global_text_to_write)
        f.close()
