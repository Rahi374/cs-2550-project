import threading
class Logger(object):
    thread_safe_lock = None
    file_to_write_out = None
    global_text_to_write = None
    trans_id = None

    @staticmethod
    def initialize(file_name):
        Logger.thread_safe_lock = threading.Lock()
        Logger.file_to_write_out = file_name
        Logger.global_text_to_write = ""

    @staticmethod
    def log(stringy):
        Logger.thread_safe_lock.acquire()
        Logger.global_text_to_write += ("" if Logger.trans_id == None else str(Logger.trans_id) + " ") +  stringy + "\n"
        Logger.thread_safe_lock.release()

    @staticmethod
    def write_log():
        f = open(Logger.file_to_write_out, "w+")
        f.write(Logger.global_text_to_write)
        f.close()
