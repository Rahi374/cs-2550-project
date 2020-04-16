class LogEntry():
    def __init__(self, before_image, after_image, inst, t_id):
        self.before_image = before_image
        self.after_image = after_image
        self.inst = inst
        self.t_id = t_id

    def __str__(self):
        return f"<{self.t_id}, {self.inst}, {self.before_image}, {self.after_image}>"

    def __repr__(self):
        return f"<{self.t_id}, {self.inst}, {self.before_image}, {self.after_image}>"
