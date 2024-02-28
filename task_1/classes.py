class User:
    def __init__(self, fullname, email):
        self.fullname = fullname
        self.email = email


class Task:
    def __init__(self, title, description, user_id, status_id=1):
        self.title = title
        self.description = description
        self.status_id = status_id
        self.user_id = user_id
