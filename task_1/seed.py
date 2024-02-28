import logging
from random import randint
from faker import Faker
from psycopg2 import DatabaseError
from connect import create_connect
from classes import User, Task

fake = Faker()


def create_user(conn, user: User):
    sql = """
    insert into users (fullname, email)
    values (%s, %s);
    """

    c = conn.cursor()
    try:
        c.execute(sql, (user.fullname, user.email))
        conn.commit()
    except DatabaseError as er:
        logging.error(f"Database error: {er}")
        conn.rollback()
    finally:
        c.close()


def create_task(conn, task: Task):
    sql = """
    insert into tasks (title, description, status_id, user_id)
    values (%s, %s, %s, %s)
    """

    c = conn.cursor()
    try:
        c.execute(sql, (task.title, task.description, task.status_id, task.user_id))
        conn.commit()
    except DatabaseError as er:
        logging.error(f"Database error: {er}")
        conn.rollback()
    finally:
        c.close()


if __name__ == "__main__":
    try:
        with create_connect() as conn:
            for _ in range(20):
                user = User(fake.name(), fake.email())
                create_user(conn, user)

            for _ in range(100):
                title = fake.sentence(
                    nb_words=6, variable_nb_words=True, ext_word_list=None
                )
                description = fake.text(max_nb_chars=200, ext_word_list=None)
                status_id = randint(1, 3)
                user_id = randint(1, 20)
                task = Task(title, description, status_id, user_id)
                create_task(conn, task)
    except RuntimeError as er:
        logging.error(f"Runtime error: {er}")
    except DatabaseError as er:
        logging.error(f"Database error: {er}")
