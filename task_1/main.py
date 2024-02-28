import logging
from psycopg2 import DatabaseError
from connect import create_connect
from pprint import pprint
from classes import User, Task


def format_data_into_dict(columns, data):
    result = []

    for row in data:
        record = {}
        for idx, column in enumerate(columns):
            record[column] = row[idx]
        result.append(record)

    return result


def get_task_by_id(conn, task_id):
    sql = f"""
    select * from tasks
    where id = {task_id};
    """

    task = None
    c = conn.cursor()
    try:
        c.execute(sql)
        task = c.fetchone()
    except DatabaseError as er:
        logging.error(f"Database error: {er}")
    finally:
        c.close()

    columns = [description[0] for description in c.description]

    if task:
        return format_data_into_dict(columns, [task])[0]

    return None


def get_tasks_by_user_id(conn, user_id):
    sql = f"""
    select * from tasks
    where user_id = {user_id}; 
    """

    rows = []
    c = conn.cursor()
    try:
        c.execute(sql)
        rows = c.fetchall()
    except DatabaseError as er:
        logging.error(f"Database error: {er}")
    finally:
        c.close()

    columns = [description[0] for description in c.description]
    return format_data_into_dict(columns, rows)


def get_tasks_by_status(conn, status):
    sql = f"""
    select * from tasks
    where status_id in (select id from status where name = '{status}');
    """

    rows = []
    c = conn.cursor()
    try:
        c.execute(sql)
        rows = c.fetchall()
    except DatabaseError as er:
        logging.error(f"Database error: {er}")
    finally:
        c.close()

    columns = [description[0] for description in c.description]
    return format_data_into_dict(columns, rows)


def change_task_status(conn, task_id, new_status_id):
    sql = f"""
    update tasks
    set status_id = {new_status_id}
    where id = {task_id};
    """

    updated_task = None

    c = conn.cursor()
    try:
        c.execute(sql)
        conn.commit()

        updated_task = get_task_by_id(conn, task_id)
    except DatabaseError as er:
        logging.error(f"Database error: {er}")
        conn.rollback()
    finally:
        c.close()

    return updated_task


def get_users_without_tasks(conn):
    sql = f"""
    select * from users
    where id not in (select user_id from tasks where user_id = users.id); 
    """

    rows = []
    c = conn.cursor()
    try:
        c.execute(sql)
        rows = c.fetchall()
    except DatabaseError as er:
        logging.error(f"Database error: {er}")
    finally:
        c.close()

    columns = [description[0] for description in c.description]
    return format_data_into_dict(columns, rows)


def create_task(conn, new_task: Task):
    sql = """
    insert into tasks (title, description, status_id, user_id)
    values (%s, %s, %s, %s)
    RETURNING id;
    """

    created_task = None
    c = conn.cursor()
    try:
        c.execute(
            sql,
            (
                new_task.title,
                new_task.description,
                new_task.status_id,
                new_task.user_id,
            ),
        )
        conn.commit()
        task_id = c.fetchone()[0]
        created_task = get_task_by_id(conn, task_id)
    except DatabaseError as er:
        logging.error(f"Database error: {er}")
        conn.rollback()
    finally:
        c.close()

    return created_task


def get_not_completed_tasks(conn):
    sql = f"""
    select * from tasks
    where not status_id = 3;
    """

    rows = []
    c = conn.cursor()
    try:
        c.execute(sql)
        rows = c.fetchall()
    except DatabaseError as er:
        logging.error(f"Database error: {er}")
    finally:
        c.close()

    columns = [description[0] for description in c.description]
    return format_data_into_dict(columns, rows)


def delete_task_by_id(conn, task_id):
    sql = f"""
    delete from tasks where id = {task_id};
    """

    info = None
    c = conn.cursor()
    try:
        c.execute(sql)
        deleted_rows = c.rowcount
        conn.commit()

        if deleted_rows > 0:
            return f"Task with {task_id} id deleted"
        else:
            return f"No task found with {task_id} id"
    except DatabaseError as er:
        logging.error(f"Database error: {er}")
        conn.rollback()
    finally:
        c.close()

    return info


def get_user_by_id(conn, user_id):
    sql = f"""
    select * from users
    where id = {user_id};
    """

    user = None
    c = conn.cursor()
    try:
        c.execute(sql)
        user = c.fetchone()
    except DatabaseError as er:
        logging.error(f"Database error: {er}")
    finally:
        c.close()

    columns = [description[0] for description in c.description]

    if user:
        return format_data_into_dict(columns, [user])[0]

    return None


def get_users_by_email(conn, email):
    sql = f"""
    select * from users where email like %s;
    """

    rows = []
    c = conn.cursor()
    try:
        c.execute(sql, (f"%{email}%",))
        rows = c.fetchall()
    except DatabaseError as er:
        logging.error(f"Database error: {er}")
    finally:
        c.close()

    columns = [description[0] for description in c.description]
    return format_data_into_dict(columns, rows)


def change_user_name(conn, user_id, new_user_name):
    sql = f"""
    update users
    set fullname = '{new_user_name}'
    where id = {user_id};
    """

    updated_user = None

    c = conn.cursor()
    try:
        c.execute(sql)
        conn.commit()

        updated_user = get_user_by_id(conn, user_id)
    except DatabaseError as er:
        logging.error(f"Database error: {er}")
        conn.rollback()
    finally:
        c.close()

    return updated_user


def get_count_tasks_by_status(conn):
    sql = f"""
    select s.id, s.name, count(*) as task_count from tasks t
    left join status s on t.status_id = s.id
    group by s.id
    order by s.id;
    """

    rows = []
    c = conn.cursor()
    try:
        c.execute(sql)
        rows = c.fetchall()
    except DatabaseError as er:
        logging.error(f"Database error: {er}")
    finally:
        c.close()

    columns = [description[0] for description in c.description]
    return format_data_into_dict(columns, rows)


def get_tasks_by_user_email_domain(conn, domain):
    sql = """
    select t.*, u.fullname as user_fullname, u.email as user_email
    from tasks t
    inner join users u on t.user_id = u.id
    where u.email like %s;
    """

    rows = []
    c = conn.cursor()
    try:
        c.execute(sql, (f"%{domain}",))
        rows = c.fetchall()
    except DatabaseError as er:
        logging.error(f"Database error: {er}")
    finally:
        c.close()

    columns = [description[0] for description in c.description]
    return format_data_into_dict(columns, rows)


def get_tasks_without_description(conn):
    sql = f"""
    select * from tasks
    where description is null;
    """

    rows = []
    c = conn.cursor()
    try:
        c.execute(sql)
        rows = c.fetchall()
    except DatabaseError as er:
        logging.error(f"Database error: {er}")
    finally:
        c.close()

    columns = [description[0] for description in c.description]
    return format_data_into_dict(columns, rows)


def get_users_and_tasks_by_status(conn, status):
    sql = """
    select u.*, t.id as task_id, t.title, t.description, t.status_id from users u
    inner join tasks t on t.user_id = u.id and t.status_id in (
	    select id from status
	    where name = %s
    )
    """

    rows = []
    c = conn.cursor()
    try:
        c.execute(sql, (status,))
        rows = c.fetchall()
    except DatabaseError as er:
        logging.error(f"Database error: {er}")
    finally:
        c.close()

    columns = [description[0] for description in c.description]
    return format_data_into_dict(columns, rows)


def get_count_tasks_by_users(conn):
    sql = """
    select u.*, coalesce(count(t.user_id), 0) as task_count from users u
    left join tasks t on t.user_id = u.id
    group by u.id;
    """

    rows = []
    c = conn.cursor()
    try:
        c.execute(sql)
        rows = c.fetchall()
    except DatabaseError as er:
        logging.error(f"Database error: {er}")
    finally:
        c.close()

    columns = [description[0] for description in c.description]
    return format_data_into_dict(columns, rows)


if __name__ == "__main__":
    try:
        with create_connect() as conn:
            # tasks = get_tasks_by_user_id(conn, 1)
            # pprint(tasks)

            # tasks = get_tasks_by_status(conn, "new")
            # pprint(tasks)

            # updated_task = change_task_status(conn, 1, 1)
            # pprint(updated_task)

            # users = get_users_without_tasks(conn)
            # pprint(users)

            # new_task = create_task(conn, Task("new task", "complete a new task", 1))
            # pprint(new_task)

            # tasks = get_not_completed_tasks(conn)
            # pprint(tasks)

            # print(delete_task_by_id(conn, 102))

            # pprint(get_users_by_email(conn, ".com"))

            # pprint(change_user_name(conn, 1, "Tomato Tomatovich"))

            # pprint(get_count_tasks_by_status(conn))

            # pprint(get_tasks_by_user_email_domain(conn, "@example.com"))

            # pprint(get_tasks_without_description(conn))

            # pprint(get_users_and_tasks_by_status(conn, "new"))

            pprint(get_count_tasks_by_users(conn))

    except RuntimeError as er:
        logging.error(f"Runtime error: {er}")
    except DatabaseError as er:
        logging.error(f"Database error: {er}")
