import psycopg2


def connect(dbname, user, password, host='localhost', port=5487):
    """
    Connect to postgresql database
    :param dbname:
    :param user:
    :param password:
    :param host:
    :param port:
    :return:
    """
    try:
        conn = psycopg2.connect(dbname=dbname,
                                user=user,
                                password=password,
                                host=host,
                                port=port)
        return conn
    except Exception as e:
        raise RuntimeError(f'Failed to connect to database {dbname}\n', e)


def close(conn):
    """
    Close connection intended to be used as an atexit function
    :param conn:
    :return:
    """
    conn.close()


def execute_cmd(conn, cmd, fetch=True, commit=False):
    """
    Execute a postgresql command and return output
    :param conn:
    :param cmd:
    :param fetch: Fetch results (default yes)
    :param commit: Commit after executing command
    :return:
    """

    with conn.cursor() as cursor:
        try:
            cursor.execute(cmd)
        except Exception as e:
            raise RuntimeError(f'Failed to run cmd: {cmd}\n', e)
        if commit:
            conn.commit()
        if fetch:
            return cursor.fetchall()
        else:
            return
