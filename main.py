from termcolor import colored
import colorama
import paramiko
import mysql.connector

import os
import sys
import json
import getpass
import platform
import sqlite3

def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d

try:
    colorama.init()

    print(" *************************************************")
    print(" *                 meteo-backup                  *")
    print(" *   Database backup tool for my meteo station   *")
    print(" *   https://github.com/bartekl1/meteo-backup    *")
    print(" *          by @bartekl1         v. 1.0          *")
    print(" *************************************************\n")

    try:
        with open('configs.json') as file:
            configs = json.load(file)
    except Exception:
        print(colored('Error! Config file don\'t exist or includes errors.', 'red'))
        sys.exit(1)

    try:
        host = configs['ssh']['host']
    except Exception:
        print(colored('Error! No SSH host specified.', 'red'))
        sys.exit(1)

    try:
        username = configs['ssh']['user']
    except Exception:
        print(colored('Error! No SSH user specified.', 'red'))
        sys.exit(1)

    password = configs['ssh']['password'] if 'password' in configs['ssh'].keys() \
        else getpass.getpass("Enter password: ")

    # try:
    #     key = paramiko.RSAKey.from_private_key_file(
    #         os.path.expanduser('~/.ssh/id_rsa'),
    #         password=password) \
    #             if 'key' in configs['ssh'] and configs['ssh']['key'] else None
    # except Exception:
    #     print(colored('Error! Invalid SSH private key or password.', 'red'))
    #     sys.exit(1)

    client = paramiko.client.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    try:
        client.connect(host, username=username, password=password)
    except Exception:
        print(colored('Error! SSH connection error.', 'red'))
        sys.exit(1)

    remote_mysql_host = configs['remote_mysql']['host'] \
        if 'host' in configs['remote_mysql'].keys() else 'localhost'
    remote_mysql_port = configs['remote_mysql']['port'] \
        if 'port' in configs['remote_mysql'].keys() else 3306
    remote_mysql_user = configs['remote_mysql']['user'] \
        if 'user' in configs['remote_mysql'].keys() else 'root'
    remote_mysql_password = configs['remote_mysql']['password'] \
        if 'password' in configs['remote_mysql'].keys() else ''
    remote_mysql_database = configs['remote_mysql']['database'] \
        if 'database' in configs['remote_mysql'].keys() else 'meteo'

    mysqldump_command = f'mysqldump --host={remote_mysql_host} --port={remote_mysql_port} --user={remote_mysql_user} --password={remote_mysql_password} {remote_mysql_database}'

    print('Running mysqldump', colored('\u2026', 'blue'), end='\r')

    mysqldump = client.exec_command(mysqldump_command)[1].read().decode()

    mysql_temp_file = open("mysql.tmp.sql", "w+")
    mysql_temp_file.write(mysqldump)

    client.close()

    print('Running mysqldump', colored('Done!', 'green'))

    print('Converting MySQL to SQLite', colored('\u2026', 'blue'), end='\r')

    # mysql2sqlite_path = os.path.abspath('./mysql2sqlite')

    dir_path = os.path.dirname(os.path.realpath(__file__))

    if platform.system() == "Windows":
        os.system(f'wsl --cd "{dir_path}" eval "./mysql2sqlite mysql.tmp.sql | sqlite3 sqlite.tmp.db" > NUL 2>&1')
    else:
        os.system('eval "./mysql2sqlite mysql.tmp.sql | sqlite3 sqlite.tmp.db" > /dev/null 2>&1')

    mysql_temp_file.close()

    os.remove('mysql.tmp.sql')

    print('Converting MySQL to SQLite', colored('Done!', 'green'))

    print('Checking databases', colored('\u2026', 'blue'), end='\r')

    con = sqlite3.connect('sqlite.tmp.db')
    con.row_factory = dict_factory
    cur = con.cursor()

    cur.execute("PRAGMA table_info(readings)")

    remote_table = cur.fetchall()

    local_mysql_host = configs['local_mysql']['host'] \
        if 'host' in configs['local_mysql'].keys() else 'localhost'
    local_mysql_port = configs['local_mysql']['port'] \
        if 'port' in configs['local_mysql'].keys() else 3306
    local_mysql_user = configs['local_mysql']['user'] \
        if 'user' in configs['local_mysql'].keys() else 'root'
    local_mysql_password = configs['local_mysql']['password'] \
        if 'password' in configs['local_mysql'].keys() else ''
    local_mysql_database = configs['local_mysql']['database'] \
        if 'database' in configs['local_mysql'].keys() else 'meteo_backup'

    db = mysql.connector.connect(
        host=local_mysql_host,
        port=local_mysql_port,
        user=local_mysql_user,
        password=local_mysql_password
    )

    cursor = db.cursor(dictionary=True)

    cursor.execute("SHOW DATABASES")

    result = cursor.fetchall()

    database_exists = local_mysql_database in [row['Database'] for row in result]

    if not database_exists:
        cursor.execute(f'CREATE DATABASE {local_mysql_database}')

    cursor.execute(f'USE {local_mysql_database}')

    cursor.execute(f'SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = "readings" AND TABLE_SCHEMA = "{local_mysql_database}"')

    result = cursor.fetchall()

    if len(result) == 0:
        primary_key = [row for row in remote_table if row['pk'] == 1][0]['name']
        query = 'CREATE TABLE readings(' \
                + ', '.join([f'{row["name"]} {row["type"]} {["", "NOT NULL"][row["notnull"]]} {["", "AUTO_INCREMENT"][int(row["name"] == primary_key)]} {["", "DEFAULT " + str(row["dflt_value"])][int(row["dflt_value"] is not None)]}' for row in remote_table]) \
                + f', PRIMARY KEY ({primary_key}) )'

        cursor.execute(query)
    else:
        local_table_columns = [col["COLUMN_NAME"] for col in result]
        compatible = False not in [col["name"] in local_table_columns for col in remote_table]

        if not compatible:
            print(colored('Warning! Local table is incompatible.', 'yellow'))

    print('Checking databases', colored('Done!', 'green'))

    print('Importing', colored('\u2026', 'blue'), end='\r')

    cur.execute("SELECT COUNT() FROM readings")

    rows_number = cur.fetchall()[0]["COUNT()"]
    rows_processed = 0
    last_id = 0

    conflicts = []

    while rows_number > rows_processed:
        cur.execute(f"SELECT * FROM readings WHERE id > {last_id} LIMIT 100")
        rows = cur.fetchall()

        cursor.execute(f'SELECT * FROM readings WHERE id BETWEEN %s AND %s', (rows[0]['id'], rows[-1]['id']))
        result = cursor.fetchall()
        result = {col['id']: col for col in result}

        rows_to_insert = []

        for row in rows:
            if row['id'] not in result.keys():
                rows_to_insert.append(row)
            else:
                pass

            rows_processed += 1
            last_id = row['id']

        if len(rows_to_insert) != 0:
            sql = 'INSERT INTO readings (' + ', '.join(rows[0].keys()) + \
                ') VALUES (' + ('%s, ' * len(rows[0].keys()))[:-2] + ')'

            cursor.executemany(sql, [tuple(row.values()) for row in rows])
            db.commit()

        print('Importing', colored(f'{rows_processed}/{rows_number}', 'blue'), end='\r')

    print('Importing', colored('Done!', 'green'), ' ' * 10)

    print(colored('\nDone!', 'blue'))

    sys.exit(0)

except KeyboardInterrupt:
    sys.exit(0)
