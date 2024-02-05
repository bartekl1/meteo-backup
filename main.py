from termcolor import colored
import colorama
import paramiko

import os
import sys
import json
import getpass
import platform

colorama.init()

print(" *********************************************")
print(" *               meteo-backup                *")
print(" * Database backup tool for my meteo station *")
print(" * https://github.com/bartekl1/meteo-backup  *")
print(" *                  v. 1.0                   *")
print(" *********************************************\n")

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

print('Running mysqldump', colored('Done!', 'green'))

client.close()

print('Converting MySQL to SQLite', colored('\u2026', 'blue'), end='\r')

# mysql2sqlite_path = os.path.abspath('./mysql2sqlite')

dir_path = os.path.dirname(os.path.realpath(__file__))

if platform.system() == "Windows":
    os.system(f'wsl --cd "{dir_path}" eval "./mysql2sqlite mysql.tmp.sql | sqlite3 sqlite.tmp.db" > NUL 2>&1')
else:
    os.system(f'eval "./mysql2sqlite mysql.tmp.sql | sqlite3 sqlite.tmp.db" > /dev/null 2>&1')

mysql_temp_file.close()

os.remove('mysql.tmp.sql')

print('Converting MySQL to SQLite', colored('Done!', 'green'))
