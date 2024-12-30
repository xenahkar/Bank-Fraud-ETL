import psycopg2
import pandas as pd
import os
from datetime import datetime
from config import Config

# подключение к локальной бд
connection_local = psycopg2.connect(**Config.local)
connection_local.autocommit = False

# подключение к удаленной бд, где хранятся сведения о картах, счетах и клиентах
connection_pg = psycopg2.connect(**Config.remote)
connection_local.autocommit = False

# создание таблиц/ очистка стейджинга
with connection_local.cursor() as cursor:
    try:
        with open(os.path.join(os.getcwd(), 'main.ddl'), 'r') as file:
            ddl_commands = file.read()
        cursor.execute(ddl_commands)
        connection_local.commit()
        print(f"Таблицы из main.ddl созданы.")

    except Exception as e:
        connection_local.rollback()
        print(f"Ошибка в создании таблиц: {e}")
        stg_tables = [
            'kkar_stg_transactions',
            'kkar_stg_terminals',
            'kkar_stg_blacklist',
            'kkar_stg_cards',
            'kkar_stg_accounts',
            'kkar_stg_clients'
       ]
        for table in stg_tables:
            try:
                cursor.execute(f'TRUNCATE TABLE {table};')
            except Exception as te:
                print(f"Ошибка в очищениитаблицы {table}: {te}")
        connection_local.commit()
        print("Стейджинг очищен.")

# получение названия файлов из папки data и даты из их названия
data_folder = 'data'
files = os.listdir(data_folder)
passport_blacklist_files = sorted([f for f in files if f.startswith("passport_blacklist_")])
terminals_files = sorted([f for f in files if f.startswith("terminals_")])
transactions_files = sorted([f for f in files if f.startswith("transactions_")])
dates = [filename[10:18] for filename in terminals_files]
for num_date in range(len(dates)):
    dates[num_date] = f'{dates[num_date][4:]}-{dates[num_date][2:4]}-{dates[num_date][:2]}'

# загрузка файла transactions_xxxxxxxx.txt в стейджинг
transactions = pd.read_csv(os.path.join(data_folder, transactions_files[0]), sep=";")
transactions["amount"] = transactions["amount"].map(lambda z: z.strip().replace(',', '.'))
query = open(f"{os.getcwd()}/sql_scripts/insert_transactions_stg.sql", "r").read()
with connection_local.cursor() as cursor:
    try:
        cursor.executemany(query, transactions.values.tolist())
        connection_local.commit()
        print("Данные 'transactions' успешно вставлены в стейджинг.")
    except Exception as e:
        connection_local.rollback()
        print(f"Ошибка вставки данных 'transactions' в стейджинг: {e}")

# загрузка файла terminals_xxxxxxxx.xlsx в стейджинг
terminals = pd.read_excel(os.path.join(data_folder, terminals_files[0]))
terminals['create_dt'] = dates[0]
query = open(f"{os.getcwd()}/sql_scripts/insert_terminals_stg.sql", "r").read()
with connection_local.cursor() as cursor:
    try:
        cursor.executemany(query, terminals.values.tolist())
        connection_local.commit()
        print("Данные 'terminals' успешно вставлены в стейджинг.")
    except Exception as e:
        connection_local.rollback()
        print(f"Ошибка вставки данных 'terminals' в стейджинг: {e}")

# загрузка файла passport_blacklist_xxxxxxxx.xlsx в стейджинг
passport_blacklist = pd.read_excel(os.path.join(data_folder, passport_blacklist_files[0]))
query = open(f"{os.getcwd()}/sql_scripts/insert_passport_blacklist_stg.sql", "r").read()
with connection_local.cursor() as cursor:
    try:
        cursor.executemany(query, passport_blacklist.values.tolist())
        connection_local.commit()
        print("Данные 'passport_blacklist' успешно вставлены в стейджинг.")
    except Exception as e:
        connection_local.rollback()
        print(f"Ошибка вставки данных 'passport_blacklist' в стейджинг: {e}")

# загрузка таблицу clients в датафрейм, а дальше в стейджинг
with connection_pg.cursor() as cursor:
    try:
        cursor.execute('SELECT * FROM info.clients')
        records = cursor.fetchall()
        col_names = [x[0] for x in cursor.description]
        clients = pd.DataFrame(records, columns=col_names)
    except Exception as e:
        print(f"Ошибка при загрузке таблицы clients в датафрейм: {e}")

query = open(f"{os.getcwd()}/sql_scripts/insert_clients_stg.sql", "r").read()
with connection_local.cursor() as cursor:
    try:
        cursor.executemany(query, clients.values.tolist())
        connection_local.commit()
        print("Данные 'clients' успешно вставлены в стейджинг.")
    except Exception as e:
        connection_local.rollback()
        print(f"Ошибка вставки данных 'clients' в стейджинг: {e}")

# загрузка таблицы accounts в датафрейм, а дальше в стейджинг
with connection_pg.cursor() as cursor:
    try:
        cursor.execute('SELECT * FROM info.accounts')
        records = cursor.fetchall()
        col_names = [x[0] for x in cursor.description]
        accounts = pd.DataFrame(records, columns=col_names)
    except Exception as e:
        print(f"Ошибка при загрузке таблицы accounts в датафрейм: {e}")

query = open(f"{os.getcwd()}/sql_scripts/insert_accounts_stg.sql", "r").read()
with connection_local.cursor() as cursor:
    try:
        cursor.executemany(query, accounts.values.tolist())
        connection_local.commit()
        print("Данные 'accounts' успешно вставлены в стейджинг.")
    except Exception as e:
        connection_local.rollback()
        print(f"Ошибка вставки данных 'accounts' в стейджинг: {e}")

# загрузка таблицы cards в датафрейм, а дальше в стейджинг
with connection_pg.cursor() as cursor:
    try:
        cursor.execute('SELECT * FROM info.cards')
        records = cursor.fetchall()
        col_names = [x[0] for x in cursor.description]
        cards = pd.DataFrame(records, columns=col_names)
    except Exception as e:
        print(f"Ошибка при загрузке таблицы cards в датафрейм: {e}")

query = open(f"{os.getcwd()}/sql_scripts/insert_cards_stg.sql", "r").read()
with connection_local.cursor() as cursor:
    try:
        cursor.executemany(query, cards.values.tolist())
        connection_local.commit()
        print("Данные 'cards' успешно вставлены в стейджинг.")
    except Exception as e:
        connection_local.rollback()
        print(f"Ошибка вставки данных 'cards' в стейджинг: {e}")

# загрузка данных из стейджинга в целевую таблицу kkar_dwh_dim_terminals
query = open(f"{os.getcwd()}/sql_scripts/insert_terminals_dwh.sql", "r").read()
try:
    with connection_local.cursor() as cursor:
        cursor.execute(query)
        connection_local.commit()
        print("Данные успешно загружены в таблицу kkar_dwh_dim_terminals.")
except Exception as e:
    print(f"Ошибка при загрузке данных в таблицу kkar_dwh_dim_terminals: {e}")

query = open(f"{os.getcwd()}/sql_scripts/update_terminals_dwh.sql", "r").read()
try:
    with connection_local.cursor() as cursor:
        cursor.execute(query)
        connection_local.commit()
        print("Данные успешно обновлены в таблице kkar_dwh_dim_terminals")
except Exception as e:
    print(f"Ошибка при обновлении данных в таблицу kkar_dwh_dim_terminals: {e}.")

# загрузка данных из стейджинга в целевую таблицу kkar_dwh_dim_cards
query = open(f"{os.getcwd()}/sql_scripts/insert_cards_dwh.sql", "r").read()
try:
    with connection_local.cursor() as cursor:
        cursor.execute(query)
        connection_local.commit()
        print("Данные успешно загружены в таблицу kkar_dwh_dim_cards.")
except Exception as e:
    print(f"Ошибка при загрузке данных в таблицу kkar_dwh_dim_cards: {e}")

# загрузка данных из стейджинга в целевую таблицу kkar_dwh_dim_accounts
query = open(f"{os.getcwd()}/sql_scripts/insert_accounts_dwh.sql", "r").read()
try:
    with connection_local.cursor() as cursor:
        cursor.execute(query)
        connection_local.commit()
        print("Данные успешно загружены в таблицу kkar_dwh_dim_accounts.")
except Exception as e:
    print(f"Ошибка при загрузке данных в таблицу kkar_dwh_dim_accounts: {e}")

# загрузка данных из стейджинга в целевую таблицу kkar_dwh_dim_clients
query = open(f"{os.getcwd()}/sql_scripts/insert_clients_dwh.sql", "r").read()
try:
    with connection_local.cursor() as cursor:
        cursor.execute(query)
        connection_local.commit()
        print("Данные успешно загружены в таблицу kkar_dwh_dim_clients.")
except Exception as e:
    print(f"Ошибка при загрузке данных в таблицу kkar_dwh_dim_clients: {e}")

# загрузка данных из стейджинга в целевую таблицу kkar_dwh_fact_passport_blacklist
query = open(f"{os.getcwd()}/sql_scripts/insert_passport_blacklist_dwh.sql", "r").read()
try:
    with connection_local.cursor() as cursor:
        cursor.execute(query)
        connection_local.commit()
        print("Данные успешно загружены в таблицу kkar_dwh_fact_passport_blacklist.")
except Exception as e:
    print(f"Ошибка при загрузке данных в таблицу kkar_dwh_fact_passport_blacklist: {e}")

# загрузка данных из стейджинга в целевую таблицу kkar_dwh_fact_passport_blacklist
query = open(f"{os.getcwd()}/sql_scripts/insert_transactions_dwh.sql", "r").read()
try:
    with connection_local.cursor() as cursor:
        cursor.execute(query)
        connection_local.commit()
        print("Данные успешно загружены в таблицу kkar_dwh_fact_transactions.")
except Exception as e:
    print(f"Ошибка при загрузке данных в таблицу kkar_dwh_fact_transactions: {e}")

# признаки мошеннических операций: запись в rep_fraud
query = open(f"{os.getcwd()}/sql_scripts/insert_fraud.sql", "r").read()
report_dt = datetime.now().strftime('%Y-%m-%d')
try:
    with connection_local.cursor() as cursor:
        commands = query.split(';')
        for command in commands:
            command = command.strip()
            if command:
                try:
                    cursor.execute(command, (report_dt,))
                    connection_local.commit()
                except Exception as e:
                    print(f"Ошибка при загрузке данных в таблицу kkar_rep_fraud: {e}.")
    print("Данные успешно загружены в таблицу kkar_rep_fraud.")
except Exception as e:
    print(f"Ошибка при подключении или выполнении: {e}.")

# закрытие соединений бд
connection_local.close()
connection_pg.close()
print(f'Отчет за {dates[0]} загружен.')

# переименование обработанных файлов и их перенос в другой каталог
try:
    os.rename(f'{os.getcwd()}/data/{terminals_files[0]}', f'{os.getcwd()}/archive/{terminals_files[0]}.backup')
    os.rename(f'{os.getcwd()}/data/{transactions_files[0]}', f'{os.getcwd()}/archive/{transactions_files[0]}.backup')
    os.rename(f'{os.getcwd()}/data/{passport_blacklist_files[0]}', f'{os.getcwd()}/archive/{passport_blacklist_files[0]}.backup')
except IndexError:
    print("Все файлы обработаны.")
