import os
import pymysql
from datetime import datetime
from colorama import init, Fore, Back, Style
from database_config import DB_CONFIG
import time

init(autoreset=True)  # colorama start

def print_colorful_log(message, color=Fore.WHITE, background=Back.BLACK, style=Style.NORMAL):
    colored_message = f"{style}{background}{color}{message}{Style.RESET_ALL}"
    print(colored_message)

def startLog():
    print_colorful_log("Auto SQL Backup System by github.com/SirYavuz", Fore.CYAN, Style.BRIGHT)

try:
    db = pymysql.connect(host=DB_CONFIG["host"], database=DB_CONFIG["database"], user=DB_CONFIG["user"], password=DB_CONFIG["password"], charset=DB_CONFIG["charset"], connect_timeout=60)
    cursor = db.cursor()
    database_name = DB_CONFIG["database"]

except pymysql.Error as e:
    print_colorful_log("An error occurred while connecting to the database:", Fore.RED, Back.BLACK)
    exit(1)

def export_table_structure_and_data():
    try:
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        year = datetime.now().strftime("%Y")
        month = datetime.now().strftime("%m")
        day = datetime.now().strftime("%d")
        hour = datetime.now().strftime("%H")
        minute = datetime.now().strftime("%M")
        
        backup_folder = DB_CONFIG["path"]
        sql_full_filename = os.path.join(backup_folder, f"{year}_{month}_{day}_full_backup__{hour}_{minute}.sql")
        
        with open(sql_full_filename, "w", encoding="utf-8") as sql_file:
            sql_file.write(f"CREATE DATABASE IF NOT EXISTS `{database_name}`;\n")
            sql_file.write(f"USE `{database_name}`;\n\n")
            
            cursor.execute("SHOW TABLES")
            tables = cursor.fetchall()

            for table in tables:
                table_name = table[0]
                cursor.execute(f"SHOW CREATE TABLE {table_name}")
                create_table_sql = cursor.fetchone()[1]
                create_table_sql = create_table_sql.replace(f"CREATE TABLE", f"CREATE TABLE IF NOT EXISTS")
                sql_file.write(f"{create_table_sql};\n\n")

                cursor.execute(f"SELECT * FROM {table_name}")
                rows = cursor.fetchall()

                if len(rows) > 0:
                    columns = cursor.description
                    column_names = [col[0] for col in columns]

                    sql_file.write(f"INSERT INTO `{table_name}` (`{column_names[0]}`")

                    for idx in range(1, len(column_names)):
                        sql_file.write(f", `{column_names[idx]}`")

                    sql_file.write(") VALUES\n")

                    for idx in range(len(rows)):
                        formatted_values = []
                        for value in rows[idx]:
                            if isinstance(value, str):
                                formatted_values.append(f"'{value}'")
                            else:
                                formatted_values.append(str(value))
                        insert_values = ', '.join(formatted_values)
                        sql_file.write(f"({insert_values})")

                        if idx < len(rows) - 1:
                            sql_file.write(",\n")
                        else:
                            sql_file.write(";\n\n")
        print_colorful_log("Database structure and data exported successfully.", Fore.GREEN)
    except Exception as e:
        print_colorful_log("An error occurred during the export process:", Fore.RED, Back.BLACK)
        print(e)

try:
    while True: # loop
        startLog()
        export_table_structure_and_data()
        
        print_colorful_log("Waiting for the next backup... ", Fore.YELLOW)
        time.sleep(DB_CONFIG["backup_interval"]) # interval
finally:
    db.close()