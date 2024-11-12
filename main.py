import os
import urllib
import json
import pandas as pd
import numpy as np
import sqlalchemy as sa
import re
from datetime import datetime, timedelta
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

#Functions
def xlsx_files(path):
    money_files = []
    act_files = []
    prev_date = datetime.now() - timedelta(days=1)
    prev_date_str = prev_date.strftime('%Y%m%d')

    for file in os.listdir(path):
        full_path = os.path.join(path, file)
        if file.lower().endswith(f'{prev_date_str}.xlsx'):
            if file.lower().startswith('д'):
                money_files.append(full_path)
            elif file.lower().startswith('у'):
                act_files.append(full_path)
    return money_files, act_files

def extract_number(text):
    patterns = [
        r'^(?:№)?(\d+(?:[/-][\d]+)+)\s+[дД][\.,]\s*с\.',
        r'(?:договор).*?№\s*((?:[\w-]+(?:\s*[-]\s*[\d]+)?(?:[/-][\d]+)*)+)',
        r'№\s*([\w\d/-]+)(?:\s+от\s+[\d\.]+)',
        r'№\s*([\w\d/-]+)',
        r'^([\w\d/-]+)$'
    ]

    if text.count('№') > 1:
        if text.startswith('№'):
            text = text[1:].lstrip()
        parts = text.split('№', 1)
        first_part = parts[0].strip()
        if not any(marker in first_part.upper() for marker in ['ДОГОВОР', 'Д.С.', 'Д,С.']):
            space_before_num = ' ' if ' №' in text else ''
            return first_part + space_before_num + '№' + parts[1].strip()
        return parts[1].strip()

    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            return match.group(1).strip()

def check_if_exists(cursor, row, table_name):
    DocumentId = row['DocumentId']
    query = f"SELECT DocumentId FROM {table_name} WHERE DocumentId = '{DocumentId}'"
    cursor.execute(query)
    return cursor.fetchone() is not None

def update_db(cursor, df, table_name):
    tmp_df = pd.DataFrame(columns=df.columns)
    for i, row in df.iterrows():
        if check_if_exists(cursor, row, table_name):
            print(f"{row['DocumentId']} Already Exist")
        else:
            tmp_df = pd.concat([tmp_df, row.to_frame().T], axis=0, ignore_index=True)  
    return tmp_df

def insert_into_table(cursor, row, table_name):
    if table_name == 'moneytable':

        BusinessId = row['BusinessId']
        ContractNum = row['ContractNum']
        ProjectNum = row['ProjectNum']
        PaymentDate = row['PaymentDate']
        PaymentType = row['PaymentType']
        PaymentSum = row['PaymentSum']
        DocumentId = row['DocumentId']

        query = f"INSERT INTO {table_name} (BusinessId, ContractNum, ProjectNum, PaymentDate, PaymentType, PaymentSum, DocumentId)" \
        f"VALUES ('{BusinessId}'," \
        f"CASE WHEN '{ContractNum}' = N'None' THEN NULL ELSE '{ContractNum}' END," \
        f"CASE WHEN '{ProjectNum}' = N'None' THEN NULL ELSE '{ProjectNum}' END," \
        f"CASE WHEN '{PaymentDate}' = N'None' THEN NULL ELSE '{PaymentDate}' END," \
        f"CASE WHEN '{PaymentType}' = N'None' THEN NULL ELSE '{PaymentType}' END," \
        f"CASE WHEN '{PaymentSum}' = N'None' THEN NULL ELSE '{PaymentSum}' END," \
        f"'{DocumentId}')"

    elif table_name == 'acttable':
        
        BusinessId = row['BusinessId']
        ContractNum = row['ContractNum']
        ProjectNum = row['ProjectNum']
        ActNum = row['ActNum']
        ActDate = row['ActDate']
        PaymentType = row['PaymentType']
        PaymentSum = row['PaymentSum']
        DocumentId = row['DocumentId']

        query = f"INSERT INTO {table_name} (BusinessId, ContractNum, ProjectNum, ActNum, ActDate, PaymentType, PaymentSum, DocumentId)" \
        f"VALUES ('{BusinessId}'," \
        f"CASE WHEN '{ContractNum}' = N'None' THEN NULL ELSE '{ContractNum}' END," \
        f"CASE WHEN '{ProjectNum}' = N'None' THEN NULL ELSE '{ProjectNum}' END," \
        f"CASE WHEN '{ActNum}' = N'None' THEN NULL ELSE '{ActNum}' END," \
        f"CASE WHEN '{ActDate}' = N'None' THEN NULL ELSE '{ActDate}' END," \
        f"CASE WHEN '{PaymentType}' = N'None' THEN NULL ELSE '{PaymentType}' END," \
        f"CASE WHEN '{PaymentSum}' = N'None' THEN NULL ELSE '{PaymentSum}' END," \
        f"'{DocumentId}')"

    cursor.execute(query)

def append_from_df_to_db(cursor, df, Table):
    for i, row in df.iterrows():
        insert_into_table(cursor, row, Table)

def connect_to_db(connection_string):
    connection_uri = f"mssql+pyodbc:///?odbc_connect={urllib.parse.quote_plus(connection_string)}"
    engine = sa.create_engine(connection_uri, fast_executemany=True, echo=True)
    connection = engine.raw_connection()
    return connection

def send_email(subject, message, from_email, to_emails, smtp_server, smtp_port):
    try:
        server = smtplib.SMTP(smtp_server, smtp_port)
        for to_email in to_emails:
            msg = MIMEMultipart()
            msg['From'] = from_email
            msg['To'] = to_email
            msg['Subject'] = subject
            msg.attach(MIMEText(message, 'plain'))
            server.send_message(msg)
    except Exception as e:
        print("Error sending email:", str(e))
    finally:
        server.quit()

def main(cursor, file_paths, table_names):
    for (file_path, table_name) in zip(xlsx_files(file_paths), table_names):
        for file in file_path:
            df = pd.read_excel(file, dtype={'№ Договора': str, '№ Проекта': str, '№ АВР': str})

            df.columns = df.columns.str.replace("БИН", "BusinessId")
            df.columns = df.columns.str.replace("№ Договора", "ContractNum")
            df.columns = df.columns.str.replace("№ Проекта", "ProjectNum")
            df.columns = df.columns.str.replace("Дата платежа", "PaymentDate")
            df.columns = df.columns.str.replace("Признак платежа (поступление или отправка)", "PaymentType", regex=False)
            df.columns = df.columns.str.replace("Сумма платежа", "PaymentSum")
            df.columns = df.columns.str.replace("УИД документа", "DocumentId")
            df.columns = df.columns.str.replace("№ АВР", "ActNum")
            df.columns = df.columns.str.replace("Дата АВР", "ActDate")

            df.replace('', None, inplace=True)
            df.replace(r'^\s*$', None, regex=True, inplace=True)
            df.replace({np.nan: None}, inplace=True)

            df['BusinessId'] = df['BusinessId'].fillna('').astype(str)
            df['BusinessId'].replace(['','0','00'], '000000000000', inplace=True)
            df_clean = df[df["BusinessId"].str.isnumeric()].copy()

            df_clean['ContractNum'] = df_clean['ContractNum'].apply(lambda x: extract_number(x) if isinstance(x, str) else x)
            
            df_to_app = update_db(cursor, df_clean, table_name)
            append_from_df_to_db(cursor, df_to_app, table_name)

        cursor.commit()

if __name__ == "__main__":
    config_path = os.path.join(os.getcwd(), 'config.json')
    with open(config_path, "r", encoding="utf-8") as config_file:
        config = json.load(config_file)
        file_paths = config['file_paths']
        table_names = config['table_names']
        connection_string = config['connection_string']
        from_email = config['mail_message']['from_email']
        to_emails = config['mail_message']['to_emails']
        smtp_server = config['mail_message']['smtp_server']
        smtp_port = config['mail_message']['smtp_port']

    connection = connect_to_db(connection_string)
    cursor = connection.cursor()

    try:
        main(cursor, file_paths, table_names)
    except Exception as e:
        error_message = f"An error occurred: {str(e)}"
        send_email("Error in ETL process", error_message, from_email, to_emails, smtp_server, smtp_port)
        raise SystemExit(1)
    finally:
        connection.close()