import os
import time

import psycopg2
from config import load_os_dotenv


#
# main = psycopg2.connect(
#     host=os.getenv("POSTGRES_HOST", default='192.168.50.83'),
#     port=os.getenv("POSTGRES_PORT", default='5432'),
#     database=os.getenv("POSTGRES_DATABASE", default='datafeed'),
#     user=os.getenv("POSTGRES_USER", default='df_user'),
#     password=os.getenv("POSTGRES_PASS", default='Dsc@2022')
# )

load_os_dotenv()
main = psycopg2.connect(
    host=os.getenv('POSTGRES_HOST'),
    port=os.getenv('POSTGRES_PORT'),
    database=os.getenv('POSTGRES_DATABASE'),
    user=os.getenv('POSTGRES_USER'),
    password=os.getenv('POSTGRES_PASS')
)



def execute(self, query):
    global main
    try:
        cursor = main.cursor()
        cursor.execute(self, query)
        main.commit()
        cursor.close()
        main.close()
    except psycopg2.OperationalError:
        main = psycopg2.connect(
            host=os.getenv("POSTGRES_HOST"),
            port=os.getenv("POSTGRES_PORT"),
            database=os.getenv("POSTGRES_DATABASE"),
            user=os.getenv("POSTGRES_USER"),
            password=os.getenv("POSTGRES_PASS")
        )
        print("Retry connect to database")
        execute(self, query)


def execute_many(self, query):
    # global main
    try:
        main = psycopg2.connect(
            host=os.getenv("POSTGRES_HOST"),
            port=os.getenv("POSTGRES_PORT"),
            database=os.getenv("POSTGRES_DATABASE"),
            user=os.getenv("POSTGRES_USER"),
            password=os.getenv("POSTGRES_PASS")
        )
        cursor = main.cursor()
        cursor.executemany(self, query)
        main.commit()
        cursor.close()
        main.close()
    except psycopg2.OperationalError:
        # main = psycopg2.connect(
        #     host=os.getenv("POSTGRES_HOST"),
        #     port=os.getenv("POSTGRES_PORT"),
        #     database=os.getenv("POSTGRES_DATABASE"),
        #     user=os.getenv("POSTGRES_USER"),
        #     password=os.getenv("POSTGRES_PASS")
        # )
        print("Retry connect to database")
        execute_many(self, query)
        # main.close()