import psycopg2
import csv as csvcon


def get_connection():

    conn = psycopg2.connect(
            database="churn",
            user="postgres",
            password="password",
            host="localhost",
            port="5433"
        )
    return conn

def get_cursor(connection):
    return connection.cursor()





