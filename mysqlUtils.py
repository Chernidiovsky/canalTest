# -*- coding: utf-8 -*-
import pandas as pd
import pymysql
from sqlalchemy import create_engine
import traceback


IP = "10.0.18.54"
user = "canal"
password = "canal"


# import mysql as pandas df
# export pandas df to mysql
def mysqlEngine(database):
    string = "mysql+pymysql://%s:%s@%s:3306/%s?charset=utf8" % (user, password, IP, database)
    engine = create_engine(string)
    return engine


def mysqlCursor(database):
    db = pymysql.connect(IP, user, password, database, charset='utf8')
    cursor = db.cursor()
    return db, cursor


def executeMysql(database, sql):
    db, cursor = mysqlCursor(database)
    try:
        cursor.execute(sql)
        db.commit()
        return True
    except:
        traceback.print_exc()
        db.rollback()
        return False


def importMysqlAsPandas(database, sql):  # 单个db内的sql
    db, cursor = mysqlCursor(database)
    df = pd.read_sql(sql, db)
    return df


def exportPandasToMysql(df, database, toTable, if_exists="append"):
    engine = mysqlEngine(database)
    if if_exists == "overwrite":
        if_exists = "replace"
    try:
        df.to_sql(name=toTable, con=engine, if_exists=if_exists, index=False)
        return True
    except:
        traceback.print_exc()
        return False