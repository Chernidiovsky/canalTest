import pandas as pd
import numpy as np
import random
from time import sleep
from mysqlUtils import importMysqlAsPandas, exportPandasToMysql, executeMysql


def createFakeData():
    name1 = "赵钱孙李周吴郑王冯陈褚卫蒋沈韩杨"
    name2 = " 子寅卯辰午未申酉亥东西南北佰仟亿兆二五八万"
    name3 = " 富强民主文明和谐忠孝礼仪荣华富贵洪福永享寿与天齐"
    names = [(n1 + n2 + n3).replace(" ", "") for n1 in name1 for n2 in name2 for n3 in name3]
    names = [name for name in names if len(name) > 1]
    random.shuffle(names)
    names = random.sample(names, 100)

    def fake(n):
        age = int(np.random.randint(20, 60))
        money = float(np.round(age * (np.random.randn() * 200 + 1000), 2))
        status = int(np.random.randint(1, 9))
        return [n, age, money, status]

    df = pd.DataFrame(data=[fake(name) for name in names], columns=["name", "age", "money", "status"])
    executeMysql("test", "truncate table foobar")
    exportPandasToMysql(df, "test", "foobar")


def randomUpdate():
    for _ in range(100):
        rowId = int(np.random.randint(1, 101))
        status = importMysqlAsPandas("test", "select status from foobar where id = %s" % rowId).values.tolist()[0][0]
        while True:
            statusNew = int(np.random.randint(1, 9))
            if statusNew != status:
                break
        executeMysql("test", "update foobar set status = %s where id = %s" % (statusNew, rowId))
        sleep(1)


if __name__ == "__main__":
    # createFakeData()
    executeMysql("test", "update foobar set status = 1 where id = 1")
    # randomUpdate()