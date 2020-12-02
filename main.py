# -*- coding:utf-8 -*-
from canalUtils import ParseCanal
from mysqlUtils import exportPandasToMysql
from datetime import datetime
import pandas as pd
import traceback


def statusChgHist(rowChangeRec):
    try:
        db = rowChangeRec["db"]
        table = rowChangeRec["table"]
        event_type = rowChangeRec["event_type"]
        data = rowChangeRec["data"]

        if db == "test" and table == "foobar" and event_type == 2:
            dt = datetime.today().strftime("%y-%m-%d %H:%M:%S")
            print(u"======== %s ========" % dt)
            data = data["after"]
            name, status = data["name"], data["status"]
            data = [[name, status, dt]]
            df = pd.DataFrame(data=data, columns=["name", "status", "update_time"])
            print("%s的status更新为%s" % (name, status))
            exportPandasToMysql(df, "test", "foobar_status_his")
    except:
        traceback.print_exc()


pc = ParseCanal("10.0.18.54", 1)
pc.run(statusChgHist)