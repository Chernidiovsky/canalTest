# -*- coding:utf-8 -*-
from canal.client import Client
from canal.protocol import EntryProtocol_pb2
from time import sleep


class ParseCanal:
    def __init__(self, ip, interval):
        """
        ip: canal服务所在ip
        interval: 每多少秒执行一次binlog抓取
        """
        self.ip = ip
        self.interval = interval

    def run(self, execution):
        """
        execution: 执行函数，要对mysql的binlog记录做何种操作，函数输入必须是rowChangeRec那样的字典
        """
        client = Client()
        client.connect(host=self.ip)
        client.check_valid()
        client.subscribe()

        while True:
            message = client.get(100)
            entries = message['entries']
            for entry in entries:
                entryType = entry.entryType
                if entryType in [EntryProtocol_pb2.EntryType.TRANSACTIONBEGIN, EntryProtocol_pb2.EntryType.TRANSACTIONEND]:
                    continue
                rowChanges = EntryProtocol_pb2.RowChange()
                rowChanges.MergeFromString(entry.storeValue)
                eventType = rowChanges.eventType
                header = entry.header
                database = header.schemaName
                table = header.tableName
                for rowChange in rowChanges.rowDatas:
                    formatData = dict()
                    if eventType == EntryProtocol_pb2.EventType.DELETE:
                        for column in rowChange.beforeColumns:
                            formatData[column.name] = column.value
                    elif eventType == EntryProtocol_pb2.EventType.INSERT:
                        for column in rowChange.afterColumns:
                            formatData[column.name] = column.value
                    else:
                        formatData['before'], formatData['after'] = dict(), dict()
                        for column in rowChange.beforeColumns:
                            formatData['before'][column.name] = column.value
                        for column in rowChange.afterColumns:
                            formatData['after'][column.name] = column.value
                    rowChangeRec = dict(
                        db=database,
                        table=table,
                        event_type=eventType,
                        data=formatData,
                    )
                    execution(rowChangeRec)
            sleep(self.interval)


if __name__ == "__main__":
    ParseCanal("10.0.18.54", 1).run(print)