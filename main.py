import time

from canal.client import Client
from canal.protocol import EntryProtocol_pb2
from mysqlUtils import IP

client = Client()
client.connect(host=IP)
client.check_valid()
client.subscribe()

while True:
    message = client.get(100)
    entries = message['entries']
    for entry in entries:
        entry_type = entry.entryType
        if entry_type in [EntryProtocol_pb2.EntryType.TRANSACTIONBEGIN, EntryProtocol_pb2.EntryType.TRANSACTIONEND]:
            continue
        row_change = EntryProtocol_pb2.RowChange()
        row_change.MergeFromString(entry.storeValue)
        event_type = row_change.eventType
        header = entry.header
        database = header.schemaName
        table = header.tableName
        for row in row_change.rowDatas:
            format_data = dict()
            if event_type == EntryProtocol_pb2.EventType.DELETE:
                for column in row.beforeColumns:
                    format_data = {
                        column.name: column.value
                    }
            elif event_type == EntryProtocol_pb2.EventType.INSERT:
                for column in row.afterColumns:
                    format_data = {
                        column.name: column.value
                    }
            else:
                format_data['before'] = format_data['after'] = dict()
                for column in row.beforeColumns:
                    format_data['before'][column.name] = column.value
                for column in row.afterColumns:
                    format_data['after'][column.name] = column.value
            data = dict(
                db=database,
                table=table,
                event_type=event_type,
                data=format_data,
            )
            print(data)
    time.sleep(1)

# client.disconnect()