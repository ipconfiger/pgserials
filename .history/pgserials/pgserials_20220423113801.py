# coding: utf-8

import json
import datetime
from dataclasses import dataclass

from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.sql import text

from .exceptions import DefinationError
from .utils import csv_val, generate_timestamp, pre_process_val


class Query:
    secondWindow: str = 's'
    minuteWindow: str = 'm'
    hourWindow: str = 'h'
    dayWindow: str = 'd'
    weekWindow: str = 'w'
    countFunc: str = 'count'
    sumFunc: str = 'sum'
    maxFunc: str = 'max'
    minFunc: str = 'min'
    avgFunc: str = 'avg'
    csvResponse: str = 'csv'
    arrayResponse: str = 'array'
    ndarrayResponse: str = 'numpy'

    def __init__(self, bucket: str):
        self.bucket = bucket
        self.start = 0
        self.stop = 0
        self.if_set_range = False
        self.filter_fields = []
        self.tag_id = -1
        self.width = ''
        self.func = ''
        self.params = {}
        self.headers = []

    def range(self, start='', stop='') -> "Query":
        self.start = start if isinstance(start, int) else generate_timestamp(start)
        self.stop = stop if isinstance(stop, int) else generate_timestamp(stop)
        self.if_set_range = True
        return self

    def fields(self, *fields) -> "Query":
        del self.filter_fields[:]
        for f in fields:
            self.filter_fields.append(f)
        if len(self.filter_fields) == 1:
            self.params['label'] = fields[0]
        return self

    def tag(self, tag_id) -> "Qeury":
        self.tag_id = tag_id
        self.params['tag_id'] = tag_id
        return self

    def window(self, window_width: str, func: str) -> "Query":
        self.width = window_width
        self.func = func
        return self

    def generate_sql(self):
        if not self.if_set_range:
            raise DefinationError('Must specify data range')
        rows = ['SELECT ']
        select_fields = []
        if self.width:
            if self.width not in [Query.secondWindow, Query.minuteWindow, Query.hourWindow, Query.dayWindow,
                                  Query.weekWindow]:
                raise DefinationError('Invalid window width:%s' % self.width)
            if self.func not in [Query.countFunc, Query.sumFunc, Query.maxFunc, Query.minFunc, Query.avgFunc]:
                raise DefinationError('Invalid window width:%s' % self.width)
            if self.width == Query.secondWindow:
                select_fields.append("TO_CHAR(TO_TIMESTAMP(ts / 1000), 'DD/MM/YYYY HH24:MI:SS') as date")
            if self.width == Query.minuteWindow:
                select_fields.append("TO_CHAR(TO_TIMESTAMP(ts / 1000), 'DD/MM/YYYY HH24:MI') as date")
            if self.width == Query.hourWindow:
                select_fields.append("TO_CHAR(TO_TIMESTAMP(ts / 1000), 'DD/MM/YYYY HH24') as date")
            if self.width == Query.dayWindow:
                select_fields.append("TO_CHAR(TO_TIMESTAMP(ts / 1000), 'DD/MM/YYYY') as date")
            if self.width == Query.weekWindow:
                select_fields.append("date_trunc('week', TO_TIMESTAMP(ts / 1000)) as date")
            self.headers.append('date')
            if self.tag_id == -1:
                select_fields.append('tag_id')
                self.headers.append('tag')
            if len(self.filter_fields) != 1:
                select_fields.append('label')
                self.headers.append('label')
            select_fields.append(f'{self.func}(val) as val')
            self.headers.append('val')
            rows.append(" , ".join(select_fields))
        else:
            select_fields.append('ts')
            self.headers.append('ts')
            select_fields.append('tag_id')
            self.headers.append('tag')
            if len(select_fields) != 1:
                select_fields.append('label')
                self.headers.append('label')
            select_fields.append('val')
            self.headers.append('val')
            rows.append(" , ".join(select_fields))
        rows.append(f' FROM {self.bucket} WHERE ts BETWEEN {self.start} AND {self.stop}')
        if self.tag_id > -1:
            rows.append(f" AND tag_id=:tag_id")
        if self.filter_fields:
            if len(self.filter_fields) == 1:
                rows.append(f" AND label=:label")
            else:
                fields_str = [f"'{f}'" for f in self.filter_fields]
                rows.append(f" AND label in ({','.join(fields_str)})")
        if self.width:
            rows.append(" GROUP BY date")
            if self.tag_id == -1:
                rows.append(", tag_id")
            if len(self.filter_fields) != 1:
                rows.append(", label")
        return "".join(rows)

    def toStatement(self):
        return text(self.generate_sql())


@dataclass
class Bucket:
    name: str

    def create_table_sql(self):
        return """CREATE TABLE %s (
    ts  bigint,
    tag_id  integer,
    label   char(36),
    val real
) PARTITION BY RANGE(ts);""" % self.name

    def create_index_sql(self):
        return f"""CREATE INDEX _{self.name}_time_brin_idx ON {self.name} 
     USING BRIN (ts) WITH (pages_per_range = 32);"""

    def main_table_exists_sql(self):
        return f"""select 1 from information_schema.tables 
    where table_schema = 'public' and table_name = '{self.name}';"""

    def partition_tableExists_sql(self, year: int, month: int):
        table_name = f"{self.name}_{year}_{month}"
        return f"""select 1 from information_schema.tables where table_schema = 'public' and table_name = '{table_name}';"""

    def create_partition_table_sql(self, year: int, month: int):
        table_name = f"{self.name}_{year}_{month}"
        start_ts = datetime.datetime(year=year, month=month, day=1, hour=0, minute=0, second=0,
                                     microsecond=0).timestamp() * 1000
        end_ts = datetime.datetime(year=year if month < 12 else year + 1, month=month + 1 if month < 12 else 1, day=1,
                                   hour=0, minute=0, second=0, microsecond=0).timestamp() * 1000
        return f"""CREATE TABLE {table_name} 
  PARTITION OF {self.name}
    FOR VALUES FROM ({int(start_ts)}::bigint) 
                 TO ({int(end_ts)}::bigint);"""

    def insert_sql(self):
        return f'INSERT INTO {self.name} ("ts", "tag_id", "label", "val") VALUES (:ts, :tag, :label, :val)'


class Client:
    def __init__(self, db: AsyncSession):
        self.db = db
        self.buckets = []
        self.buckets_dict = {}

    def regist_buckets(self, *buckets):
        for b in buckets:
            self.buckets.append(b)
            self.buckets_dict[b.name] = b
        return self

    async def async_init(self):
        for idx, b in enumerate(self.buckets):
            bucket: Bucket = b
            bucket.tag_id = idx
            rs = await self.db.execute(text(bucket.main_table_exists_sql()))
            if rs.first():
                pass
            else:
                await self.db.execute(text(bucket.create_table_sql()))
                today = datetime.datetime.now()
                await self._extends_table(bucket, today.year, today.month)
                await self.db.execute(text(bucket.create_index_sql()))
        await self.db.commit()

    async def insert(self, bucket_name: str, timestamp: int, tag_id: int, **values):
        bucket: Bucket = self.buckets_dict.get(bucket_name)
        statement = text(bucket.insert_sql())
        for label, val in values.items():
            params = dict(ts=timestamp, tag=tag_id, label=label, val=val)
            await self.db.execute(statement, params)

    def groupResult(self, results):
        labels = {}
        arrays = []
        template_label_vals = json.dumps([''] * 1000)
        label_vals = []
        key = ''
        tag_id = -1
        for row in results:
            ts = row[0]
            _tag_id = row[1]
            label = row[2].strip()
            val = row[3]
            if key != ts:
                if key != '':
                    arrays.append([key, tag_id] + label_vals)
                key = ts
                tag_id = _tag_id
                labels[label] = 0
                label_vals = json.loads(template_label_vals)
                label_vals[0] = val
            else:
                if label not in labels:
                    idx = len(labels.keys())
                    labels[label] = idx
                    label_vals[idx] = val
                else:
                    idx = labels[label]
                    label_vals[idx] = val
        else:
            arrays.append([key, tag_id] + label_vals)
        headers = ['ts', 'tag_id'] + [k for k, v in sorted(labels.items(), key=lambda item: item[1])]
        arrays.insert(0, headers)
        return [r[:len(headers)] for r in arrays]

    def formatCSV(self, query: Query, results):
        arrays = self.formatArray(query, results)
        return "\n".join([",".join(map(csv_val, r)) for r in arrays])

    def formatArray(self, query: Query, results):
        if not query.width:
            arrays = self.groupResult(results)
        else:
            headers = ['date']
            if query.tag_id == -1:
                headers.append('tag')
            if len(query.filter_fields) != 1:
                headers.append('_label')
            headers.append('_val')
            arrays = []
            arrays.append(headers)
            for r in results:
                arrays.append(list([pre_process_val(c) for c in r]))
        return arrays

    def formatNumpy(self, query: Query, results):
        import numpy as np
        dtype = []
        array = self.formatArray(query, results)
        for header in array[0]:
            if header == "ts":
                dtype.append((header, 'i8'))
                continue
            if header == "date":
                dtype.append((header, 'S20'))
                continue
            if header == "tag":
                dtype.append((header, 'i4'))
                continue
            if header == 'label':
                dtype.append(('_label', 'S20'))
                continue
            if header == '_label':
                dtype.append(('_label', 'S20'))
                continue
            dtype.append((header, 'f8'))
        return np.array([tuple(r) for r in array[1:]], dtype=dtype)

    async def query(self, query: Query, response_type: str = ''):
        statement = query.toStatement()
        results = await self.db.execute(statement, query.params)
        if response_type == Query.csvResponse:
            return self.formatCSV(query, results)
        if response_type == Query.arrayResponse:
            return self.formatArray(query, results)
        if response_type == Query.ndarrayResponse:
            return self.formatNumpy(query, results)
        return [r for r in results]


    async def _extends_table(self, bucket: Bucket, year: int, month: int):
        rs = await self.db.execute(text(bucket.partition_tableExists_sql(year, month)))
        if rs.first():
            pass
        else:
            await self.db.execute(text(bucket.create_partition_table_sql(year, month)))

    async def extends_table(self, bucket: str, year: int, month: int):
        bucket = self.buckets_dict.get(bucket)
        await self._extends_table(bucket, year, month)


