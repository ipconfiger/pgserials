# pgserials
use postgresql to process time serials data

The idea based on this blog:[Designing high-performance time series data tables on Amazon RDS for PostgreSQL](https://aws.amazon.com/cn/blogs/database/designing-high-performance-time-series-data-tables-on-amazon-rds-for-postgresql/) 

## Code featuers

1. based on asyncio, it's asynchronous
2. only need PostgreSQL version hight than v9.5, dot't need any other plugins or extensions be installed
3. use partision table to handle huge data
4. based on sqlalchemy async core, can work in existing projects with no pain 


## Install

    pip install pgserials

## Init bucket

```python

BUCKET = Bucket('netspeed')

engine = create_async_engine(
        "postgresql+asyncpg://alexander:@127.0.0.1/brin_test", echo=True,
    )

async with engine.connect() as conn:
    await Client(conn).regist_buckets(BUCKET).async_init()

```

pgserials create partition table ranged by time, it's default create partition table by month. async_init method create partition table current month, so you can use it immediately

you can create partition table by hand like 

```python
async with engine.connect() as conn:
    await Client(conn).regist_buckets(BUCKET).extends_table(year, month)

```

## Insert data

```python
async with engine.connect() as conn:
    client = Client(conn).regist_buckets(BUCKET)
    ts = int(int(time.time() * 1000))
    for i in range(10000):
        for tag in range(10):
            ts += 1000
            await client.insert(BUCKET.name, ts, tag, ping=random.randint(10, 1000), delay=random.randint(50, 500))
            await conn.commit()
    print('insert done')
```

## Query data

### A. Direct Query a range of data

```python
async with engine.connect() as conn:
    client = Client(conn).regist_buckets(BUCKET)
    resp = await client.query(
        Query(BUCKET.name).range(start='-5m')
    )
    print(resp)
```

range method specify two keyword arguments, start and stop, you can directly set timestamp such as  int(time.time() * 1000), or string for human reading: 5s or 5sec or 5seconds, 10minute, 10m 10min also ok, prefix subtract mean???s time be reduced by current timestamp.

you can specify fields to filter labels
```python
async with engine.connect() as conn:
    client = Client(conn).regist_buckets(BUCKET)
    resp = await client.query(
        Query(BUCKET.name).range(start='-5m').fields('delay')
    )
    print(resp)
```

if you don't specify keyword argument response_type, pgserials will return database record directly.
response_type have 3 options: csvResponse/arrayResponse/ndarrayResponse

1. csvResponse will return a csv file string
2. arrayResponse will return a nested List
3. ndarrayResponse will return a named numpy ndarray

### B. Aggregate function to group query result

```python
async with engine.connect() as conn:
    client = Client(conn).regist_buckets(BUCKET)
    resp = await client.query(
        Query(BUCKET.name).range(start='-5m').window(Query.minuteWindow, Query.maxFunc)
    )
    print(resp)
```

use window method to specify time range to group by,and Aggregate function to apply in data

Options of time range

1. Query.minuteWindow
2. Query.secondWindow
3. Query.minuteWindow
4. Query.hourWindow
5. Query.dayWindow
6. Query.weekWindow

Options of Aggregate function

1. Query.countFunc
2. Query.sumFunc
3. Query.maxFunc
4. Query.minFunc
5. Query.avgFunc


