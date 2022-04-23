import asyncio
import random
import time

from sqlalchemy.ext.asyncio import create_async_engine

from pgserials import Client, Bucket, Query

BUCKET = Bucket('netspeed')


def get_conn():
    engine = create_async_engine(
        "postgresql+asyncpg://alex:@127.0.0.1/brin_test", echo=True,
    )
    return engine.connect()


async def init_buckets():
    async with get_conn() as conn:
        await Client(conn).regist_buckets(BUCKET).async_init()


async def insert_data():
    async with get_conn() as conn:
        client = Client(conn).regist_buckets(BUCKET)
        ts = int(int(time.time() * 1000))
        for i in range(10000):
            for tag in range(10):
                ts += 1000
                await client.insert(BUCKET.name, ts, tag, ping=random.randint(10, 1000), delay=random.randint(50, 500))
                await conn.commit()
        print('insert done')
        client.


async def query_data():
    async with get_conn() as conn:
        client = Client(conn).regist_buckets(BUCKET)
        csv_resp = await client.query(Query(BUCKET.name).range(start='-5m').fields('ping'), response_type=Query.csvResponse)
        print(csv_resp)


async def group_date():
    async with get_conn() as conn:
        client = Client(conn).regist_buckets(BUCKET)
        npdata = await client.query(Query(BUCKET.name).range(start='-5m').window(Query.minuteWindow, Query.maxFunc),
                                    response_type=Query.ndarrayResponse)
        print(npdata["_val"])


def main():
    while True:
        print('1: init bucket')
        print('2: insert data')
        print('3: query data')
        print('4: group data')
        print('q: quit')
        option = input()
        if option == "1":
            asyncio.run(init_buckets())
        if option == "2":
            asyncio.run(insert_data())
        if option == "3":
            asyncio.run(query_data())
        if option == "4":
            asyncio.run(group_date())
        if option == "q":
            break


if __name__ == "__main__":
    main()
