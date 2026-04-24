import asyncio
import uuid
from asyncio import Future
from datetime import datetime, timedelta, UTC
from typing import Any

from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
from loguru import logger

from cassandra_2026.systems.filez.common import execute_async_awaitable
from cassandra_2026.systems.filez.model import StoredFile





def get_cluster_session():
    # 1. Connect to Cassandra
    # 1.1 Connect to the 3-node Cassandra cluster on port 9044
    contact_points = ['10.10.1.231', '10.10.1.232', '10.10.1.233']

    # Initialize the cluster with the nodes and the custom port
    # cluster = Cluster(['127.0.0.1'])
    cluster = Cluster(
        contact_points=contact_points,
        port=9044,
        # Optional but recommended: ensures your queries distribute properly
        # load_balancing_policy=DCAwareRoundRobinPolicy(local_dc='dc1')
    )

    KS = 'filez'  # hide in .env
    session = cluster.connect(f'{KS}')
    return cluster, session

async def main():
    cluster, session = get_cluster_session()

    # Prepare statements for better performance
    insert_stmt = session.prepare(
        "INSERT INTO files (file_id, author_id, filename, created_at, content) VALUES (?, ?, ?, ?, ?)"
    )
    get_by_id_stmt = session.prepare("SELECT * FROM files WHERE file_id = ?")
    get_by_author_stmt = session.prepare("SELECT * FROM files WHERE author_id = ?")
    # Note: range queries with SAI can be prepared too
    get_by_time_range_stmt = session.prepare(
        "SELECT * FROM files WHERE created_at >= ? AND created_at <= ?"
    )

    # 2. Example Data
    file_id = uuid.uuid4()
    author_id = uuid.uuid4()
    now = datetime.now(UTC)
    file_content = b"Hello, Cassandra 5.0 with SAI!!"

    # 3. Store a File (Async Insert)
    print("Storing file...")
    await execute_async_awaitable(
        session,
        insert_stmt,
        (file_id, author_id, "example.txt", now, file_content)
    )

    # 4. Query 1: Retrieve by file_id (Primary Key)
    print("\nRetrieving by file_id:")
    result_id = await execute_async_awaitable(session, get_by_id_stmt, (file_id,))
    for row in result_id:
        print(f"Found: {row.filename} by {row.author_id}")

    # 5. Query 2: Retrieve by author_id (Using SAI)
    print("\nRetrieving by author_id:")
    result_author = await execute_async_awaitable(session, get_by_author_stmt, (author_id,))
    for row in result_author:
        print(f"Found: {row.filename} created at {row.created_at}")

    # 6. Query 3: Retrieve by created_at range (Using SAI)
    print("\nRetrieving by time range:")
    start_time = now - timedelta(hours=10)
    end_time = now + timedelta(hours=1)

    result_range = await execute_async_awaitable(
        session,
        get_by_time_range_stmt,
        (start_time, end_time)
    )
    for row in result_range:
        print(f"Found in range: {row.filename} (ID: {row.file_id})")
        file = StoredFile(file_id=row.file_id,
                          author_id=row.author_id,
                          filename=row.filename,
                          content=row.content,
                          created_at=row.created_at, )
        print(file)

    cluster.shutdown()


if __name__ == "__main__":
    asyncio.run(main())