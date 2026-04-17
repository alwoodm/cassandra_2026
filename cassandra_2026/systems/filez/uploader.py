import os
from asyncio import run
from datetime import datetime
from uuid import uuid4

from cassandra.cluster import Cluster
from loguru import logger

from cassandra_2026.systems.filez.example_zero import FileRepository
from cassandra_2026.systems.filez.model import StoredFile


def save_file(file: StoredFile, dir='drive'):
    path = os.path.join(dir, file.filename)
    with open(path, 'wb') as f:
        f.write(file.content)


def load_file(filename, dir='drive'):
    path = os.path.join(dir, filename)
    with open(path, 'rb') as f:
        return StoredFile(file_id=uuid4(), author_id=uuid4(), filename=filename, content=f.read(),
                          created_at=datetime.now())

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

    repo = FileRepository(session)


    # file = StoredFile(file_id=uuid4(), author_id=uuid4(),
    #                   filename='test.txt', content=b'Hello, world!', created_at=datetime.now())

    file = load_file('iceberg.jpg')
    await repo.insert_file(file)
    logger.info(f'File saved: {file.filename}')

    zz = await repo.get_file_by_id(file.file_id)
    save_file(zz, dir='dd')

    # save_file(file)
    # ff = load_file('test.txt')
    # print(ff)

    cluster.shutdown()

if __name__ == '__main__':
    run(main())



