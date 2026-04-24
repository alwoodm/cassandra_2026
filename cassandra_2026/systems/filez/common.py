from asyncio import Future, get_event_loop
from typing import Any


# Helper function to convert Cassandra's ResponseFuture to an asyncio Future
def execute_async_awaitable(session, query, parameters=None) -> Future[Any]:
    loop = get_event_loop()
    future = loop.create_future()

    cassandra_future = session.execute_async(query, parameters)

    def on_success(result):
        loop.call_soon_threadsafe(future.set_result, result)

    def on_error(exception):
        loop.call_soon_threadsafe(future.set_exception, exception)

    cassandra_future.add_callbacks(on_success, on_error)
    return future
