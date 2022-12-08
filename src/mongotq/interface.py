import logging
from typing import Union, List

from mongotq.task_queue import TaskQueue


def get_task_queue(database_name: str,
                   collection_name: str,
                   host: Union[str, List[str]],
                   ttl: int) -> TaskQueue:
    """
    Returns a TaskQueue instance for the given collection name.
    If the TaskQueue collection does not exist, a new MongoDB collection
    will be created.

    :return: a TaskQueue instance
    """
    queue = TaskQueue(
        database=database_name,
        collection=collection_name,
        host=host,
        ttl=ttl,
    )
    collection_names = queue.database.list_collection_names()
    # Creates a new collection and an index
    if collection_name not in collection_names:
        queue.database.create_collection(name=collection_name)
        queue.create_index()
        logging.info(f'TaskQueue collection created: {collection_name}')
    return queue
