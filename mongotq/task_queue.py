import datetime
import logging
import os
from collections.abc import Mapping, MutableMapping
from functools import cached_property
from pprint import pprint
from typing import Any, Union, List, Dict, Iterable, Generator

import pandas
from pymongo import MongoClient, ASCENDING, DESCENDING
from pymongo.collection import Collection, ReturnDocument
from pymongo.database import Database
from pymongo.results import InvalidOperation

from .anomalies import NonPendingAssignedAnomaly
from .interface import get_mongo_client
from .task import (Task, STATUS_NEW, STATUS_PENDING,
                   STATUS_FAILED, STATUS_SUCCESSFUL)


class TaskQueue:
    """A queueing data structure on top of a MongoDB collection"""

    def __init__(
            self,
            database: str,
            collection: str,
            host: Union[str, List[str]],
            tag: str = None,
            ttl: int = -1,
            max_retries: int = 3,
            discard_strategy: str = 'keep',
    ):
        """
        Instantiates a TaskQueue object using the provided MongoDB collection.

        :param database: MongoDB database name where the TaskQueue collection
                         is located at.
        :param collection: MongoDB collection name which acts as
                           the queue data structure.
        :param host: MongoDB instance hostname or a qualified URI.
                     Multiple hosts can be provided as a list.
        :param tag: consumer process tag by which tasks are assigned with.
        :param ttl: timeout in seconds to expire tasks.
                    Expired tasks are defined as tasks which has remained in
                    the `STATUS_PENDING` status for more than `self.ttl`
                    amount of time (ephemeral queue). `self.ttl=-1`
                    indicates that the TaskQueue never expires the
                    tasks (permanent queue).
        :param max_retries: maximum number of retries before discarding a Task.
        :param discard_strategy: whether to "keep" or to "remove" discarded
                                 tasks. Discarded tasks are defined as tasks
                                 which has reached the `self.max_retries`
                                 times of failure. The default strategy is
                                 to keep discarded tasks in the TaskQueue.
        """
        self.database_name = database
        self.collection_name = collection
        self.host = host
        self.tag = tag
        self.ttl = ttl
        self.max_retries = max_retries
        self.discard_strategy = discard_strategy

        self._mongo_client = None

    def __repr__(self):
        return f'TaskQueue collection name: {self.collection_name}'

    def _get_mongo_client(self) -> MongoClient:
        """
        Returns a MongoDB client instance.

        :return: MongoDB client instance
        """
        if self._mongo_client is None:
            self._mongo_client = get_mongo_client(self.host)
        return self._mongo_client

    @cached_property
    def database(self) -> Database:
        client = self._get_mongo_client()
        return client.get_database(self.database_name)

    @cached_property
    def collection(self) -> Collection:
        return self.database.get_collection(self.collection_name)

    @cached_property
    def assignment_tag(self) -> str:
        return self.tag or f'consumer_{os.getpid()}'

    @cached_property
    def expires(self) -> bool:
        return self.ttl != -1

    def size(self) -> int:
        """
        Returns size of the TaskQueue by counting the number of documents
        in the collection.

        :return: document count
        """
        return self.collection.count_documents({})

    def _expire_tasks(self, force: bool = False) -> None:
        """
        Updates the pending tasks which are already expired. Expired tasks
        are defined as tasks which has remained in the `STATUS_PENDING` status
        for more than `self.ttl` amount of time (ephemeral queue).

        :param force: whether to act even the TaskQueue never expires tasks.
        :return:
        """
        if self.expires or force:
            now = datetime.datetime.now()
            ttl_ago = now - datetime.timedelta(seconds=self.ttl)
            update_result = self.collection.update_many(
                filter={'assignedTo': {'$ne': None},
                        'modifiedAt': {'$lt': ttl_ago.timestamp()},
                        'status': {'$eq': STATUS_PENDING}},
                update={'$set': {'assignedTo': None,
                                 'modifiedAt': now.timestamp(),
                                 'status': STATUS_FAILED},
                        '$inc': {'retries': 1}},
            )
            if not update_result.acknowledged:
                raise InvalidOperation
            if not update_result.matched_count:
                logging.info('no expired tasks found')
            else:
                logging.info(f'updated {update_result.modified_count} out of '
                             f'{update_result.matched_count} expired task(s).')

    def _discard_tasks(self) -> None:
        """
        Deletes the failed tasks which has reached (or exceeded) the
        `self.max_retries` limit. Discarded tasks are defined as tasks which
        has reached the `self.max_retries` times of failure.

        :return:
        """
        delete_result = self.collection.delete_many(
            filter={'retries': {'$gte': self.max_retries}},
        )
        if not delete_result.acknowledged:
            raise InvalidOperation
        if not delete_result.deleted_count:
            logging.info('no discardable tasks found')
        else:
            logging.info(f'deleted {delete_result.deleted_count} '
                         'failed task(s).')

    def resolve_anomalies(self, dry_run: bool = True) -> None:
        """
        Updates tasks deviating from the defined lifecycle stages.
        Task anomalies could happen due to manual manipulations of
        the underlying MongoDB collection, invoking low-level methods of the
        TaskQueue out of the defined lifecycle, or any unexpected behaviors
        occurred at the backend. Found tasks with anomalies are logged as
        warnings and if `dy_run=False`, the necessary changes will be
        committed to resolve the anomalies.

        :param dry_run: whether to resolve the found anomalies or just
                        log them as warnings without updating the tasks
        :return:
        """
        now_timestamp = datetime.datetime.now().timestamp()
        anomalies = [NonPendingAssignedAnomaly(now_timestamp)]
        for anomaly in anomalies:
            anomaly_name = anomaly.__class__.__name__

            cur = self.collection.find(filter=anomaly.filter)
            if cur is None or not (docs := list(cur)):
                logging.info(f'no {anomaly_name} found')
                continue
            logging.warning(f'{len(docs)} anomalies found:')
            for doc in docs:
                pprint(self._wrap_task(doc))

            # Commit necessary changes
            if not dry_run:
                update_result = self.collection.update_many(
                    filter=anomaly.filter,
                    update=anomaly.update
                )
                if not update_result.acknowledged:
                    raise InvalidOperation
                if not update_result.matched_count:
                    logging.warning(f'no {anomaly_name} found, false alarm?!')
                logging.info(f'fixed {update_result.modified_count} out of '
                             f'{update_result.matched_count} expired tasks.')

    def refresh(self) -> None:
        """
        Update the expired and discarded tasks.
        Expired tasks are defined as tasks which has remained in
        the `STATUS_PENDING` status for more than `self.ttl`
        amount of time (ephemeral queue).
        Discarded tasks are defined as tasks which has reached
        the `self.max_retries` times of failure.

        :return:
        """
        self._expire_tasks()
        self._discard_tasks()

    def append(self, payload: Any, priority: int = 0) -> None:
        """
        Inserts a Task into the TaskQueue.

        :param payload: Task payload
        :param priority: Task priority
        :return:
        """
        insert_result = self.collection.insert_one(
            document=Task(priority=priority, payload=payload),
        )
        if not insert_result.acknowledged:
            raise InvalidOperation

    def pop(self) -> Task:
        """
        Removes a completed Task from the TaskQueue and returns it. The removed
        Task must be in `STATUS_SUCCESSFUL` status.

        :return: the removed Task
        """
        doc = self.collection.find_one_and_delete(
            filter={'status': STATUS_SUCCESSFUL},
            sort=[('priority', DESCENDING),
                  ('createdAt', ASCENDING)],
        )
        return self._wrap_task(doc)

    def bulk_append(self, tasks: Iterable[Task], ordered=True):
        insert_result = self.collection.insert_many(documents=tasks,
                                                    ordered=ordered)
        if not insert_result.acknowledged:
            raise InvalidOperation

    def update(self, task: Task) -> None:
        """
        Updates the given Task's attributes in the TaskQueue.

        :param task: the Task object that the original Task needs
                     to be updated with
        :return:
        """
        task.modifiedAt = datetime.datetime.now().timestamp()
        update_result = self.collection.update_one(
            filter={'_id': task.object_id_},
            update={'$set': task},
            upsert=True,
        )
        if not update_result.acknowledged:
            raise InvalidOperation
        if update_result.upserted_id:
            logging.debug('Task upserted!')

    def next(self) -> Union[Task, None]:
        """
        Gets the next unassigned Task from the TaskQueue and returns
        a wrapped `Task` object of it.

        :return: the next Task at the front of the TaskQueue
        """
        doc = self.collection.find_one_and_update(
            filter={'assignedTo': None,
                    'status': STATUS_NEW,
                    'retries': {'$lt': self.max_retries}},
            update={'$set': {'assignedTo': self.assignment_tag,
                             'modifiedAt': datetime.datetime.now().timestamp(),
                             'status': STATUS_PENDING}},
            sort=[('priority', DESCENDING),
                  ('createdAt', ASCENDING)],
            return_document=ReturnDocument.AFTER,
        )
        return self._wrap_task(doc)

    def next_many(self, count: int = 0):
        cur = self.collection.find(
            filter={'assignedTo': None,
                    'status': STATUS_NEW,
                    'retries': {'$lt': self.max_retries}},
            sort=[('priority', DESCENDING),
                  ('createdAt', ASCENDING)],
            limit=count
        )
        tasks = [self._wrap_task(c) for c in cur]
        update_result = self.collection.update_many(
            filter={'_id': {'$in': [t['_id'] for t in tasks]}},
            update={'$set': {'assignedTo': self.assignment_tag,
                             'modifiedAt': datetime.datetime.now().timestamp(),
                             'status': STATUS_PENDING}},
            upsert=False,
        )
        if not update_result.acknowledged:
            raise InvalidOperation
        if not update_result.matched_count:
            logging.error('no tasks found to update')
        if update_result.matched_count != len(tasks):
            logging.error('not all tasks are updated!')
        return tasks

    def head(self, n: int = 10) -> None:
        """
        Prints the n first tasks at the front of the TaskQueue.
        The front of a queue, by definition, is the end that items are removed
        from.

        :param n: number of tasks to be returned
        :return:
        """
        cur = self.collection.find(
            sort=[('priority', DESCENDING),
                  ('createdAt', ASCENDING)],
        )
        if cur is None:
            return
        for doc in cur[:n]:
            pprint(self._wrap_task(doc))
        print(f'{self.size():,} Task(s) available in the TaskQueue')

    def tail(self, n: int = 10) -> None:
        """
        Prints the last n tasks at the back of the TaskQueue.
        The back of a queue, by definition, is the end that items are inserted
        at.

        :param n: number of to be returned
        :return:
        """
        cur = self.collection.find(
            sort=[('priority', DESCENDING),
                  ('createdAt', DESCENDING)]
        )
        if cur is None:
            return
        for doc in reversed(list((cur[:n]))):
            pprint(self._wrap_task(doc))
        print(f'{self.size():,} Task(s) available in the TaskQueue')

    def status_info(self) -> None:
        cur = self.collection.aggregate([{
            '$group': {'_id': '$status',
                       'count': {'$sum': 1}}
        }])
        stats = {c['_id']: c['count'] for c in cur}
        total = sum(stats.values())
        m_len = len(str(total))
        padding = m_len + m_len // 3
        assert total == self.size()

        print('Tasks status:')
        print(f'Status'.ljust(15), 'Count'.ljust(padding))
        print(f'{"-" * 10}'.ljust(15), f'{"-" * padding}'.ljust(padding))
        for status, count in sorted(stats.items()):
            print(f'{status}'.ljust(15), f'{count:,}'.rjust(padding))
        print()
        print(f'Total:'.ljust(15), f'{total:,}'.rjust(padding))

    def delete_iter(self, tasks: List[Task]):
        delete_result = self.collection.delete_many(
            filter={'_id': {'$in': [t['_id'] for t in tasks]}},
        )
        if not delete_result.acknowledged:
            raise InvalidOperation

    def generate_loc(self, *args, **kwargs) -> Generator:
        cur = self.collection.find(*args, **kwargs)
        for c in cur:
            yield self._wrap_task(c)

    def loc(self, *args, **kwargs) -> List[Task]:
        return list(self.generate_loc(*args, **kwargs))

    def to_list(self) -> List[Dict[str, Any]]:
        """
        Returns a list of the Tasks available in the TaskQueue.

        :return: the list of Tasks
        """
        cur = self.collection.find()
        return list(cur) if cur else []

    def to_dataframe(self):
        """
        Returns a DataFrame of the Tasks available in the TaskQueue.

        :return: the DataFrame of Tasks
        """
        tasks = self.to_list()
        return pandas.DataFrame(tasks)

    # Task lifecycle
    def on_success(self, task: Task) -> None:
        """
        Task has been successfully executed.

        :param task: the successful Task
        :return:
        """
        task.assignedTo = None
        task.modifiedAt = datetime.datetime.now().timestamp()
        task.status = STATUS_SUCCESSFUL
        self.update(task)

    def on_failure(self, task: Task, error_message: str = None) -> None:
        """
        Task execution resulted in failure.

        :param task: the failed Task
        :param error_message: the error message to be stored with the Task
        :return:
        """
        task.assignedTo = None
        task.modifiedAt = datetime.datetime.now().timestamp()
        task.status = STATUS_FAILED
        task.errorMessage = error_message
        task.retries += 1
        self.update(task)

    def on_retry(self, task: Task) -> None:
        """
        Task is released and its state gets updated.

        :param task: the Task that needs to be retried.
        :return:
        """
        return self.collection.find_one_and_update(
            filter={'_id': task.object_id_,
                    'assignedTo': self.assignment_tag},
            update={'$set': {'assignedTo': None, 'modifiedAt': None},
                    '$inc': {'retries': 1}}
        )

    def create_index(self) -> str:
        """
        Creates a MongoDB compound index of query keys.

        :return: name of the created index
        """
        return self.collection.create_index(
            keys=[('_id', ASCENDING),
                  ('assignedTo', ASCENDING),
                  ('modifiedAt', ASCENDING)],
            background=True,
        )

    @staticmethod
    def _wrap_task(doc: Union[Mapping, MutableMapping]) -> Union[Task, None]:
        """
        Creates a Task object from the given document mapping.

        :param doc: MongoDB document
        :return: the created Task
        """
        if not doc or \
                not isinstance(doc, (Mapping, MutableMapping)):
            logging.debug('invalid Task')
            return None
        return Task(**doc)

    def _clear(self) -> None:
        """
        Clears out the Tasks from the TaskQueue by removing the collection from
        MongoDB.
        Should be run with caution!

        :return:
        """
        delete_result = self.collection.delete_many(
            filter={'assignedTo': {'$exists': True},
                    'createdAt': {'$exists': True},
                    'modifiedAt': {'$exists': True},
                    'status': {'$exists': True},
                    'errorMessage': {'$exists': True},
                    'retries': {'$exists': True},
                    'priority': {'$exists': True}},
        )
        if not delete_result.acknowledged:
            raise InvalidOperation
