# Mongo TaskQueue
[![Python 3.6](https://img.shields.io/badge/python-3.8%20%7C%203.9%20%7C%203.10%20%7C%203.11-blue.svg)](https://pypi.org/project/mongo-taskqueue)
[![PyPI version](https://badge.fury.io/py/mongo-taskqueue.svg)](https://badge.fury.io/py/mongo-taskqueue)
![MongoDB](https://img.shields.io/badge/MongoDB-%234ea94b.svg?style=flat&logo=mongodb&logoColor=white)

Mongo-TaskQueue is a queueing data structure built on top of a MongoDB 
collection.

## Quick start
Just create an instance of TaskQueue and start to append to your queue:
```python
>>> from mongotq import get_task_queue

>>> task_queue = get_task_queue(
        database_name=YOUR_MONGO_DATABASE_NAME,
        collection_name='queueGeocoding',
        host=YOUR_MONGO_DB_URI,
        ttl=-1  # permanent queue
    )
>>> task_queue.append({'species': 'Great Horned Owl'})
>>> task_queue.append({'species': 'Eastern Screech Owl'})
>>> task_queue.append({'species': 'Northern Saw-whet Owl'})
>>> task_queue.append({'species': 'Snowy Owl'})
>>> task_queue.append({'species': 'Whiskered Screech Owl'})
```

Then you can simply check the tail of your queue:
```python
>>> task_queue.tail(n=5)
```
```python
{'_id': ObjectId('6392375588c63227371c693c'),
 'assignedTo': None,
 'createdAt': datetime.datetime(2022, 12, 8, 14, 13, 25, 99685),
 'errorMessage': None,
 'modifiedAt': datetime.datetime(2022, 12, 8, 14, 13, 25, 99685),
 'payload': {'species': 'Great Horned Owl'},
 'priority': 0,
 'retries': 0,
 'status': 'new'}
{'_id': ObjectId('6392375588c63227371c693d'),
 'assignedTo': None,
 'createdAt': datetime.datetime(2022, 12, 8, 14, 13, 25, 129570),
 'errorMessage': None,
 'modifiedAt': datetime.datetime(2022, 12, 8, 14, 13, 25, 129570),
 'payload': {'species': 'Eastern Screech Owl'},
 'priority': 0,
 'retries': 0,
 'status': 'new'}
{'_id': ObjectId('6392375588c63227371c693e'),
 'assignedTo': None,
 'createdAt': datetime.datetime(2022, 12, 8, 14, 13, 25, 155404),
 'errorMessage': None,
 'modifiedAt': datetime.datetime(2022, 12, 8, 14, 13, 25, 155404),
 'payload': {'species': 'Northern Saw-whet Owl'},
 'priority': 0,
 'retries': 0,
 'status': 'new'}
{'_id': ObjectId('6392375588c63227371c693f'),
 'assignedTo': None,
 'createdAt': datetime.datetime(2022, 12, 8, 14, 13, 25, 179804),
 'errorMessage': None,
 'modifiedAt': datetime.datetime(2022, 12, 8, 14, 13, 25, 179804),
 'payload': {'species': 'Snowy Owl'},
 'priority': 0,
 'retries': 0,
 'status': 'new'}
{'_id': ObjectId('6392375588c63227371c6940'),
 'assignedTo': None,
 'createdAt': datetime.datetime(2022, 12, 8, 14, 13, 25, 204284),
 'errorMessage': None,
 'modifiedAt': datetime.datetime(2022, 12, 8, 14, 13, 25, 204284),
 'payload': {'species': 'Whiskered Screech Owl'},
 'priority': 0,
 'retries': 0,
 'status': 'new'}
5 Task(s) available in the TaskQueue
```


## Installation
The only dependency is [pyMongo](https://pymongo.readthedocs.io/en/stable/).
The easiest way to install Mongo-TaskQueue is using `pip`:
```bash
$ pip install mongo-taskqueue
```