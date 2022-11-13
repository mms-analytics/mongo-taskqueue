from abc import ABC, abstractmethod
from typing import Dict

from .task import STATUS_PENDING, STATUS_FAILED


class BaseAnomaly(ABC):

    @abstractmethod
    def __init__(self, filter_predicate: Dict, update_predicate: Dict):
        self._filter = filter_predicate
        self._update = update_predicate

    @property
    def filter(self):
        return self._filter

    @property
    def update(self):
        return self._update


class NonPendingAssignedAnomaly(BaseAnomaly):

    def __init__(self, timestamp: float):
        filter_predicate = {'assignedTo': {'$ne': None},
                            'status': {'$ne': STATUS_PENDING}}
        update_predicate = {'$set': {'assignedTo': None,
                                     'modifiedAt': timestamp,
                                     'status': STATUS_FAILED},
                            '$inc': {'retries': 1}}
        super(NonPendingAssignedAnomaly, self).__init__(filter_predicate,
                                                        update_predicate)
