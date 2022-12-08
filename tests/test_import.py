import unittest


class TestImport(unittest.TestCase):

    def test_import_modules(self):
        import importlib

        self.assertIsNotNone(importlib.import_module('mongotq'))
        self.assertIsNotNone(importlib.import_module('mongotq.task_queue'))
        self.assertIsNotNone(importlib.import_module('mongotq.task'))
        self.assertIsNotNone(importlib.import_module('mongotq.interface'))
        self.assertIsNotNone(importlib.import_module('mongotq.anomalies'))

    def test_import_classes(self):
        from mongotq import NonPendingAssignedAnomaly
        from mongotq import TaskQueue
        from mongotq import Task

        self.assertIsNotNone(NonPendingAssignedAnomaly)
        self.assertIsNotNone(TaskQueue)
        self.assertIsNotNone(Task)

    def test_import_attributes(self):
        from mongotq import STATUS_NEW, STATUS_PENDING, STATUS_FAILED, \
            STATUS_SUCCESSFUL

        self.assertIsNotNone(STATUS_NEW)
        self.assertIsNotNone(STATUS_PENDING)
        self.assertIsNotNone(STATUS_FAILED)
        self.assertIsNotNone(STATUS_SUCCESSFUL)
