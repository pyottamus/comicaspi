from contextlib import contextmanager
from functools import WRAPPER_ASSIGNMENTS, WRAPPER_UPDATES, wraps, update_wrapper
from inspect import signature, Signature, _ParameterKind
from typing import Self
import sqlite3
import sys
import warnings
from enum import IntEnum

class TransFNType(IntEnum):
    TRANSACTION = 1
    TRANSACTIONALIZED = 2
    TRANSACTIONALIZED_PROPERTY = 3

class TransState(IntEnum):
    OPEN = 0,
    TRANSACTION = 1,
    SAVEPOINT = 2

#__all__ = 'transaction', 'transactable', 'Transactor', 'TransFNType', 'TransactionBase'

class TransactorContextmgr:
    def __init__(self, root: 'Transactor', parent: Self=None):
        self.root = root
        self.parent = parent
    def __enter__(self):
        return self
    def __exit__(self, exc_type, exc_value, traceback):
        assert self.root.connection.in_transaction, \
               "Transaction already closed"
        if exc_type is None and exc_value is None and traceback is None:
            self.root.commit()
        else:
            self.root.rollback()

    
            
class Transactor(sqlite3.Cursor):
    __slots__ = 'transaction'
    def __init__(self, connection):
        super().__init__(connection)
        self.transaction = None
    def __enter__(self):
        return self
    def __exit__(self, exc_type, exc_value, traceback):
        
        if self.transaction is not None:
            return self.transaction.__exit__(exc_type, exc_value, traceback)
            
    def savepoint(self):
        self.execute("SAVEPOINT")
        self.transaction = TransactorContextmgr(self, self.transaction)
        return self
    
    def begin(self):
        assert self.transaction is None, \
               "Cannot BEGIN transaction while in transaction"

        self.execute("BEGIN")
        self.transaction = TransactorContextmgr(self)
        return self
    def getone(self, sql):
        ret = self.execute(sql).fetchone()
        if ret is None:
            return None
        return ret[0]
    def begin_immediate(self):
        assert self.transaction is None, \
               "Cannot BEGIN IMMEDIATE transaction while in transaction"
        
        self.execute("BEGIN IMMEDIATE")
        self.transaction = TransactorContextmgr(self)
        return self

    def begin_exclusive(self):
        assert self.transaction is None, \
               "Cannot BEGIN EXCLUSIVE transaction while in transaction"

        self.execute("BEGIN EXCLUSIVE")
        self.transaction = TransactorContextmgr(self)
        return self

    def commit(self):
        assert self.transaction is not None, \
               "Cannot COMMIT when not in transaction"
        self.execute("COMMIT")
        self.transaction = self.transaction.parent
        
    def rollback(self):
        assert self.transaction is not None, \
               "Cannot ROLLBACK when not in transaction"

        self.execute("ROLLBACK")
        self.transaction = self.transaction.parent
        
    def close(self):
        while self.transaction is not None:
            self.rollback()
        con = self.connection
        super().close()
        con.close()
