import datetime as DT

from sqlalchemy import create_engine
from sqlalchemy import (Table, Column, Integer, String, MetaData, ForeignKey,
                        DateTime, UniqueConstraint, Index)
from sqlalchemy.sql import select, and_
from sqlalchemy.pool import NullPool

from stolos import dag_tools as dt
from sqlalchemy.exc import DBAPIError

from stolos import log


metadata = MetaData()
jobs = Table(
    'jobs', metadata,
    Column('id', Integer, primary_key=True),
    Column('app_name', String),
    Column('job_id', String),
    UniqueConstraint('app_name', 'job_id', name='unique_job_defs')
)

Index('idx_app_name_job_id', jobs.c.job_id, jobs.c.app_name)

runs = Table(
    'runs', metadata,
    Column('job_def_id', Integer, ForeignKey('jobs.id')),
    Column('id', Integer, primary_key=True),
    Column('retry', Integer),
    Column('start_time', DateTime),
    Column('end_time', DateTime),
    Column('status', String)
)

parameters_table = Table(
    'parameters', metadata,
    Column('job_def_id', Integer, ForeignKey('jobs.id')),
    Column('key', String),
    Column('value', String)
)


class EventLogger(object):
    def __init__(self, db_connect_str, app_name, job_id, attempt):
        self.app_name = app_name
        self.job_id = job_id
        self.attempt = attempt
        try:
            self.engine = create_engine(db_connect_str, echo=False,
                                        poolclass=NullPool)
            self.conn = self.engine.connect()
            metadata.create_all(self.engine)
            self._insert_job_def()
            self.conn.close()
        except DBAPIError as e:
            log.warn("Database error occured: %s", e)

    def _insert_job_def(self):
        self.job_parameters = dt.parse_job_id(self.app_name, self.job_id)
        primary_key = self.find_existing_job()
        if primary_key is None:
            trans = self.conn.begin()
            ins = jobs.insert().values(app_name=self.app_name,
                                       job_id=self.job_id)
            result = self.conn.execute(ins)
            self.job_def_id = result.inserted_primary_key[0]
            self._save_job_parameters()
            trans.commit()
        else:
            self.job_def_id = primary_key

        ins = runs.insert().values(job_def_id=self.job_def_id,
                                   retry=self.attempt,
                                   start_time=DT.datetime.utcnow().isoformat())

        result = self.conn.execute(ins)
        self.job_run_id = result.inserted_primary_key[0]

    def _save_job_parameters(self):
        self.conn.execute(parameters_table.insert(), [
            {'job_def_id': self.job_def_id,
             'key': k,
             'value': v}
            for k, v in self.job_parameters.items()
        ])

    def find_existing_job(self):
        s = select([jobs]).where(and_(jobs.c.app_name == self.app_name,
                                      jobs.c.job_id == self.job_id))
        result = self.conn.execute(s)
        row = result.fetchone()
        if row is None:
            return None
        return row["id"]

    def job_failed(self):
        self._update_status("FAILED")

    def job_succeeded(self):
        self._update_status("SUCCEEDED")

    def _update_status(self, status):
        self.conn = self.engine.connect()
        date = DT.datetime.utcnow()
        date_str = date.isoformat()
        try:
            stmt = (runs.update().
                    where(runs.c.id == self.job_run_id).
                    values(end_time=date_str, status=status))
            self.conn.execute(stmt)
            self.conn.close()
        except DBAPIError as e:
            log.warn("Database error occured: %s", e)
        except AttributeError:
            log.warn("No connection to database")
