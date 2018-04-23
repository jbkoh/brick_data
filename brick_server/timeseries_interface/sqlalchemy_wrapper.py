from datetime import datetime

from sqlalchemy.engine import create_engine
from sqlalchemy import Table, Column, String, MetaData, DateTime, Float
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.dialects import postgresql

import pdb

Base = declarative_base()

TABLE_NAME = 'brick_data'

class BrickData(Base):
    __tablename__ = TABLE_NAME
    uuid = Column(String, primary_key=True)
    time = Column(DateTime, primary_key=True)
    value = Column(Float)

class SqlalchemyTimeseries(object):

    def __init__(self, dbname, user, pw, host, port=5601):
        db_str = 'postgres://{user}:{pw}@{host}:{port}/{dbname}'\
            .format(dbname=dbname, host=host, port=port, user=user, pw=pw)
        self.db = create_engine(db_str)
        self.conn = self.db.connect()
        meta = MetaData(self.db)
        self.table = Table(TABLE_NAME, meta,
                           Column('uuid', String),
                           Column('time', DateTime),
                           Column('value', Float))
        Session = sessionmaker(self.db)
        self.s= Session()
        try:
            Base.metadata.create_all(self.db)
        except Exception as e:
            print(e)
            pdb.set_trace()

    def _init_table(self):
        sql = """
        CREATE TABLE brick_data (
            time TIMESTAMPTZ NOT NULL,
            uuid TEXT NOT NULL,
            value DOUBLE PRECISION NULL
            );

        CREATE INDEX uuid_idx on brick_data (uuid);
        CREATE INDEX time_idx on brick_data (time);
        SELECT create_hypertable('brick_data', 'time', 'uuid');
        """
        #CREATE UNIQUE INDEX row_idx on brick_data (uuid,time);

    def add_data(self, data):
        objs = [{'uuid': datum[0],
                 'time': datetime.fromtimestamp(datum[1]),
                 'value': datum[2]}
                for datum in data]
        stmt = postgresql.insert(BrickData.__table__).values(objs)
        upsert_stmt = stmt.on_conflict_do_update(
            index_elements=['uuid', 'time'],
            set_={'value': stmt.excluded.value})
        self.s.execute(upsert_stmt)
        self.s.commit()

    def query_data(self, begin_time=None, end_time=None, uuids=[]):
        if uuids:
            data = self.s.query(BrickData).filter(BrickData.uuid.in_(uuids))
        else:
            data = self.s.query(BrickData)
        if begin_time:
            begin_time = datetime.fromtimestamp(begin_time)
            data = data.filter(BrickData.time >= begin_time)
        if end_time:
            end_time = datetime.fromtimestamp(end_time)
            data = data.filter(BrickData.time < end_time)
        res = data.all()


    def add_data_dep(self, data):
        if not data:
            raise Exception('Empty data to insert')
        objs = [BrickData(uuid=datum[0],
                          time=datetime.fromtimestamp(datum[1]),
                          value=datum[2])
                for datum in data]
        self.s.bulk_save_objects(objs)
        self.s.commit()


if __name__ == '__main__':
    dbname = 'brick'
    user = 'bricker'
    pw = 'brick-demo'
    host = 'localhost'
    port = 6001
    sql = SqlalchemyTimeseries(dbname, user, pw, host, port)
    data = [
        ['id3', 1524536788, 999],
        ['id4', 1524536788, 999],
        ['id5', 1524537788, 999],
    ]
    sql.add_data(data)
    begin_time = 1524436788 - 100
    end_time = 1524437788 + 10000
    uuids = ['id1']
    sql.query_data(begin_time, end_time, uuids)
