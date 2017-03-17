from sqlalchemy import create_engine
from sqlalchemy import (Table, Column, String, Float, MetaData, ForeignKey, DateTime, UniqueConstraint, Index)
from sqlalchemy.sql import select, and_
from sqlalchemy.pool import NullPool
from stolos import dag_tools as dt
from sqlalchemy.exc import DBAPIError
from stolos import log
import os
import time
import logging
import datetime
import pandas as pd

logger = logging.getLogger('stolos/resources')

def extract_mem_and_cpus(info):
    """
    extract memory and cpus from the string
    mem=DIGITS cpus=DIGITS
    """
    try:
        info1, info2 = info.split()
        type1, num1 = info1.split('=')
        type2, num2 = info2.split('=')
        if type1 == 'mem': return num1, num2
        elif type1 == 'cpus':return num2, num1
        else: return None, None
    except:
        return None, None

def build_arg_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument('--db_connect_str', type=str,
                        default='postgresql+psycopg2://stolos:mypassword@postgresql-stolos.cv2la3grv21c.us-east-1.rds.amazonaws.com:5432/postgres',
                        help=('SQL alchemy connection string for database where job status will be saved'))
    parser.add_argument('--date', default=datetime.now().strftime('%Y%m%d'),
                        help="save in sql server with this date")
    return parser


if __name__ == '__main__':
    """
    read memory and cpus configuration from ds-config for each application,
    and save in postgresql server
    """

    NS = build_arg_parser().parse_args()
    date = NS.date
    db_connect_str = NS.db_connect_str
    path = 'ds-config/ds-config/'
    clean = pd.DataFrame(columns=['main_app', 'app_name', 'memory', 'cpus'])
    excluded_apps = ['stolos-scripts.json', 'stolos.json', 'userpred-models.json']
    files = [path + f for f in os.listdir(path) if '_' not in f and f not in excluded_apps]

    ## generate the resource table from ds-config json files
    for file in files:
        raw = pd.read_json(file, orient='index')
        main_app = (file.split('/')[2]).replace('.json', '')
        for i in range(len(raw)):
            info = raw.RELAY_MESOS_TASK_RESOURCES[i]
            memory, cpus = extract_mem_and_cpus(info)
            if memory and cpus:
                clean.loc[len(clean)+1] = [main_app, main_app +'/' + raw.index[i], memory, cpus]
    clean['date']=date
    print(clean)

    ## save the local table to postgresql
    engine = create_engine(db_connect_str, echo=False, poolclass=NullPool)
    conn = engine.connect()
    metadata = MetaData(bind=engine)

    config = Table('config', metadata,
                     Column('main_app', String),
                     Column('app_name', String),
                     Column('cpus', Float),
                     Column('memory', Float),
                     Column('date', String))

    ## if resource table doesn't exists, create it:
    if not engine.dialect.has_table(engine, 'config'):
        metadata.create_all(engine)

    ## update the resource table
    for i in range(1, len(clean)+1):
        try:
            clause = config.insert().values(main_app=clean.main_app[i], app_name=clean.app_name[i],
                          cpus=clean.cpus[i], memory=clean.memory[i], date=clean.date[i])
            result = conn.execute(clause)
        except Exception as e:
            error_msg = "failed to update the resource table: %s" % (str(e))
            print(error_msg)
           # logger.error(error_msg)

    conn.close()
