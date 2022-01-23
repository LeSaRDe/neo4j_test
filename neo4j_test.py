"""
TEST #1: Load a dataset, and construct a graph DB in Neo4j via Python.
    Test dataset: https://www.kaggle.com/Cornell-University/arxiv
    Attribute brief:
        id: ArXiv ID (can be used to access the paper, see below)
        submitter: Who submitted the paper
        authors: Authors of the paper
        title: Title of the paper
        comments: Additional info, such as number of pages and figures
        journal-ref: Information about the journal the paper was published in
        doi: [https://www.doi.org](Digital Object Identifier)
        abstract: The abstract of the paper
        categories: Categories / tags in the ArXiv system
        versions: A version history

TEST #2: Sample contact network
    Test dataset: contact_network_sample.txt
    Note Properties:
        pid: (int) Person ID
        hid: (int) Household ID
        age: (int) Age
        age_group: (str) One of ['p' (0-4), 's' (5-17), 'a' (18-49), 'o' (50-64), 'g' (65+)]
        gender: (int) 1 or 2
        fips: (str)
        home_lat: (float) Home latitude
        home_lon: (float) Home longitude
        admin1: (str)
        admin2: (str)
        admin3: (str)
        admin4: (str)
    Edge Properties:
        duration: (int) Contact by second
        src_act: (str) Source Activity
        trg_act: (str) Target Activity

NOTE:
    Temporarily we focus on TEST #2.
"""


import json
import logging
import csv
import os
import sys
import time
import math
import sqlite3
from os import path, walk
import re

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from neo4j import GraphDatabase
import snap


g_init_cn_folder = '/home/mf3jh/workspace/data/epihiper/'
g_int_cn_folder = '/home/mf3jh/workspace/data/epihiper/'
g_epihiper_output_folder = '/home/mf3jh/workspace/data/epihiper/'

g_init_cn_path = ''.join([g_init_cn_folder, 'wy_contact_network_config_m_5_M_40_a_1000_m-contact_0_with_lid.txt'])
g_int_cn_path_fmt = ''.join([g_int_cn_folder, 'network_{0}'])
g_person_trait_path = ''.join([g_init_cn_folder, 'wy_persontrait_epihiper.txt'])
g_epihiper_output_path = ''.join([g_epihiper_output_folder, 'output.csv'])
g_epihiper_output_db_path = ''.join([g_epihiper_output_folder, 'output.db'])
g_int_cn_cnt = 10
g_l_int_cn_path = [g_int_cn_path_fmt.format(str(i)) for i in range(g_int_cn_cnt)]

g_sample_cnt = None

g_neo4j_server_uri_fmt = 'neo4j://{0}:7687'
g_neo4j_username = 'neo4j'
g_neo4j_password = 'michal'

g_neo4j_db_name = 'samplecontactnetwork'
g_epihiper_output_tb_name = 'epihiper_output'


def load_arxiv_samples(ds_path, num_rec):
    """
    Load in a specific number of records from a given dataset
    :param
        ds_path: str
            The full path to the dataset
        num_rec: int
            Specified number of records
    :return:
        Pandas DataFrame
    """
    print('[load_in_arxiv_samples] Starts.')
    l_rec = []
    with open(ds_path, 'r') as in_fd:
        ln_cnt = 0
        for ln in in_fd:
            rec_json = json.loads(ln)
            arxiv_id = rec_json['id']
            authors = [' '.join(name).strip() for name in rec_json['authors_parsed']]
            title = rec_json['title']
            categories = [cat.strip() for cat in rec_json['categories'].split(' ')]
            l_rec.append((arxiv_id, authors, title, categories))
            ln_cnt += 1
            if ln_cnt >= num_rec:
                break

    df_sample = pd.DataFrame(l_rec, columns=['arxiv_id', 'authors', 'title', 'categories'])
    df_sample = df_sample.set_index('arxiv_id')
    print('[load_in_arxiv_samples] All done.')
    return df_sample


def load_contact_network_samples(ds_path, g_sample_cnt=None):
    """
    Note that the first line is the schema and the second line is the header.
    :return: Pandas DataFrame
    """
    print('[load_contact_network_samples] Starts.')
    if g_sample_cnt is not None and g_sample_cnt <= 2:
        raise Exception('[load_contact_network_samples] g_sample_cnt needs to be greater than 2.')

    l_rec = []
    with open(ds_path, 'r') as in_fd:
        csv_reader = csv.reader(in_fd, delimiter=',')
        ln_cnt = 0
        for row_idx, row in enumerate(csv_reader):
            if row_idx == 0 or row_idx == 1:
                continue
            else:
                if g_sample_cnt is not None and ln_cnt >= g_sample_cnt:
                    break
                targetPID = int(row[0])
                targetActivity = row[1]
                sourcePID = int(row[2])
                sourceActivity = row[3]
                duration = int(row[4])
                l_rec.append((targetPID, targetActivity, sourcePID, sourceActivity, duration))
                ln_cnt += 1
    df_contact_network = pd.DataFrame(l_rec, columns=['targetPID', 'targetActivity', 'sourcePID',
                                                      'sourceActivity', 'duration'])
    print('[load_contact_network_samples] All done with %s records.' % len(df_contact_network))
    return df_contact_network


def load_person_trait(ds_path):
    """
    Note that the first line is the schema and the second line is the header.
    :return: Pandas DataFrame
    """
    print('[load_person_trait] Starts.')

    l_rec = []
    with open(ds_path, 'r') as in_fd:
        csv_reader = csv.reader(in_fd, delimiter=',')
        for row_idx, row in enumerate(csv_reader):
            if row_idx == 0 or row_idx == 1:
                continue
            else:
                pid = int(row[0])
                hid = int(row[1])
                age = int(row[2])
                age_group = row[3]
                gender = int(row[4])
                fips = row[5]
                home_lat = float(row[6])
                home_lon = float(row[7])
                admin1 = row[8]
                admin2 = row[9]
                admin3 = row[10]
                admin4 = row[11]
                l_rec.append((pid, hid, age, age_group, gender, fips, home_lat, home_lon, admin1, admin2, admin3, admin4))
    df_person_trait = pd.DataFrame(l_rec, columns=['pid', 'hid', 'age', 'age_group', 'gender', 'fips', 'home_lat',
                                                   'home_lon', 'admin1', 'admin2', 'admin3', 'admin4'])
    df_person_trait = df_person_trait.set_index('pid')
    print('[load_person_trait] All done with %s records.' % len(df_person_trait))
    return df_person_trait


def load_epihiper_output(ds_path):
    print('[load_epihiper_output] Starts.')

    l_rec = []
    with open(ds_path, 'r') as in_fd:
        csv_reader = csv.reader(in_fd, delimiter=',')
        for row_idx, row in enumerate(csv_reader):
            if row_idx == 0:
                continue
            else:
                tick = int(row[0])
                pid = int(row[1])
                exit_state = row[2]
                contact_pid = int(row[3])
                if contact_pid == -1:
                    contact_pid = None
                lid = int(row[4])
                if lid == -1:
                    lid = None
            l_rec.append((tick, pid, exit_state, contact_pid, lid))
    df_output = pd.DataFrame(l_rec, columns=['tick', 'pid', 'exit_state', 'contact_pid', 'lid'])
    print('[load_epihiper_output] All done with %s output records.' % len(df_output))
    return df_output


def connect_to_neo4j_driver(uri, auth, kwargs):
    """
    Connect to a Neo4j drive using the given URI.
    :param
        uri: str
            Can be "bolt" or "neo4j".
            Supported formats are:
                bolt://host[:port]
                bolt+ssc://host[:port]
                bolt+s://host[:port]
                neo4j://host[:port][?routing_context]
                neo4j+ssc://host[:port][?routing_context]
                neo4j+s://host[:port][?routing_context]
            This URI can be retrieved from Neo4j browser or cypher-shell.
            More details can be found in here:
            https://neo4j.com/docs/api/python-driver/current/api.html#driver-construction
    :param
        auth: tuple or neo4j.Auth:
            The simplest format: (USERNAME, PASSWORD): (str, str)
            Other authentication methods are also supported.
            https://neo4j.com/docs/api/python-driver/current/api.html#auth-ref
    :return: neo4j.Driver
        A driver instance if succeeds. None, otherwise.
    """
    print('[connect_to_neo4j_driver] Connecting to %s...' % uri)
    if uri is None or len(uri) <= 0:
        return None

    try:
        driver = GraphDatabase.driver(uri=uri, auth=auth, **kwargs)
    except Exception as e:
        logging.error('[connect_to_neo4j_driver] Failed to connect to Neo4j driver: %s' % e)
        return None

    print('[connect_to_neo4j_driver] Connection established.')
    return driver


def get_neo4j_session(neo4j_driver, session_config=None):
    """
    Create a Neo4j Session given (or not) configurations.
    :param
        neo4j_driver: neo4j.Driver
            The driver instance.
    :param
        session_config: dict
            Available keys:
                'bookmarks'
                'database'
                'default_access_mode'
                'fetch_size'
    :return: neo4j.Session
        A session instance if succeeds. None, otherwise.
    """
    if neo4j_driver is None:
        raise Exception('[get_neo4j_session] Neo4j driver is not valid.')
    # print('[get_neo4j_session] Starts.')

    try:
        if session_config is not None:
            neo4j_session = neo4j_driver.session(**session_config)
        else:
            neo4j_session = neo4j_driver.session()
    except Exception as e:
        logging.error('[get_neo4j_session] Failed to create Neo4j session: %s' % e)
        return None

    # print('[get_neo4j_session] Create a Neo4j session.')
    return neo4j_session


def execute_neo4j_queries(neo4j_driver, neo4j_session_config, l_query_str, l_query_param=None, need_ret=False):
    """
    Execute a sequence of queries to Neo4j DB server. Only one commit is executed if necessary.
    NOTE:
        If 'l_query_param' is not None, len(l_query_param) should be equal to len(l_query_str).
        If no param for a query when 'l_query_param' is not None, the corresponding param should be None.
    :param
        neo4j_driver: neo4j.Driver
    :param
        neo4j_session_config: dict
            See 'get_neo4j_session'.
    :param
        l_query_str: list of str
    :param
        l_query_param: list of dict
    :param
        need_ret: bool
            True: return resutls
            False: no return
    :return: list of neo4j.Result or None
    """
    if neo4j_driver is None:
        raise Exception('[execute_neo4j_query] neo4j_driver is None. Run "neo4j_driver" cmd first.')
    if l_query_str is None or len(l_query_str) <= 0:
        print('[execute_neo4j_query] No query is available.')
        return None
    if l_query_param is not None and len(l_query_str) != len(l_query_param):
        raise Exception('[execute_neo4j_query] l_query_param does not match l_query_str.')

    neo4j_session = get_neo4j_session(neo4j_driver, session_config=neo4j_session_config)

    timer_start = time.time()
    try:
        # EXPLICIT TRANSACTIONS ARE USED
        l_ret = []
        with neo4j_session.begin_transaction() as neo4j_tx:
            for query_id, query_str in enumerate(l_query_str):
                query_param = None
                if l_query_param is not None:
                    query_param = l_query_param[query_id]
                results = neo4j_tx.run(query_str, query_param)
                if need_ret:
                    # !!!CAUTION!!!
                    # Here we need to use 'values()' function to retain the results instead of 'data()'
                    # because 'data()' may miss some data in the results!
                    l_ret.append(results.values())
            neo4j_tx.commit()
        neo4j_session.close()
    except Exception as e:
        logging.error('[execute_neo4j_query] Failed query: %s' % e)
        return None

    print('[execute_neo4j_query] All done in %s secs.' % str(time.time() - timer_start))
    if need_ret:
        return l_ret
    else:
        return None


def create_init_cn(neo4j_driver, init_cn_batch_size=100000):
    """
    Create initial contact graph, including person trait, in Neo4j DB. Since loading the whole contact network may
    overwhelm our memory, we load in it batch by batch.
    NOTE:
        - We assume that the person trait records are unique by their PIDs.
        - We also assume that the set of people listed in the person trait file is a superset of people appearing in the
          initial contact network file.
        - And we don't check these two items particularly.
    """
    print('[create_init_cn] Starts.')
    timer_start_init = time.time()
    timer_start = time.time()

    # CONFIGURE NEO4J SESSION
    neo4j_session_config = {'database': g_neo4j_db_name}

    # LOAD PERSON TRAIT
    # Person trait files are typically reasonable in size. So we load in the whole file.
    l_person_trait = []
    with open(g_person_trait_path, 'r') as in_fd:
        csv_reader = csv.reader(in_fd, delimiter=',')
        for row_idx, row in enumerate(csv_reader):
            if row_idx == 0 or row_idx == 1:
                continue
            else:
                pid = int(row[0])
                hid = int(row[1])
                age = int(row[2])
                age_group = row[3]
                gender = int(row[4])
                fips = row[5]
                home_lat = float(row[6])
                home_lon = float(row[7])
                admin1 = row[8]
                admin2 = row[9]
                admin3 = row[10]
                admin4 = row[11]
                l_person_trait.append({'pid': pid,
                                       'hid': hid,
                                       'age': age,
                                       'age_group': age_group,
                                       'gender': gender,
                                       'fips': fips,
                                       'home_lat': home_lat,
                                       'home_lon': home_lon,
                                       'admin1': admin1,
                                       'admin2': admin2,
                                       'admin3': admin3,
                                       'admin4': admin4})

    # CREATE NODES BASED ON PERSON TRAIT
    query_str = '''unwind $rec as rec
                   create (n:PERSON {pid: rec.pid, hid: rec.hid, age: rec.age, age_group: rec.age_group, 
                   gender: rec.gender, fips: rec.fips, home_lat: rec.home_lat, home_lon: rec.home_lon, 
                   admin1: rec.admin1, admin2: rec.admin2, admin3: rec.admin3, admin4: rec.admin4})'''
    query_param = {'rec': l_person_trait}
    execute_neo4j_queries(neo4j_driver, neo4j_session_config, [query_str], l_query_param=[query_param])
    print('[create_init_cn] Create %s nodes in %s secs.' % (len(l_person_trait), time.time() - timer_start))

    # LOAD IN INITIAL CONTACT NETWORK DATA BATCH BY BATCH
    timer_start = time.time()
    occur_time_stamp = -1
    query_str = '''unwind $rec as rec
                   match (src:PERSON), (trg:PERSON)
                   where src.pid=rec.sourcePID and trg.pid=rec.targetPID
                   create (src)-[r:CONTACT {occur: %s, src_act:rec.sourceActivity, trg_act:rec.targetActivity,
                   duration:rec.duration}]->(trg)
                ''' % occur_time_stamp
    total_cnt = 0
    l_init_cn_batch = []
    with open(g_init_cn_path, 'r') as in_fd:
        csv_reader = csv.reader(in_fd, delimiter=',')
        for row_idx, row in enumerate(csv_reader):
            if row_idx == 0 or row_idx == 1:
                continue
            else:
                targetPID = int(row[0])
                targetActivity = row[1]
                sourcePID = int(row[2])
                sourceActivity = row[3]
                duration = int(row[4])
                l_init_cn_batch.append({'targetPID': targetPID,
                                        'targetActivity': targetActivity,
                                        'sourcePID': sourcePID,
                                        'sourceActivity': sourceActivity,
                                        'duration': duration})

            # CREATE EDGES FOR EACH BATCH
            if len(l_init_cn_batch) >= init_cn_batch_size:
                query_param = {'rec': l_init_cn_batch}
                execute_neo4j_queries(neo4j_driver, neo4j_session_config, [query_str], l_query_param=[query_param])
                total_cnt += len(l_init_cn_batch)
                l_init_cn_batch = []
                print('[create_init_cn] Created %s edges in %s secs.' % (total_cnt, time.time() - timer_start))
        if len(l_init_cn_batch) > 0:
            query_param = {'rec': l_init_cn_batch}
            execute_neo4j_queries(neo4j_driver, neo4j_session_config, [query_str], l_query_param=[query_param])
            total_cnt += len(l_init_cn_batch)
            print('[create_init_cn] Created %s edges in %s secs.' % (total_cnt, time.time() - timer_start))
    print('[create_init_cn] All done. Running time: %s ' % str(time.time() - timer_start_init))


def create_int_cn(neo4j_driver, int_cn_cnt, int_cn_batch_size=100000):
    """
    As intermediate contact networks will not introduce new nodes, it's all about edges.
    TODO
        The edge loading part can be separated as an individual function. And thus both 'create_int_cn' and
        'create_init_cn' can be refactored.
    """
    print('[create_int_cn] Starts.')

    if int_cn_cnt <= 0:
        logging.error('[create_int_cn] No intermediate contact network is specified by "int_cn_cnt".')
        return None

    timer_start_init = time.time()
    timer_start = time.time()

    # CONFIGURE NEO4J SESSION
    neo4j_session_config = {'database': g_neo4j_db_name}

    # LOAD IN INTERMEDIATE CONTACT NETWORKS
    # occur_time_stamp = -1
    query_str_fmt = '''unwind $rec as rec
                       match (src:PERSON), (trg:PERSON)
                       where src.pid=rec.sourcePID and trg.pid=rec.targetPID
                       create (src)-[r:CONTACT {occur: %s, src_act:rec.sourceActivity, trg_act:rec.targetActivity,
                       duration:rec.duration}]->(trg)
                    '''
    for int_cn_idx in range(int_cn_cnt):
        total_cnt_per_int_cn = 0
        l_int_cn_batch = []
        with open(g_int_cn_path_fmt.format(str(int_cn_idx)), 'r') as in_fd:
            csv_reader = csv.reader(in_fd, delimiter=',')
            for row_idx, row in enumerate(csv_reader):
                if row_idx == 0 or row_idx == 1:
                    continue
                else:
                    targetPID = int(row[0])
                    targetActivity = row[1]
                    sourcePID = int(row[2])
                    sourceActivity = row[3]
                    duration = int(row[4])
                    l_int_cn_batch.append({'targetPID': targetPID,
                                           'targetActivity': targetActivity,
                                           'sourcePID': sourcePID,
                                           'sourceActivity': sourceActivity,
                                           'duration': duration})
                # CREATE EDGES FOR EACH BATCH
                if len(l_int_cn_batch) >= int_cn_batch_size:
                    query_param = {'rec': l_int_cn_batch}
                    execute_neo4j_queries(neo4j_driver, neo4j_session_config, [query_str_fmt % str(int_cn_idx)],
                                          l_query_param=[query_param])
                    total_cnt_per_int_cn += len(l_int_cn_batch)
                    l_int_cn_batch = []
                    print('[create_int_cn] Int CN %s: Created %s edges in %s secs.'
                          % (int_cn_idx, total_cnt_per_int_cn, time.time() - timer_start))
            if len(l_int_cn_batch) > 0:
                query_param = {'rec': l_int_cn_batch}
                execute_neo4j_queries(neo4j_driver, neo4j_session_config, [query_str_fmt % str(int_cn_idx)],
                                      l_query_param=[query_param])
                total_cnt_per_int_cn += len(l_int_cn_batch)
                print('[create_int_cn] Int CN %s: Created %s edges in %s secs.'
                      % (int_cn_idx, total_cnt_per_int_cn, time.time() - timer_start))
            print('[create_int_cn] Int CN %s: All done in %s secs.' % (int_cn_idx, time.time() - timer_start))

    print('[create_int_cn] All done. Running time: %s ' % str(time.time() - timer_start_init))


def create_int_cn_auto_search(neo4j_driver, search_folder, int_cn_batch_size=1000000):
    """
    Search for intermediate files and load in.
    """
    print('[create_int_cn_auto_search] Starts.')

    if not path.exists(search_folder):
        logging.error('[create_int_cn_auto_search] search_folder %s does not exist.' % search_folder)
        return None

    int_cn_file_fmt = 'network\[\d+\]'

    timer_start_init = time.time()
    timer_start = time.time()

    # CONFIGURE NEO4J SESSION
    neo4j_session_config = {'database': g_neo4j_db_name}

    # LOAD IN INTERMEDIATE CONTACT NETWORKS
    # occur_time_stamp = -1
    query_str_fmt = '''unwind $rec as rec
                       match (src:PERSON), (trg:PERSON)
                       where src.pid=rec.sourcePID and trg.pid=rec.targetPID
                       create (src)-[r:CONTACT {occur: %s, src_act:rec.sourceActivity, trg_act:rec.targetActivity,
                       duration:rec.duration}]->(trg)
                    '''

    for (dirpath, dirname, filenames) in walk(search_folder):
        for filename in filenames:
            if re.match(int_cn_file_fmt, filename) is None:
                continue

            l_num_str = re.findall(r'[0-9]+', filename)
            if len(l_num_str) != 1:
                logging.error('[create_int_cn_auto_search] Confusing file occurs: %s' % filename)
                continue

            int_cn_idx = int(l_num_str[0])

            total_cnt_per_int_cn = 0
            l_int_cn_batch = []
            with open(path.join(dirpath, filename), 'r') as in_fd:
                csv_reader = csv.reader(in_fd, delimiter=',')
                for row_idx, row in enumerate(csv_reader):
                    if row_idx == 0 or row_idx == 1:
                        continue
                    else:
                        targetPID = int(row[0])
                        targetActivity = row[1]
                        sourcePID = int(row[2])
                        sourceActivity = row[3]
                        duration = int(row[4])
                        l_int_cn_batch.append({'targetPID': targetPID,
                                               'targetActivity': targetActivity,
                                               'sourcePID': sourcePID,
                                               'sourceActivity': sourceActivity,
                                               'duration': duration})
                    # CREATE EDGES FOR EACH BATCH
                    if len(l_int_cn_batch) >= int_cn_batch_size:
                        query_param = {'rec': l_int_cn_batch}
                        execute_neo4j_queries(neo4j_driver, neo4j_session_config, [query_str_fmt % str(int_cn_idx)],
                                              l_query_param=[query_param])
                        total_cnt_per_int_cn += len(l_int_cn_batch)
                        l_int_cn_batch = []
                        print('[create_int_cn_auto_search] Int CN %s: Created %s edges in %s secs.'
                              % (int_cn_idx, total_cnt_per_int_cn, time.time() - timer_start))
                if len(l_int_cn_batch) > 0:
                    query_param = {'rec': l_int_cn_batch}
                    execute_neo4j_queries(neo4j_driver, neo4j_session_config, [query_str_fmt % str(int_cn_idx)],
                                          l_query_param=[query_param])
                    total_cnt_per_int_cn += len(l_int_cn_batch)
                    print('[create_int_cn_auto_search] Int CN %s: Created %s edges in %s secs.'
                          % (int_cn_idx, total_cnt_per_int_cn, time.time() - timer_start))
                print('[create_int_cn_auto_search] Int CN %s: All done in %s secs.'
                      % (int_cn_idx, time.time() - timer_start))

    print('[create_int_cn_auto_search] All done. Running time: %s ' % str(time.time() - timer_start_init))


def create_epihiper_output_db():
    """
    Return True if successes, False otherwise.
    """
    print('[create_epihiper_output_db] Starts.')

    try:
        db_con = sqlite3.connect(g_epihiper_output_db_path)
    except Exception as e:
        logging.error('[create_epihiper_output_db] %s' % e)
        return False
    print('[create_epihiper_output_db] Database created.')

    if db_con is not None:
        sql_str = '''create table if not exists %s
                     (
                        out_id integer primary key,
                        tick integer not null,
                        pid integer not null,
                        exit_state text,
                        contact_pid integer, 
                        lid text
                     )
                  ''' % g_epihiper_output_tb_name
        try:
            db_cur = db_con.cursor()
            db_cur.execute(sql_str)
        except Exception as e:
            logging.error('[create_epihiper_output_db] %s' % e)
            return False
        finally:
            db_con.close()
        print('[create_epihiper_output_db] Table created.')

    print('[create_epihiper_output_db] All done.')
    return True


def load_epihiper_output_to_db(batch_size=10000):
    """
    Return True if successes, False otherwise.
    TODO
        It may look neater refactoring the DB connection and closure to 'with' statement.
    """
    print('[load_epihiper_output_to_db] Starts.')
    timer_start = time.time()

    if not path.exists(g_epihiper_output_path):
        raise Exception('[load_epihiper_output_to_db] %s does not exist.' % g_epihiper_output_path)

    try:
        db_con = sqlite3.connect(g_epihiper_output_db_path)
        db_cur = db_con.cursor()
    except Exception as e:
        logging.error('[load_epihiper_output_to_db] %s' % e)
        return False

    sql_str = '''insert into %s (out_id, tick, pid, exit_state, contact_pid, lid) values (?,?,?,?,?,?)''' \
              % g_epihiper_output_tb_name

    err = False
    out_id = 0
    with open(g_epihiper_output_path, 'r') as in_fd:
        csv_reader = csv.reader(in_fd, delimiter=',')
        for row_idx, row in enumerate(csv_reader):
            if row_idx == 0:
                continue
            else:
                tick = int(row[0])
                pid = int(row[1])
                exit_state = row[2]
                contact_pid = int(row[3])
                if contact_pid == -1:
                    contact_pid = None
                lid = int(row[4])
                if lid == -1:
                    lid = None
            try:
                db_cur.execute(sql_str, (out_id, tick, pid, exit_state, contact_pid, lid))
                out_id += 1
                if out_id % batch_size == 0 and out_id >= batch_size:
                    db_con.commit()
                    print('[load_epihiper_output_to_db] Committed %s recs in %s secs.'
                          % (out_id, time.time() - timer_start))
            except Exception as e:
                logging.error('[load_epihiper_output_to_db] row: %s, error: %s' % (row, e))
                err = True
        try:
            db_con.commit()
            print('[load_epihiper_output_to_db] Committed %s recs in %s secs.'
                  % (out_id, time.time() - timer_start))
        except Exception as e:
            logging.error('[load_epihiper_output_to_db] row: %s, error: %s' % (row, e))
            err = True

    db_con.close()

    if err:
        print('[load_epihiper_output_to_db] Return with errors in %s secs.' % str(time.time() - timer_start))
    else:
        print('[load_epihiper_output_to_db] All done in %s secs.' % str(time.time() - timer_start))
    return not err


def create_indexes_on_epihipter_output_db():
    """
    Return True if successes, False otherwise.
    """
    print('[create_indexes_on_epihipter_output_db] Starts.')
    timer_start = time.time()

    try:
        db_con = sqlite3.connect(g_epihiper_output_db_path)
        db_cur = db_con.cursor()
    except Exception as e:
        logging.error(e)
        return None

    sql_str_1 = '''drop index if exists idx_tick'''
    sql_str_2 = '''drop index if exists idx_pid'''

    try:
        db_cur.execute(sql_str_1)
        db_cur.execute(sql_str_2)
    except Exception as e:
        logging.error('[create_indexes_on_epihipter_output_db] Drop indexes: %s' % e)
        return False

    sql_str_1 = '''create index if not exists idx_tick on %s (tick)''' % g_epihiper_output_tb_name
    sql_str_2 = '''create index if not exists idx_pid on %s (pid)''' % g_epihiper_output_tb_name

    try:
        db_cur.execute(sql_str_1)
        db_cur.execute(sql_str_2)
    except Exception as e:
        logging.error('[create_indexes_on_epihipter_output_db] Create indexes: %s' % e)
        return False

    db_con.close()
    print('[create_indexes_on_epihipter_output_db] All done in %s secs.' % str(time.time() - timer_start))
    return True


def fetch_pids_by_exit_state(exit_state):
    """
    Return the list of PIDs over time for a given exit state.
    :return: pandas DataFrame
        Index: tick (int)
        Column: pid (list of int)
    """
    print('[fetch_pids_by_exit_state] Starts.')
    timer_start = time.time()

    try:
        db_con = sqlite3.connect(g_epihiper_output_db_path)
        db_cur = db_con.cursor()
    except Exception as e:
        logging.error(e)
        return None

    sql_str = '''select tick, pid from %s where exit_state="%s"''' \
              % (g_epihiper_output_tb_name, exit_state)
    try:
        db_cur.execute(sql_str)
        rows = db_cur.fetchall()
    except Exception as e:
        logging.error('[fetch_pids_by_exit_state] %s' % e)
        return None

    d_pid_by_tick = dict()
    for row in rows:
        tick = int(row[0])
        pid = int(row[1])
        if tick not in d_pid_by_tick:
            d_pid_by_tick[tick] = [pid]
        else:
            d_pid_by_tick[tick].append(pid)

    l_rec = []
    for tick in d_pid_by_tick:
        l_rec.append((tick, d_pid_by_tick[tick]))
    df_pid_by_tick = pd.DataFrame(l_rec, columns=['tick', 'pid'])
    df_pid_by_tick = df_pid_by_tick.set_index('tick')
    pd.to_pickle(df_pid_by_tick, ''.join([g_epihiper_output_folder, 'output_pid_over_time_by_%s.pickle' % exit_state]))

    db_con.close()
    print('[fetch_pids_by_exit_state] All done in %s secs.' % str(time.time() - timer_start))
    return df_pid_by_tick


def duration_distribution(neo4j_driver, df_output_pid_over_time, out_name_suffix, l_t=None, mode='in_1nn'):
    """
    Get the duration distribution over a subgraph of contact network at time points specified by 'l_t'.
    The subgraph is defined by 'mode'.
    :param
        df_output_pid_over_time: pandas DataFrame
            Filtered PIDs over time. The subgraph is constructed starting from these PIDs.
    :param
        l_t: list of int
            The given time points. '-1' means the initial contact graph.
    :param
        mode: str
            'in_1nn': The subgraph is the 1-nearest-neighbor graph induced by incoming edges based on 'l_core_pids'.
    :return: 2D ndarray
        Dim 0: Durations sorted in the ascending order.
        Dim 1: Counts of durations.
    """
    print('[duration_distribution] Starts.')
    timer_start = time.time()

    neo4j_session_config = {'database': g_neo4j_db_name}

    if mode == 'in_1nn':
        query_str = '''with $l_core_pid as l_core_pid, $tick as tick
                       match (n:PERSON) where n.pid in l_core_pid
                       match ()-[r:CONTACT]->(n) where r.occur = tick
                       return r.duration as d
                    '''

    if l_t is None:
        l_t = df_output_pid_over_time.index.to_list()

    l_dist_rec = []
    for tick, pid_rec in df_output_pid_over_time.loc[l_t].iterrows():
        d_duration = dict()
        l_core_pids = pid_rec['pid']
        query_param = {'l_core_pid': l_core_pids, 'tick': tick}
        l_ret = execute_neo4j_queries(neo4j_driver, neo4j_session_config, [query_str], l_query_param=[query_param],
                                      need_ret=True)
        for rec in l_ret[0]:
            duration = int(rec[0])
            if duration not in d_duration:
                d_duration[duration] = 1
            else:
                d_duration[duration] += 1
        if len(d_duration) <= 0:
            continue
        l_dist_rec.append((tick, d_duration))

    df_dist = pd.DataFrame(l_dist_rec, columns=['tick', 'duration_dist'])
    df_dist = df_dist.set_index('tick')
    pd.to_pickle(df_dist, ''.join([g_epihiper_output_folder, 'duration_dist_', out_name_suffix, '.pickle']))
    print('[duration_distribution] Output results.')

    # PLOT
    fig, axes = plt.subplots(ncols=1, nrows=1)
    d_draw = dict()
    for tick, dist_rec in df_dist.iterrows():
        d_dist = dist_rec['duration_dist']
        l_duration = []
        for duration in d_dist:
            l_duration += [duration] * d_dist[duration]
        d_draw['t' + str(tick)] = l_duration
    sns.histplot(d_draw, multiple='stack', legend=True, ax=axes)
    axes.set_title('Duration Distribution Over Time With Exit State %s' % out_name_suffix, fontsize=12,
                   fontweight='semibold')
    axes.set_xlabel('Duration', fontweight='semibold')
    axes.set_ylabel('Frequency', fontweight='semibold')
    plt.tight_layout(pad=1.0)
    plt.savefig(''.join([g_epihiper_output_folder, 'duration_distribution_', out_name_suffix, '.PNG']), format='PNG')
    plt.show()
    plt.clf()
    plt.close()

    print('[duration_distribution] All done in %s secs.' % str(time.time() - timer_start))


def add_node_to_snap_graph(tneanet_ins, pid, hid, age, age_group, gender, home_lat, home_lon, fips, admin1, admin2,
                           admin3, admin4):
    """
    Each node is uniquely identified by its PID.
    """
    if tneanet_ins.IsNode(pid):
        return tneanet_ins

    tneanet_ins.AddNode(pid)
    tneanet_ins.AddIntAttrDatN(pid, hid, 'hid')
    tneanet_ins.AddIntAttrDatN(pid, age, 'age')
    tneanet_ins.AddStrAttrDatN(pid, age_group, 'age_group')
    tneanet_ins.AddIntAttrDatN(pid, gender, 'gender')
    tneanet_ins.AddFltAttrDatN(pid, home_lat, 'home_lat')
    tneanet_ins.AddFltAttrDatN(pid, home_lon, 'home_lon')
    tneanet_ins.AddStrAttrDatN(pid, fips, 'fips')
    tneanet_ins.AddStrAttrDatN(pid, admin1, 'admin1')
    tneanet_ins.AddStrAttrDatN(pid, admin2, 'admin2')
    tneanet_ins.AddStrAttrDatN(pid, admin3, 'admin3')
    tneanet_ins.AddStrAttrDatN(pid, admin4, 'admin4')

    return tneanet_ins


def add_edge_to_snap_graph(tneanet_ins, src_pid, trg_pid, duration, src_act, trg_act, occur=None):
    """
    Multi-edges are supported.
    """
    eid = tneanet_ins.AddEdge(src_pid, trg_pid)
    tneanet_ins.AddIntAttrDatE(eid, duration, 'duration')
    tneanet_ins.AddStrAttrDatE(eid, src_act, 'src_act')
    tneanet_ins.AddStrAttrDatE(eid, trg_act, 'trg_act')
    if occur is not None:
        tneanet_ins.AddIntAttrDatE(eid, occur, 'occur')

    return tneanet_ins


def output_in_1nn(neo4j_driver, df_output_pid_over_time, graph_name_suffix):
    """
    Read in subsets of nodes over time specified by 'df_output_pid_over_time', and output TNEANet graph files for
    the time points.
    """
    print('[output_in_1nn] Starts.')
    timer_start = time.time()

    neo4j_session_config = {'database': g_neo4j_db_name}
    query_str = '''with $l_core_pid as l_core_pid, $tick as tick
                   match (t:PERSON) where t.pid in l_core_pid
                   match (s:PERSON)-[r:CONTACT]->(t) where r.occur = tick
                   return s, t, r
                '''
    for tick, pid_rec in df_output_pid_over_time.iterrows():
        l_core_pids = pid_rec['pid']
        query_param = {'l_core_pid': l_core_pids, 'tick': tick}
        l_ret = execute_neo4j_queries(neo4j_driver, neo4j_session_config, [query_str], l_query_param=[query_param],
                                      need_ret=True)
        if len(l_ret[0]) <= 0:
            continue

        tneanet_ins = snap.TNEANet.New()
        for ret in l_ret[0]:
            src = ret[0]
            src_pid = int(src['pid'])
            src_hid = int(src['hid'])
            src_age = int(src['age'])
            src_age_group = src['age_group']
            src_gender = int(src['gender'])
            src_home_lat = float(src['home_lat'])
            src_home_lon = float(src['home_lon'])
            src_fips = src['fips']
            src_admin1 = src['admin1']
            src_admin2 = src['admin2']
            src_admin3 = src['admin3']
            src_admin4 = src['admin4']
            add_node_to_snap_graph(tneanet_ins, src_pid, src_hid, src_age, src_age_group, src_gender, src_home_lat,
                                   src_home_lon, src_fips, src_admin1, src_admin2, src_admin3, src_admin4)

            trg = ret[1]
            trg_pid = int(trg['pid'])
            trg_hid = int(trg['hid'])
            trg_age = int(trg['age'])
            trg_age_group = trg['age_group']
            trg_gender = int(trg['gender'])
            trg_home_lat = float(trg['home_lat'])
            trg_home_lon = float(trg['home_lon'])
            trg_fips = trg['fips']
            trg_admin1 = trg['admin1']
            trg_admin2 = trg['admin2']
            trg_admin3 = trg['admin3']
            trg_admin4 = trg['admin4']
            add_node_to_snap_graph(tneanet_ins, trg_pid, trg_hid, trg_age, trg_age_group, trg_gender, trg_home_lat,
                                   trg_home_lon, trg_fips, trg_admin1, trg_admin2, trg_admin3, trg_admin4)

            edge = ret[2]
            edge_duration = edge['duration']
            edge_src_act = edge['src_act']
            edge_trg_act = edge['trg_act']
            add_edge_to_snap_graph(tneanet_ins, src_pid, trg_pid, edge_duration, edge_src_act, edge_trg_act)

        fd_out = snap.TFOut(''.join([g_epihiper_output_folder, 'in_1nn_', graph_name_suffix,
                                     '_t%s.snap_graph' % str(tick)]))
        tneanet_ins.Save(fd_out)
        fd_out.Flush()
        print('[output_in_1nn] Output SNAP graph for tick %s with %s nodes and %s edges.'
              % (tick, tneanet_ins.GetNodes(), tneanet_ins.GetEdges()))

    print('[output_in_1nn] All done in %s secs.' % str(time.time() - timer_start))


def output_in_1nn_batch(neo4j_driver, df_output_pid_over_time, batch_size, graph_name_suffix):
    print('[output_in_1nn_batch] Starts.')
    timer_start = time.time()

    neo4j_session_config = {'database': g_neo4j_db_name}
    query_str_fmt = '''with $l_core_pid as l_core_pid, $tick as tick
                       match (t:PERSON) where t.pid in l_core_pid
                       match (s:PERSON)-[r:CONTACT]->(t) where r.occur = tick
                       return s, t, r
                       skip %s limit %s
                    '''

    for tick, pid_rec in df_output_pid_over_time.iterrows():
        l_core_pids = pid_rec['pid']
        query_param = {'l_core_pid': l_core_pids, 'tick': tick}
        skip = 0
        limit = batch_size
        batch_cnt = 0
        while True:
            l_ret = execute_neo4j_queries(neo4j_driver, neo4j_session_config, [query_str_fmt % (skip, limit)],
                                          l_query_param=[query_param],
                                          need_ret=True)
            if len(l_ret[0]) <= 0:
                print('[output_in_1nn_batch] Done graph output with %s batches for tick %s' % (batch_cnt, tick))
                break

            tneanet_ins = snap.TNEANet.New()
            for ret in l_ret[0]:
                src = ret[0]
                src_pid = int(src['pid'])
                src_hid = int(src['hid'])
                src_age = int(src['age'])
                src_age_group = src['age_group']
                src_gender = int(src['gender'])
                src_home_lat = float(src['home_lat'])
                src_home_lon = float(src['home_lon'])
                src_fips = src['fips']
                src_admin1 = src['admin1']
                src_admin2 = src['admin2']
                src_admin3 = src['admin3']
                src_admin4 = src['admin4']
                add_node_to_snap_graph(tneanet_ins, src_pid, src_hid, src_age, src_age_group, src_gender, src_home_lat,
                                       src_home_lon, src_fips, src_admin1, src_admin2, src_admin3, src_admin4)

                trg = ret[1]
                trg_pid = int(trg['pid'])
                trg_hid = int(trg['hid'])
                trg_age = int(trg['age'])
                trg_age_group = trg['age_group']
                trg_gender = int(trg['gender'])
                trg_home_lat = float(trg['home_lat'])
                trg_home_lon = float(trg['home_lon'])
                trg_fips = trg['fips']
                trg_admin1 = trg['admin1']
                trg_admin2 = trg['admin2']
                trg_admin3 = trg['admin3']
                trg_admin4 = trg['admin4']
                add_node_to_snap_graph(tneanet_ins, trg_pid, trg_hid, trg_age, trg_age_group, trg_gender, trg_home_lat,
                                       trg_home_lon, trg_fips, trg_admin1, trg_admin2, trg_admin3, trg_admin4)

                edge = ret[2]
                edge_duration = edge['duration']
                edge_src_act = edge['src_act']
                edge_trg_act = edge['trg_act']
                add_edge_to_snap_graph(tneanet_ins, src_pid, trg_pid, edge_duration, edge_src_act, edge_trg_act)

            fd_out = snap.TFOut(''.join([g_epihiper_output_folder, 'in_1nn_', graph_name_suffix,
                                         '_t%s_%s.snap_graph' % (tick, batch_cnt)]))
            tneanet_ins.Save(fd_out)
            fd_out.Flush()

            context = snap.TTableContext()
            node_ttable = snap.TTable.GetNodeTable(tneanet_ins, context)
            edge_ttable = snap.TTable.GetEdgeTable(tneanet_ins, context)

            fd_out = snap.TFOut(''.join([g_epihiper_output_folder, 'node_ttable_%s_t%s_%s.bin'
                                         % (graph_name_suffix, tick, batch_cnt)]))
            node_ttable.Save(fd_out)
            fd_out.Flush()
            fd_out = snap.TFOut(''.join([g_epihiper_output_folder, 'edge_ttable_%s_t%s_%s.bin'
                                         % (graph_name_suffix, tick, batch_cnt)]))
            edge_ttable.Save(fd_out)
            fd_out.Flush()

            print('[output_in_1nn_batch] Output SNAP graph for tick %s on offset %s and batch size %s with %s nodes and %s edges.'
                  % (tick, skip, len(l_ret[0]), tneanet_ins.GetNodes(), tneanet_ins.GetEdges()))
            skip += limit
            batch_cnt += 1

    print('[output_in_1nn_batch] All done in %s secs.' % str(time.time() - timer_start))


def output_in_1nn_ttables_from_tneanet(tneanet_file_path, graph_name_suffix):
    print('[output_in_1nn_ttables_from_tneanet] Starts.')
    timer_start = time.time()

    fd_in = snap.TFIn(tneanet_file_path)
    tneanet_ins = snap.TNEANet.Load(fd_in)

    context = snap.TTableContext()
    node_ttable = snap.TTable.GetNodeTable(tneanet_ins, context)
    edge_ttable = snap.TTable.GetEdgeTable(tneanet_ins, context)

    fd_out = snap.TFOut(''.join([g_epihiper_output_folder, 'node_ttable_%s.bin' % graph_name_suffix]))
    node_ttable.Save(fd_out)
    fd_out.Flush()
    fd_out = snap.TFOut(''.join([g_epihiper_output_folder, 'edge_ttable_%s.bin' % graph_name_suffix]))
    edge_ttable.Save(fd_out)
    fd_out.Flush()
    print('[output_in_1nn_ttables_from_tneanet] All done for %s in %s secs.'
          % (graph_name_suffix, time.time() - timer_start))


def output_in_1nn_ttables(neo4j_driver, df_output_pid_over_time, graph_name_suffix):
    """
    Read in subsets of nodes over time specified by 'df_output_pid_over_time', and output TTable files (including
    a node table and an edge table for each graph) for the time points.
    TODO
        SNAP does not support converting a TNEANet graph to TTables directly. So we take the query output from Neo4j,
        and output a CSV file for nodes and another CSV file for edges. Then we load in the CSV files for nodes and
        edges, and output them as TTable files respectively.
        Can we make this easier?
    !!!CAUTION!!!
        Seems that SNAP 6.0 has some bugs in loading in CSV files and constructing TTables. The attributes in a CSV
        file cannot be all integer type. Otherwise, it fails. See issue here:
        https://github.com/NSSAC/EpiHiper-network_analytics/issues/3
    """
    print('[output_in_1nn_ttables] Starts.')
    timer_start = time.time()

    neo4j_session_config = {'database': g_neo4j_db_name}
    query_str = '''with $l_core_pid as l_core_pid, $tick as tick
                   match (t:PERSON) where t.pid in l_core_pid
                   match (s:PERSON)-[r:CONTACT]->(t) where r.occur = tick
                   return s, t, r
                '''
    node_attr = ['pid', 'hid', 'age', 'age_group', 'gender', 'home_lat', 'home_lon', 'fips', 'admin1', 'admin2',
                 'admin3', 'admin4']
    edge_attr = ['duration', 'src_act', 'trg_act']

    edgeschema = snap.Schema()
    edgeschema.Add(snap.TStrTAttrPr("duration", snap.atInt))
    edgeschema.Add(snap.TStrTAttrPr("src_act", snap.atStr))
    edgeschema.Add(snap.TStrTAttrPr("trg_act", snap.atStr))

    nodeschema = snap.Schema()
    nodeschema.Add(snap.TStrTAttrPr("pid", snap.atInt))
    nodeschema.Add(snap.TStrTAttrPr("hid", snap.atInt))
    nodeschema.Add(snap.TStrTAttrPr("age", snap.atInt))
    nodeschema.Add(snap.TStrTAttrPr("age_group", snap.atStr))
    nodeschema.Add(snap.TStrTAttrPr("gender", snap.atInt))
    nodeschema.Add(snap.TStrTAttrPr("home_lat", snap.atFlt))
    nodeschema.Add(snap.TStrTAttrPr("home_lon", snap.atFlt))
    nodeschema.Add(snap.TStrTAttrPr("fips", snap.atStr))
    nodeschema.Add(snap.TStrTAttrPr("admin1", snap.atStr))
    nodeschema.Add(snap.TStrTAttrPr("admin2", snap.atStr))
    nodeschema.Add(snap.TStrTAttrPr("admin3", snap.atStr))
    nodeschema.Add(snap.TStrTAttrPr("admin4", snap.atStr))

    for tick, pid_rec in df_output_pid_over_time.iterrows():
        l_core_pids = pid_rec['pid']
        query_param = {'l_core_pid': l_core_pids, 'tick': tick}
        l_ret = execute_neo4j_queries(neo4j_driver, neo4j_session_config, [query_str], l_query_param=[query_param],
                                      need_ret=True)
        if len(l_ret[0]) <= 0:
            continue

        node_csv_file_name = ''.join([g_epihiper_output_folder, 'tmp_node_%s_%s.csv' % (graph_name_suffix, tick)])
        l_node_rec = [ret[0] for ret in l_ret[0]] + [ret[1] for ret in l_ret[0]]
        df_node = pd.DataFrame(l_node_rec)
        df_node.drop_duplicates(subset=['pid'], inplace=True)
        df_node.to_csv(node_csv_file_name, columns=node_attr, index=False)
        print('[output_in_1nn_ttables] Output node CSV for tick %s' % str(tick))

        edge_csv_file_name = ''.join([g_epihiper_output_folder, 'tmp_edge_%s_%s.csv' % (graph_name_suffix, tick)])
        l_edge_rec = [ret[2] for ret in l_ret[0]]
        df_edge = pd.DataFrame(l_edge_rec)
        df_edge.to_csv(edge_csv_file_name, columns=edge_attr, index=False)
        print('[output_in_1nn_ttables] Output edge CSV for tick %s' % str(tick))

        context = snap.TTableContext()
        node_ttable = snap.TTable.LoadSS(nodeschema, node_csv_file_name, context, ",", snap.TBool(True))
        edge_ttable = snap.TTable.LoadSS(edgeschema, edge_csv_file_name, context, ",", snap.TBool(True))

        os.remove(node_csv_file_name)
        os.remove(edge_csv_file_name)

        node_ttable_file_name = ''.join([g_epihiper_output_folder, 'node_ttable_%s_%s.bin' % (graph_name_suffix, tick)])
        edge_ttable_file_name = ''.join([g_epihiper_output_folder, 'edge_ttable_%s_%s.bin' % (graph_name_suffix, tick)])
        fd_out = snap.TFOut(node_ttable_file_name)
        node_ttable.Save(fd_out)
        fd_out.Flush()
        fd_out = snap.TFOut(edge_ttable_file_name)
        edge_ttable.Save(fd_out)
        fd_out.Flush()

        print('[output_in_1nn_ttables] Output node and edge TTables for tick %s in %s secs.'
              % (tick, time.time() - timer_start))
    print('[output_in_1nn_ttables] All done in %s secs.' % str(time.time() - timer_start))


if __name__ == '__main__':
    ############################################################
    #   USAGE
    #   # PURGE DB
    #   > python neo4j_test.py "neo4j_driver->purge_db"
    #   # CREATE DB
    #   > python neo4j_test.py "neo4j_driver->create_db->create_constraints->create_constraints_person_trait->create_indexes->create_indexes_person_trait"
    #   # CREATE INITIAL CONTACT NETWORK
    #   > python neo4j_test.py "neo4j_driver->build_init_cn"
    #   # CREATE INTERMEDIATE CONTACT NETWORKS
    #   > python neo4j_test.py "neo4j_driver->build_int_cn"
    #   # CREATE EPIHIPER OUTPUT DATABASE
    #   > python neo4j_test.py "create_epihiper_output_db->load_epihiper_output_data->create_epihiper_output_db_indexes"
    #   # DURATION DISTRIBUTION W.R.T. EXIT STATE
    #   > python neo4j_test.py "fetch_pids_by_exit_state->neo4j_driver->duration_distribution"
    #   # OUTPUT IN-1NN SNAP GRAPHS FOR EXIT STATE
    #   > python neo4j_test.py "neo4j_driver->output_in_1nn"
    #   # OUTPUT IN-1NN NODE AND EDGE TTABLES FOR EXIT STATE
    #   > python neo4j_test.py "neo4j_driver->output_in_1nn_ttables"
    #   # OUTPUT IN-1NN NODE AND EDGE TTABLES FOR EXIT STATE FROM TNEANET
    #   > python neo4j_test.py "output_in_1nn_ttables_from_tneanet"
    #   NOTE
    #   1. All commands should be linked by '->'.
    #   2. The order of commands matters.
    ############################################################
    logging.basicConfig(level=logging.ERROR)

    neo4j_hostname = sys.argv[1]
    g_neo4j_server_uri = g_neo4j_server_uri_fmt.format(neo4j_hostname)

    if g_neo4j_server_uri is None:
        print('[main] Please set Neo4j server URI to "g_neo4j_server_uri".')
    if g_neo4j_username is None:
        print('[main] Please set username to "g_neo4j_username" if any. Ignore this if authentication is disabled.')
    if g_neo4j_password is None:
        print('[main] Please set password to "g_neo4j_password" if any. Ignore this if authentication is disabled.')
    if g_neo4j_db_name is None:
        print('[main] Please set DB name to "g_neo4j_db_name".')

    cmd_pipeline = sys.argv[2]
    l_cmd = [cmd.strip().lower() for cmd in cmd_pipeline.split('->')]
    print('[main] Commands to be executed: %s' % l_cmd)

    df_contact_network_sample = None
    df_person_trait = None
    neo4j_driver = None

    for cmd in l_cmd:
        # LOAD IN DATA SAMPLES
        if cmd == 'load_init_cn':
            print('[main] load_init_cn starts.')
            df_contact_network_sample = load_contact_network_samples(g_init_cn_path, g_sample_cnt)
            df_person_trait = load_person_trait(g_person_trait_path)
            print('[main] load_init_cn done.')

        # CONNECT TO NEO4J DRIVER
        elif cmd == 'neo4j_driver':
            print('[main] neo4j_driver starts.')
            neo4j_driver = connect_to_neo4j_driver(g_neo4j_server_uri, (g_neo4j_username, g_neo4j_password),
                                                   {'max_connection_lifetime': 1000})
            print('[main] neo4j_driver done.')

        # CREATE NEO4J DB
        # NOTE: Every query execution needs a session
        # NOTE: Neo4j does NOT support symbols well in database naming, and it is actually case-insensitive.
        #       These are different from their documents.
        elif cmd == 'create_db':
            print('[main] create_db starts.')
            if neo4j_driver is None:
                raise Exception('[main] neo4j_driver is None. Run "create_driver" first.')
            query_str = '''create database {0} if not exists'''.format(g_neo4j_db_name)
            execute_neo4j_queries(neo4j_driver, None, [query_str])
            print('[main] create_db done.')

        # CREATE CONSTRAINTS
        elif cmd == 'create_constraints':
            print('[main] create_constraints starts.')
            neo4j_session_config = {'database': g_neo4j_db_name}
            # Existence of the 'pid' property of node
            query_str_constraint_1 = '''create constraint pid_exist if not exists
                                        on (n:PERSON)
                                        assert n.pid is not null'''
            # Uniqueness of the 'pid' property of node
            query_str_constraint_2 = '''create constraint pid_unique if not exists
                                        on (n:PERSON)
                                        assert n.pid is unique'''
            # Existence of the 'duration' property of edge
            query_str_constraint_3 = '''create constraint duration_exist if not exists
                                        on ()-[r:CONTACT]-()
                                        assert r.duration is not null'''
            # Existence of the 'src_act' property of edge (i.e. sourceActivity)
            query_str_constraint_4 = '''create constraint src_act_exist if not exists
                                        on ()-[r:CONTACT]-()
                                        assert r.src_act is not null'''
            # Existence of the 'trg_act' property of edge (i.e. targetActivity)
            query_str_constraint_5 = '''create constraint trg_act_exist if not exists
                                        on ()-[r:CONTACT]-()
                                        assert r.trg_act is not null'''
            # Existence of the 'occur' property of edge
            query_str_constraint_6 = '''create constraint occur_exist if not exists
                                        on ()-[r:CONTACT]-()
                                        assert r.occur is not null'''
            execute_neo4j_queries(neo4j_driver, neo4j_session_config, [query_str_constraint_1,
                                                                       query_str_constraint_2,
                                                                       query_str_constraint_3,
                                                                       query_str_constraint_4,
                                                                       query_str_constraint_5,
                                                                       query_str_constraint_6])
            print('[main] create_constraints done.')

        # CREATE CONSTRAINTS FOR PERSON TRAIT
        elif cmd == 'create_constraints_person_trait':
            print('[main] create_constraints_person_trait starts.')
            neo4j_session_config = {'database': g_neo4j_db_name}
            # Existence of the 'hid' property of node
            query_str_constraint_1 = '''create constraint hid_exist if not exists
                                        on (n:PERSON)
                                        assert n.hid is not null'''
            # Existence of the 'age' property of node
            query_str_constraint_2 = '''create constraint age_exist if not exists
                                        on (n:PERSON)
                                        assert n.age is not null'''
            # Existence of the 'age_group' property of node
            query_str_constraint_3 = '''create constraint age_group_exist if not exists
                                        on (n:PERSON)
                                        assert n.age_group is not null'''
            execute_neo4j_queries(neo4j_driver, neo4j_session_config, [query_str_constraint_1,
                                                                       query_str_constraint_2,
                                                                       query_str_constraint_3])
            print('[main] create_constraints_person_trait done.')

        # CREATE INDEXES
        elif cmd == 'create_indexes':
            print('[main] create_indexes starts.')
            neo4j_session_config = {'database': g_neo4j_db_name}
            # Index for 'duration'
            query_str_idx_1 = '''create btree index idx_duration if not exists for ()-[r:CONTACT]-() on (r.duration)'''
            # Index for 'src_act'
            query_str_idx_2 = '''create index idx_src_act if not exists for ()-[r:CONTACT]-() on (r.src_act)'''
            # Index for 'trg_act'
            query_str_idx_3 = '''create index idx_trg_act if not exists for ()-[r:CONTACT]-() on (r.trg_act)'''
            # Index for 'occur'
            query_str_idx_4 = '''create btree index idx_occur if not exists for ()-[r:CONTACT]->() on (r.occur)'''
            execute_neo4j_queries(neo4j_driver, neo4j_session_config, [query_str_idx_1, query_str_idx_2,
                                                                       query_str_idx_3, query_str_idx_4])
            print('[main] create_indexes done.')

        # CREATE INDEXES FOR PERSON TRAIT
        elif cmd == 'create_indexes_person_trait':
            print('[main] create_indexes_person_trait starts.')
            neo4j_session_config = {'database': g_neo4j_db_name}
            # Index for 'age'
            query_str_idx_1 = '''create btree index idx_age if not exists for (n:PERSON) on (n.age)'''
            # Index for 'age_group'
            query_str_idx_2 = '''create index idx_age_group if not exists for (n:PERSON) on (n.age_group)'''
            # Index for 'gender'
            query_str_idx_3 = '''create index idx_gender if not exists for (n:PERSON) on (n.age_group)'''
            # Index for 'fips'
            query_str_idx_4 = '''create index idx_fips if not exists for (n:PERSON) on (n.fips)'''
            execute_neo4j_queries(neo4j_driver, neo4j_session_config, [query_str_idx_1, query_str_idx_2,
                                                                       query_str_idx_3, query_str_idx_4])
            print('[main] create_indexes_person_trait done.')

        # DELETE EVERYTHING IN DB
        elif cmd == 'purge_db':
            print('[main] purge_db starts.')
            neo4j_session_config = {'database': g_neo4j_db_name}
            # Remove all data
            query_str = '''match (s)-[r]->(t) delete r, s, t'''
            execute_neo4j_queries(neo4j_driver, neo4j_session_config, [query_str])
            # Remove all constraints and some related indexes
            query_str = '''call apoc.schema.assert({}, {}, true)'''
            execute_neo4j_queries(neo4j_driver, neo4j_session_config, [query_str])
            # Remove all other indexes if any
            l_index_name = ['idx_duration', 'idx_src_act', 'idx_trg_act', 'idx_age', 'idx_occur', 'idx_age_group',
                            'idx_gender', 'idx_fips']
            query_str = '''drop index {0} if exists'''
            l_query_str = [query_str.format(index_name) for index_name in l_index_name]
            execute_neo4j_queries(neo4j_driver, neo4j_session_config, l_query_str)
            print('[main] purge_db done.')

        # BUILD GRAPH BY USING "MERGE" WITH SINGLE BATCH
        elif cmd == 'build_graph_by_merge':
            print('[main] build_graph_by_merge starts.')
            neo4j_session_config = {'database': g_neo4j_db_name}
            query_str = '''unwind $rec as rec
                           merge (src: PERSON {pid: rec.sourcePID})
                           merge (trg: PERSON {pid: rec.targetPID})
                           merge (src)-[r: CONTACT {src_act: rec.sourceActivity, trg_act: rec.targetActivity,
                           duration: rec.duration}]->(trg)
                        '''
            query_param = {'rec': df_contact_network_sample.to_dict('records')}
            execute_neo4j_queries(neo4j_driver, neo4j_session_config, [query_str], l_query_param=[query_param])
            print('[main] build_graph_by_merge done.')

        # BUILD GRAPH BY USING "MERGE" WITH MULTIPLE BATCHES
        elif cmd == 'build_graph_by_merge_batch':
            print('[main] build_graph_by_merge_batch starts.')
            num_batch = 20
            neo4j_session_config = {'database': g_neo4j_db_name}
            query_str = '''unwind $rec as rec
                           merge (src: PERSON {pid: rec.sourcePID}
                           merge (trg: PERSON {pid: rec.targetPID})
                           merge (src)-[r: CONTACT {src_act: rec.sourceActivity, trg_act: rec.targetActivity,
                           duration: rec.duration}]->(trg)
                        '''
            batch_size = math.ceil(len(df_contact_network_sample) / num_batch)
            timer_start = time.time()
            cur_pointer = 0
            while cur_pointer < len(df_contact_network_sample):
                query_param = {'rec': df_contact_network_sample[cur_pointer:cur_pointer + batch_size].to_dict('records')}
                execute_neo4j_queries(neo4j_driver, neo4j_session_config, [query_str], l_query_param=[query_param])
                cur_pointer += batch_size
                print('%s recs committed in %s secs.' % (cur_pointer, time.time() - timer_start))
            print('All done in %s secs.' % str(time.time() - timer_start))
            print('[main] build_graph_by_merge_batch done.')

        # BUILD GRAPH BY USING "CREATE"
        elif cmd == 'build_graph_by_create':
            print('[main] build_graph_by_create starts.')
            timer_start = time.time()
            neo4j_session_config = {'database': g_neo4j_db_name}
            query_str = '''unwind $rec as rec
                           create (n:PERSON {pid: rec})'''
            query_param = {'rec': list(set(df_contact_network_sample['targetPID'].to_list()).union(df_contact_network_sample['sourcePID'].to_list()))}
            execute_neo4j_queries(neo4j_driver, neo4j_session_config, [query_str], l_query_param=[query_param])
            query_str = '''unwind $rec as rec
                           match (src:PERSON), (trg:PERSON)
                           where src.pid=rec.sourcePID and trg.pid=rec.targetPID
                           create (src)-[r:CONTACT {src_act:rec.sourceActivity, trg_act:rec.targetActivity,
                           duration:rec.duration}]->(trg)
                        '''
            query_param = {'rec': df_contact_network_sample.to_dict('records')}
            execute_neo4j_queries(neo4j_driver, neo4j_session_config, [query_str], l_query_param=[query_param])
            print('Running time: %s' % str(time.time() - timer_start))
            print('[main] build_graph_by_create done.')

        # BUILD INITIAL CONTACT NETWORK WITH PRESON TRAIT
        elif cmd == 'build_init_cn':
            print('[main] build_init_cn starts.')
            create_init_cn(neo4j_driver)
            print('[main] build_init_cn done.')

        # BUILD INTERMEDIATE CONTACT NETWORKS
        elif cmd == 'build_int_cn':
            print('[main] build_int_cn starts.')
            int_cn_cnt = 10
            create_int_cn(neo4j_driver, int_cn_cnt)
            print('[main] build_int_cn done.')

        elif cmd == 'build_int_cn_auto_search':
            print('[main] build_int_cn_auto_search starts.')
            search_folder = g_epihiper_output_folder
            create_int_cn_auto_search(neo4j_driver, search_folder, int_cn_batch_size=1000000)
            print('[main] build_int_cn_auto_search done.')

        # CREATE EPIHIPER OUTPUT SQLITE DATABASE
        elif cmd == 'create_epihiper_output_db':
            print('[main] create_epihiper_output_db starts.')
            create_epihiper_output_db()
            print('[main] create_epihiper_output_db done.')

        # LOAD EPIHIPER OUTPUT DATA
        elif cmd == 'load_epihiper_output_data':
            print('[main] load_epihiper_output_data starts.')
            load_epihiper_output_to_db()
            print('[main] load_epihiper_output_data done.')

        # CREATE INDEXES ON EPIHIPER OUTPUT DATABASE
        elif cmd == 'create_epihiper_output_db_indexes':
            print('[main] create_epihiper_output_db_indexes starts.')
            create_indexes_on_epihipter_output_db()
            print('[main] create_epihiper_output_db_indexes done.')

        # FETCH PIDs OVER TIME FROM EPIHIPER OUTPUT DATABASE FOR A GIVEN EXIT STATE
        elif cmd == 'fetch_pids_by_exit_state':
            print('[main] fetch_pids_by_exit_state starts.')
            exit_state = 'Isymp_s'
            fetch_pids_by_exit_state(exit_state)
            print('[main] fetch_pids_by_exit_state done.')

        # COMPUTE DISTRIBUTION OF DURATION OVER TIME
        elif cmd == 'duration_distribution':
            print('[main] duration_distribution starts.')
            exit_state = 'Isymp_s'
            df_output_pid_over_time = pd.read_pickle(''.join([g_epihiper_output_folder,
                                                              'output_pid_over_time_by_%s.pickle' % exit_state]))
            duration_distribution(neo4j_driver, df_output_pid_over_time, exit_state)
            print('[main] duration_distribution done.')

        # OUTPUT TNEANET GRAPHS TO FILES
        elif cmd == 'output_in_1nn':
            print('[main] output_in_1nn starts.')
            exit_state = 'Isymp_s'
            df_output_pid_over_time = pd.read_pickle(''.join([g_epihiper_output_folder,
                                                              'output_pid_over_time_by_%s.pickle' % exit_state]))
            output_in_1nn(neo4j_driver, df_output_pid_over_time, exit_state)
            print('[main] output_in_1nn done.')

        # OUTPUT BATCHED TNEANET GRAPHS TO FILES
        elif cmd == 'output_in_1nn_batch':
            print('[main] output_in_1nn_batch starts.')
            exit_state = 'Isymp_s'
            df_output_pid_over_time = pd.read_pickle(''.join([g_epihiper_output_folder,
                                                              'output_pid_over_time_by_%s.pickle' % exit_state]))
            batch_size = 100
            output_in_1nn_batch(neo4j_driver, df_output_pid_over_time, batch_size, exit_state)
            print('[main] output_in_1nn done.')

        # OUTPUT TTABLES FOR NODES AND EDGES
        elif cmd == 'output_in_1nn_ttables':
            print('[main] output_in_1nn_ttables starts.')
            exit_state = 'Isymp_s'
            df_output_pid_over_time = pd.read_pickle(''.join([g_epihiper_output_folder,
                                                              'output_pid_over_time_by_%s.pickle' % exit_state]))
            output_in_1nn_ttables(neo4j_driver, df_output_pid_over_time, exit_state)
            print('[main] output_in_1nn_ttables done.')

        # OUTPUT TTABLES FOR NODES AND EDGES FROM TNEANET INSTANCE
        elif cmd == 'output_in_1nn_ttables_from_tneanet':
            print('[main] output_in_1nn_ttables_from_tneanet starts.')
            exit_state = 'Isymp_s'
            for tick in [5, 6, 7, 8, 9]:
                graph_name_suffix = '%s_t%s' % (exit_state, tick)
                tneanet_file_path = ''.join([g_epihiper_output_folder, 'in_1nn_%s.snap_graph' % graph_name_suffix])
                output_in_1nn_ttables_from_tneanet(tneanet_file_path, graph_name_suffix)
            print('[main] output_in_1nn_ttables_from_tneanet done.')

        # QUERY POPULATION DISTRIBUTION GROUPED BY age_group
        elif cmd == 'population_dist_by_age_group':
            print('[main] population_dist_by_age_group starts.')
            timer_start = time.time()
            neo4j_session_config = {'database': g_neo4j_db_name}
            query_str = '''match (n)
                           with distinct n.age_group as age_group
                           unwind age_group as each_age_group
                           match (m)
                           where m.age_group = each_age_group
                           return each_age_group, count(m)'''
            ret = execute_neo4j_queries(neo4j_driver, neo4j_session_config, [query_str], need_ret=True)
            print(ret)
            print('Running time: %s' % str(time.time() - timer_start))
            print('[main] population_dist_by_age_group done.')

        # QUERY SOURCE ACTIVITY DISTRIBUTION
        elif cmd == 'src_act_dist':
            print('[main] src_act_dist starts.')
            timer_start = time.time()
            neo4j_session_config = {'database': g_neo4j_db_name}
            query_str = '''match ()-[r]->()
                           with distinct r.src_act as src_act
                           unwind src_act as each_src_act
                           match ()-[q]->()
                           where q.src_act = each_src_act
                           return each_src_act, count(q)'''
            execute_neo4j_queries(neo4j_driver, neo4j_session_config, [query_str])
            print('Running time: %s' % str(time.time() - timer_start))
            print('[main] src_act_dist done.')

        elif cmd == 'infect_in_deg_dist_at_t':
            print('[main] infect_in_deg_dist_at_t starts.')
            timer_start = time.time()
            t = 14
            neo4j_session_config = {'database': g_neo4j_db_name}
            df_output = load_epihiper_output(g_epihiper_output_path)
            df_output = df_output.set_index('tick')
            l_infect_pid = list(set(df_output.loc[t]['pid'].to_list()))
            print('Running time: %s' % str(time.time() - timer_start))
            query_str = '''unwind $infect_pid as infect_pid
                           match (n:PERSON {pid: infect_pid})
                           return infect_pid, apoc.node.degree(n, "<CONTACT")'''
            query_param = {'infect_pid': l_infect_pid}
            ret = execute_neo4j_queries(neo4j_driver, neo4j_session_config, [query_str], l_query_param=[query_param])
            print([item for item in ret[0] if item['apoc.node.degree(n, "<CONTACT")'] > 0])
            print('[main] infect_in_deg_dist_at_t done.')


    print('Finished.')