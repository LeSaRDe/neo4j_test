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
import sys
import time
import math

import pandas as pd
from neo4j import GraphDatabase


g_ds_path = None
g_person_trait_path = None
g_sample_cnt = 10000

g_neo4j_server_uri = None
g_neo4j_username = None
g_neo4j_password = None

g_neo4j_db_name = None


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
    logging.debug('[load_in_arxiv_samples] Starts.')
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
    logging.debug('[load_in_arxiv_samples] All done.')
    return df_sample


def load_contact_network_samples(ds_path, g_sample_cnt):
    """
    Note that the first line is the schema and the second line is the header.
    :return: Pandas DataFrame
    """
    logging.debug('[load_contact_network_samples] Starts.')
    if g_sample_cnt <= 2:
        raise Exception('[load_contact_network_samples] g_sample_cnt needs to be greater than 2.')

    l_rec = []
    with open(ds_path, 'r') as in_fd:
        csv_reader = csv.reader(in_fd, delimiter=',')
        ln_cnt = 0
        for row_idx, row in enumerate(csv_reader):
            if row_idx == 0 or row_idx == 1:
                continue
            else:
                if ln_cnt >= g_sample_cnt:
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
    logging.debug('[load_contact_network_samples] All done with %s records.' % len(df_contact_network))
    return df_contact_network


def load_person_trait(ds_path):
    """
    Note that the first line is the schema and the second line is the header.
    :return: Pandas DataFrame
    """
    logging.debug('[load_person_trait] Starts.')

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
    logging.debug('[load_person_trait] All done with %s records.' % len(df_person_trait))
    return df_person_trait


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
    logging.debug('[connect_to_neo4j_driver] Connecting to %s...' % uri)
    if uri is None or len(uri) <= 0:
        return None

    try:
        driver = GraphDatabase.driver(uri=uri, auth=auth, **kwargs)
    except Exception as e:
        logging.error('[connect_to_neo4j_driver] Failed to connect to Neo4j driver: %s' % e)
        return None

    logging.debug('[connect_to_neo4j_driver] Connection established.')
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
    logging.debug('[get_neo4j_session] Starts.')

    try:
        neo4j_session = neo4j_driver.session(**session_config)
    except Exception as e:
        logging.error('[get_neo4j_session] Failed to create Neo4j session: %s' % e)
        return None

    logging.debug('[get_neo4j_session] Create a Neo4j session.')
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
        logging.debug('[execute_neo4j_query] No query is available.')
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
                    l_ret.append(results.data())
            neo4j_tx.commit()
        neo4j_session.close()
    except Exception as e:
        logging.error('[execute_neo4j_query] Failed query: %s' % e)
        return None

    logging.debug('[execute_neo4j_query] All done in %s secs.' % str(time.time() - timer_start))
    if need_ret:
        return l_ret
    else:
        return None


if __name__ == '__main__':
    ############################################################
    #   USAGE
    #   > python neo4j_test.py "load_data->neo4j_driver->create_db->create_constraints
    #   ->create_constraints_person_trait->create_indexes->create_indexes_person_trait->build_contact_network"
    #   NOTE
    #   1. All commands should be linked by '->'.
    #   2. The order of commands matter.
    ############################################################
    logging.basicConfig(level=logging.DEBUG)

    if g_neo4j_server_uri is None:
        print('[main] Please set Neo4j server URI to "g_neo4j_server_uri".')
    if g_neo4j_username is None:
        print('[main] Please set username to "g_neo4j_username" if any. Ignore this if authentication is disabled.')
    if g_neo4j_password is None:
        print('[main] Please set password to "g_neo4j_password" if any. Ignore this if authentication is disabled.')
    if g_neo4j_db_name is None:
        print('[main] Please set DB name to "g_neo4j_db_name".')

    cmd_pipeline = sys.argv[1]
    l_cmd = [cmd.strip().lower() for cmd in cmd_pipeline.split('->')]
    logging.debug('[main] Commands to be executed: %s' % l_cmd)

    df_contact_network_sample = None
    df_person_trait = None
    neo4j_driver = None

    for cmd in l_cmd:
        # LOAD IN DATA SAMPLES
        if cmd == 'load_data':
            logging.debug('[main] load_data starts.')
            df_contact_network_sample = load_contact_network_samples(g_ds_path, g_sample_cnt)
            df_person_trait = load_person_trait(g_person_trait_path)
            logging.debug('[main] load_data done.')

        # CONNECT TO NEO4J DRIVER
        elif cmd == 'neo4j_driver':
            logging.debug('[main] neo4j_driver starts.')
            neo4j_driver = connect_to_neo4j_driver(g_neo4j_server_uri, (g_neo4j_username, g_neo4j_password),
                                                   {'max_connection_lifetime': 1000})
            logging.debug('[main] neo4j_driver done.')

        # CREATE NEO4J DB
        # NOTE: Every query execution needs a session
        # NOTE: Neo4j does NOT support symbols well in database naming, and it is actually case-insensitive.
        #       These are different from their documents.
        elif cmd == 'create_db':
            logging.debug('[main] create_db starts.')
            if neo4j_driver is None:
                raise Exception('[main] neo4j_driver is None. Run "create_driver" first.')
            query_str = '''create database {0}'''.format(g_neo4j_db_name)
            execute_neo4j_queries(neo4j_driver, None, [query_str])
            logging.debug('[main] create_db done.')

        # CREATE CONSTRAINTS
        elif cmd == 'create_constraints':
            logging.debug('[main] create_constraints starts.')
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
            execute_neo4j_queries(neo4j_driver, neo4j_session_config, [query_str_constraint_1,
                                                                       query_str_constraint_2,
                                                                       query_str_constraint_3,
                                                                       query_str_constraint_4,
                                                                       query_str_constraint_5])
            logging.debug('[main] create_constraints done.')

        # CREATE CONSTRAINTS FOR PERSON TRAIT
        elif cmd == 'create_constraints_person_trait':
            logging.debug('[main] create_constraints_person_trait starts.')
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
            logging.debug('[main] create_constraints_person_trait done.')

        # CREATE INDEXES
        elif cmd == 'create_indexes':
            logging.debug('[main] create_indexes starts.')
            neo4j_session_config = {'database': g_neo4j_db_name}
            # Index for 'duration'
            query_str_idx_1 = '''create btree index idx_duration if not exists for ()-[r:CONTACT]-() on (r.duration)'''
            # Index for 'src_act'
            query_str_idx_2 = '''create index idx_src_act if not exists for ()-[r:CONTACT]-() on (r.src_act)'''
            # Index for 'trg_act'
            query_str_idx_3 = '''create index idx_trg_act if not exists for ()-[r:CONTACT]-() on (r.trg_act)'''
            # Index for 'age'
            query_str_idx_4 = '''create btree index idx_age if not exists for (n:PERSON) on (n.age)'''
            # Index for
            execute_neo4j_queries(neo4j_driver, neo4j_session_config, [query_str_idx_1, query_str_idx_2,
                                                                       query_str_idx_3])
            logging.debug('[main] create_indexes done.')

        # CREATE INDEXES FOR PERSON TRAIT
        elif cmd == 'create_indexes_person_trait':
            logging.debug('[main] create_indexes_person_trait starts.')
            neo4j_session_config = {'database': g_neo4j_db_name}
            # Index for 'age'
            query_str_idx_1 = '''create btree index idx_age if not exists for (n:PERSON) on (n.age)'''
            # Index for 'age_group'
            query_str_idx_2 = '''create index idx_age_group if not exists for (n:PERSON) on (n.age_group)'''
            # Index for 'gender'
            query_str_idx_3 = '''create index idx_gender if not exists for (n:PERSON) on (n.age_group)'''
            # Index for 'fips'
            query_str_idx_4 = '''create index idx_fips if not exists for (n:PERSON) on (n.fips)'''
            # Index for
            execute_neo4j_queries(neo4j_driver, neo4j_session_config, [query_str_idx_1, query_str_idx_2,
                                                                       query_str_idx_3, query_str_idx_4])
            logging.debug('[main] create_indexes_person_trait done.')

        # DELETE EVERYTHING IN DB
        elif cmd == 'purge_db':
            logging.debug('[main] purge_db starts.')
            neo4j_session_config = {'database': g_neo4j_db_name}
            # Remove all data
            query_str = '''match (s)-[r]->(t) delete r, s, t'''
            execute_neo4j_queries(neo4j_driver, neo4j_session_config, [query_str])
            # Remove all constraints and some related indexes
            query_str = '''call apoc.schema.assert({}, {}, true)'''
            execute_neo4j_queries(neo4j_driver, neo4j_session_config, [query_str])
            # Remove all other indexes if any
            l_index_name = ['idx_duration', 'idx_src_act', 'idx_trg_act']
            query_str = '''drop index {0} if exists'''
            l_query_str = [query_str.format(index_name) for index_name in l_index_name]
            execute_neo4j_queries(neo4j_driver, neo4j_session_config, l_query_str)
            logging.debug('[main] purge_db done.')

        # BUILD GRAPH BY USING "MERGE" WITH SINGLE BATCH
        elif cmd == 'build_graph_by_merge':
            logging.debug('[main] build_graph_by_merge starts.')
            neo4j_session_config = {'database': g_neo4j_db_name}
            query_str = '''unwind $rec as rec
                           merge (src: PERSON {pid: rec.sourcePID})
                           merge (trg: PERSON {pid: rec.targetPID})
                           merge (src)-[r: CONTACT {src_act: rec.sourceActivity, trg_act: rec.targetActivity,
                           duration: rec.duration}]->(trg)
                        '''
            query_param = {'rec': df_contact_network_sample.to_dict('records')}
            execute_neo4j_queries(neo4j_driver, neo4j_session_config, [query_str], l_query_param=[query_param])
            logging.debug('[main] build_graph_by_merge done.')

        # BUILD GRAPH BY USING "MERGE" WITH MULTIPLE BATCHES
        elif cmd == 'build_graph_by_merge_batch':
            logging.debug('[main] build_graph_by_merge_batch starts.')
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
                logging.debug('%s recs committed in %s secs.' % (cur_pointer, time.time() - timer_start))
            logging.debug('All done in %s secs.' % str(time.time() - timer_start))
            logging.debug('[main] build_graph_by_merge_batch done.')

        # BUILD GRAPH BY USING "CREATE"
        elif cmd == 'build_graph_by_create':
            logging.debug('[main] build_graph_by_create starts.')
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
            logging.debug('Running time: %s' % str(time.time() - timer_start))
            logging.debug('[main] build_graph_by_create done.')

        # BUILD CONTACT NETWORK WITH PRESON TRAIT
        elif cmd == 'build_contact_network':
            logging.debug('[main] build_contact_network starts.')
            timer_start = time.time()
            neo4j_session_config = {'database': g_neo4j_db_name}
            query_str = '''unwind $rec as rec
                           create (n:PERSON {pid: rec.pid, hid: rec.hid, age: rec.age, age_group: rec.age_group, 
                           gender: rec.gender, fips: rec.fips, home_lat: rec.home_lat, home_lon: rec.home_lon, 
                           admin1: rec.admin1, admin2: rec.admin2, admin3: rec.admin3, admin4: rec.admin4})'''
            s_pid = set(df_contact_network_sample['targetPID'].to_list())\
                .union(df_contact_network_sample['sourcePID'].to_list())
            df_person = df_person_trait.loc[s_pid]
            df_person.reset_index(inplace=True)
            query_param = {'rec': df_person.to_dict('records')}
            execute_neo4j_queries(neo4j_driver, neo4j_session_config, [query_str], l_query_param=[query_param])
            query_str = '''unwind $rec as rec
                           match (src:PERSON), (trg:PERSON)
                           where src.pid=rec.sourcePID and trg.pid=rec.targetPID
                           create (src)-[r:CONTACT {src_act:rec.sourceActivity, trg_act:rec.targetActivity,
                           duration:rec.duration}]->(trg)
                        '''
            query_param = {'rec': df_contact_network_sample.to_dict('records')}
            execute_neo4j_queries(neo4j_driver, neo4j_session_config, [query_str], l_query_param=[query_param])
            logging.debug('Running time: %s' % str(time.time() - timer_start))
            logging.debug('[main] build_contact_network done.')

        # QUERY INCOMING DEGREE DISTRIBUTION GROUPED BY age_group
        elif cmd == 'in_deg_dist_by_age_group':
            logging.debug('[main] in_deg_dist_by_age_group starts.')
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
            logging.debug('Running time: %s' % str(time.time() - timer_start))
            logging.debug('[main] in_deg_dist_by_age_group done.')

        # QUERY SOURCE ACTIVITY DISTRIBUTION
        elif cmd == 'src_act_dist':
            logging.debug('[main] src_act_dist starts.')
            timer_start = time.time()
            neo4j_session_config = {'database': g_neo4j_db_name}
            query_str = '''match ()-[r]->()
                           with distinct r.src_act as src_act
                           unwind src_act as each_src_act
                           match ()-[q]->()
                           where q.src_act = each_src_act
                           return each_src_act, count(q)'''
            ret = execute_neo4j_queries(neo4j_driver, neo4j_session_config, [query_str], need_ret=True)
            print(ret)
            logging.debug('Running time: %s' % str(time.time() - timer_start))
            logging.debug('[main] src_act_dist done.')

    logging.debug('Finished.')