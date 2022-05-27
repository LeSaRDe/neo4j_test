"""
NOTE
    1. Data Structure for Contact Networks Stored in Neo4j
        Node Properties:
            pid: (int) Person ID (unique)
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
            occur: (int) Occurrence time stamp for this edge (-1 means the initial)
            duration: (int) Contact by second
            src_act: (str) Source Activity
            trg_act: (str) Target Activity

    2. Data Structure for Node and Edge TTables
        Node:
            Hidden ID:
                node_id: (int) Unique ID for nodes, same as the 'pid' attribute.
            Attibutes:
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
        Edge:
            Hidden ID:
                edg_id: (int) Unique ID for edges. Note that 'edg_id' is literally in this exact string!
            Hidden Attributes:
                src_id: (int) ID for the source node.
                dst_id: (int) ID for the destination nodes.
            Attributes:
                occur: (int) Occurrence time stamp for this edge (-1 means the initial)
                duration: (int) Contact by second
                src_act: (str) Source Activity
                trg_act: (str) Target Activity

    3. How to use this script
        - Check out __main__ beforehand for objective functions. The Community edition is different from the Enterprise
          edition in various perspectives. Check out all 'CAUTION' for the differences.
        - Check out all 'TODO', and make modifications when necessary.
"""

# import json
import logging
import csv
import os
import sys
import time
import math
import sqlite3
from os import path, walk
import re
import multiprocessing
import threading

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from neo4j import GraphDatabase
import snap


################################################################################
#   GLOBAL VARIABLES
################################################################################
# !!!CAUTION!!!
# Set up the edition before anything else going.
g_neo4j_edition = 'community'
# g_neo4j_edition = 'enterprise'

# TODO
# This value is the number of threads available for concurrency.
# Can be modified case by case.
g_concurrency = math.ceil(multiprocessing.cpu_count() * 0.8)
# g_concurrency = 1

g_neo4j_hostname_env_key = 'NEO4J_HOSTNAME'

g_state = 'wy'


g_init_cn_folder = '/project/biocomplexity/mf3jh/neo4j_workspace_%s_test/import/' % g_state
# TODO
# 'g_int_cn_folder' should be a folder under 'g_init_cn_folder'.
# Modify 'g_int_cn_folder' to the desired folder.
g_int_cn_folder = '%s_replicate_0' % g_state
g_int_cn_file_fmt = 'network_no_head_sorted_\d+'
g_epihiper_output_folder = '/project/biocomplexity/mf3jh/epihiper_data/'


# !!!CAUTION!!!
# Make sure that the files specified by 'g_init_cn_file_name', 'g_person_trait_file_name' and 'g_int_cn_file_fmt' are
# CSV files without schema, i.e. the first line of file should be the header and the rest is data.
# The following command can help remove the schema:
# ```
#   sed -e '1, 1d' sample_with_schema.csv > sample_no_schema.csv
# ```
# With '1, 1d', the first '1' means "starting from the 1st line", the second '1' means "up to the 1st line",
# and 'd' means removing all lines involved. As another example, '2, 10d' means removing the lines from the 2nd
# line to the 10th line inclusively.
#
# In addition, when using 'apoc' to load in edges, to avoid potential locking issues, the edge CSV file should be
# sorted first. And the following commands can help with sorting.
# ```
#   sed -e '1, 2d' sample_with_schema.csv > sample_pure_data.csv
#   sort --field-separator=',' --key=1,5 sample_pure_data.csv > sample_pure_data_sorted.csv
#   sed -i '1s/^/targetPID,targetActivity,sourcePID,sourceActivity,duration,LID\n/' sample_pure_data_sorted.csv
# ```
# We need to remove all headers beforehand, which is done by the first command. Then we sort the pure data using the
# second command. And finally we add the CSV header, not the schema, back to the beginning of the sorted file.
# With "--field-separator=','", each data record in the CSV file is separated into fields by comma,
# and with "--key=1,5" the sorting is performed upon the fields from the 1st to the 5th.

if g_state == 'wy':
    g_init_cn_file_name = 'wy_contact_network_config_m_5_M_40_a_1000_m-contact_0_with_lid_no_head_sorted.txt'
    g_person_trait_file_name = 'wy_persontrait_epihiper_no_head.txt'
elif g_state == 'va':
    g_init_cn_file_name = 'va_contact_network_config_m_5_M_40_a_1000_m-contact_0_with_lid_no_head_sorted.txt'
    g_person_trait_file_name = 'va_persontrait_epihiper_no_head.txt'
elif g_state == 'ny':
    g_init_cn_file_name = 'ny_contact_network_config_m_5_M_40_a_1000_m-contact_0_with_lid_no_head_sorted.txt'
    g_person_trait_file_name = 'ny_persontrait_epihiper_no_head.txt'
else:
    raise Exception('[global variables] Invalid g_state!')

g_init_cn_path = path.join(g_init_cn_folder, g_init_cn_file_name)
g_person_trait_path = path.join(g_init_cn_folder, g_person_trait_file_name)
g_epihiper_output_path = path.join(g_epihiper_output_folder, g_int_cn_folder, 'output.csv')
g_epihiper_output_db_path = path.join(g_epihiper_output_folder, g_int_cn_folder, 'output.db')

g_neo4j_server_uri = None
g_neo4j_server_uri_fmt = 'neo4j://{0}:7687'

# !!!CAUTION!!!
# If the Neo4j server is set with authentication disabled, the username and password are not necessary.
# Though, in this case, we don't have to change anything in code.
g_neo4j_username = ''
g_neo4j_password = ''

# !!!CAUTION!!!
# The Community edition dose not support creating databases. And 'neo4j' is the ONLY built-in database available to us.
if g_neo4j_edition == 'community':
    g_neo4j_db_name = 'neo4j'
elif g_neo4j_edition == 'enterprise':
    g_neo4j_db_name = 'cndb'

g_epihiper_output_tb_name = 'epihiper_output'


# Only for examle queries
g_example_query_folder = '/project/biocomplexity/mf3jh/example_queries/'
g_example_query_each_folder_fmt = path.join(g_example_query_folder, 'query_{0}')


################################################################################
#   NEO4J OPERATION FUNCTIONS
################################################################################
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
    logging.critical('[connect_to_neo4j_driver] Connecting to %s...' % uri)
    if uri is None or len(uri) <= 0:
        return None

    try:
        driver = GraphDatabase.driver(uri=uri, auth=auth, **kwargs)
    except Exception as e:
        logging.error('[connect_to_neo4j_driver] Failed to connect to Neo4j driver: %s' % e)
        return None

    logging.critical('[connect_to_neo4j_driver] Connection established.')
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
    # logging.critical('[get_neo4j_session] Starts.')

    try:
        if session_config is not None:
            neo4j_session = neo4j_driver.session(**session_config)
        else:
            neo4j_session = neo4j_driver.session()
    except Exception as e:
        logging.error('[get_neo4j_session] Failed to create Neo4j session: %s' % e)
        return None

    # logging.critical('[get_neo4j_session] Create a Neo4j session.')
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
        logging.critical('[execute_neo4j_query] No query is available.')
        return None
    if l_query_param is not None and len(l_query_str) != len(l_query_param):
        raise Exception('[execute_neo4j_query] l_query_param does not match l_query_str.')

    neo4j_session = get_neo4j_session(neo4j_driver, session_config=neo4j_session_config)

    # timer_start = time.time()
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

    # logging.critical('[execute_neo4j_query] All done in %s secs.' % str(time.time() - timer_start))
    if need_ret:
        return l_ret
    else:
        return None


def create_nodes_for_init_cn_by_create_method_single_task(task_id, neo4j_driver, neo4j_session_config,
                                                          l_node_data, init_cn_batch_size):
    """
    Given a set of node records, insert them into DB one by one, and commit batch by batch.
    """
    logging.critical('[create_nodes_for_init_cn_by_create_method_single_task] Task %s: Starts.' % str(task_id))
    logging.critical('[create_nodes_for_init_cn_by_create_method_single_task] Task %s: %s nodes to be created.'
                     % (task_id, len(l_node_data)))
    timer_start = time.time()
    query_str = '''unwind $rec as rec
                   create (n:PERSON {pid: rec.pid, hid: rec.hid, age: rec.age, age_group: rec.age_group, 
                   gender: rec.gender, fips: rec.fips, home_lat: rec.home_lat, home_lon: rec.home_lon, 
                   admin1: rec.admin1, admin2: rec.admin2, admin3: rec.admin3, admin4: rec.admin4})'''
    for i in range(0, len(l_node_data), init_cn_batch_size):
        query_param = {'rec': l_node_data[i: i + init_cn_batch_size]}
        execute_neo4j_queries(neo4j_driver, neo4j_session_config, [query_str], l_query_param=[query_param])
        logging.critical('[create_nodes_for_init_cn_by_create_method_single_task] Task %s: Created %s nodes in %s secs.'
                         % (task_id, len(l_node_data[i: i + init_cn_batch_size]), time.time() - timer_start))

    logging.critical('[create_nodes_for_init_cn_by_create_method_single_task] Task %s: All done in %s secs.'
                     % (task_id, time.time() - timer_start))


def create_nodes_for_init_cn(neo4j_driver, batch_size, method='apoc', task_carrier_type='thread'):
    """
    Create nodes for the initial contact graph.
    :param
        method: str
            - 'apoc': Directly call 'apoc.periodic.iterate()' to import data.
            - 'create': Create nodes one by one in code.
    :param
        task_carrier_type: str
            Meaningful only for the 'create' method.
            - 'thread': Use multithreading.
    NOTE:
        When using 'apoc', make sure the 'import' folder has been exposed, and the person trait file is available in
        this folder. Also, the import person trait file should be a straightforward CSV file, i.e. the first line
        should be header, and the rest is the data. The schema part should be removed if any.
    """
    logging.critical('[create_nodes_for_init_cn] Starts.')
    timer_start = time.time()

    # CONFIGURE NEO4J SESSION
    neo4j_session_config = {'database': g_neo4j_db_name}

    # CREATE NODES WITH PERSON TRAITS
    if method == 'apoc':
        query_str = \
            '''
            CALL apoc.periodic.iterate
            (
                "CALL apoc.load.csv('file:///%s', 
                                    {mapping:{pid:{type:'int'}, hid:{type:'int'}, age:{type:'int'}, 
                                     age_group:{type:'string'}, gender:{type:'int'}, county_fips:{type:'string'}, 
                                     home_latitude:{type:'float'}, home_longitude:{type:'float'}, 
                                     admin1:{type:'string'}, admin2:{type:'string'}, admin3:{type:'string'}, 
                                     admin4:{type:'string'}}}) 
                 yield map as rec return rec",
                "create (n:PERSON {pid: rec.pid, hid: rec.hid, age: rec.age, age_group: rec.age_group, 
                                   gender: rec.gender, fips: rec.county_fips, home_lat: rec.home_latitude, 
                                   home_lon: rec.home_longitude, admin1: rec.admin1, admin2: rec.admin2, 
                                   admin3: rec.admin3, admin4: rec.admin4})",
                {parallel:true, batchSize:%s, concurrency:%s}
            )
        ''' % (g_person_trait_file_name, batch_size, g_concurrency)
        execute_neo4j_queries(neo4j_driver, neo4j_session_config, [query_str])
        logging.critical('[create_nodes_for_init_cn] Creating nodes done in %s secs.' % str(time.time() - timer_start))

    elif method == 'create':
        # READ IN ALL NODE RECORDS
        l_person_trait = []
        with open(g_person_trait_path, 'r') as in_fd:
            csv_reader = csv.reader(in_fd, delimiter=',')
            for row_idx, row in enumerate(csv_reader):
                if row_idx == 0:
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

        # PARTITION INTO TASKS
        if task_carrier_type == 'proc':
            task_carrier = multiprocessing.Process
            logging.critical('[create_nodes_for_init_cn] Use multiprocessing.')
        elif task_carrier_type == 'thread':
            task_carrier = threading.Thread
            logging.critical('[create_nodes_for_init_cn] Use multithreading.')
        else:
            raise Exception('[create_nodes_for_init_cn] task_carrier_type can only be "proc" or "thread"!')

        # RUN TASKS IN PARALLEL
        l_task_instance = []
        num_nodes = len(l_person_trait)
        task_size = math.ceil(num_nodes / g_concurrency)
        task_num_id = 0
        for i in range(0, num_nodes, task_size):
            if i + task_size < num_nodes:
                task_data = l_person_trait[i:i + task_size]
            else:
                task_data = l_person_trait[i:]
            task_id = 'Task ' + str(task_num_id)
            task_instance = task_carrier(target=create_nodes_for_init_cn_by_create_method_single_task,
                                         args=(task_id, neo4j_driver, neo4j_session_config, task_data, batch_size),
                                         name=task_id)
            task_instance.start()
            l_task_instance.append(task_instance)
            task_num_id += 1

        while len(l_task_instance) > 0:
            for task_instance in l_task_instance:
                if task_instance.is_alive():
                    task_instance.join(1)
                else:
                    l_task_instance.remove(task_instance)
        logging.critical('[create_nodes_for_init_cn] All done in %s secs.' % str(time.time() - timer_start))


def create_edges(edge_file, occur, neo4j_driver, batch_size, method='apoc', task_carrier_type='thread'):
    """
    :param
        edge_file: str
            - File name: For the 'apoc' method.
            - Full path: For the 'create' method.
    :param
        task_carrier_type: str
            Meaningful only for the 'create' method.
            - 'thread': Use multithreading.
    """
    logging.critical('[create_edges] starts.')
    timer_start = time.time()

    # CONFIGURE NEO4J SESSION
    neo4j_session_config = {'database': g_neo4j_db_name}

    if method == 'apoc':
        query_str = \
            '''
            CALL apoc.periodic.iterate
            (
                "CALL apoc.load.csv('file:///%s', 
                                    {mapping:{sourcePID:{type:'int'}, targetPID:{type:'int'}, duration:{type:'int'}}}) 
                 yield map as rec return rec",
                "match (src:PERSON), (trg:PERSON) where src.pid=rec.sourcePID and trg.pid=rec.targetPID 
                 create (src)-[r:CONTACT {occur: %s, src_act:rec.sourceActivity, trg_act:rec.targetActivity, 
                 duration:rec.duration}]->(trg)",
                {parallel:true, batchSize:%s, concurrency:%s}
            )
        ''' % (edge_file, occur, batch_size, g_concurrency)
        execute_neo4j_queries(neo4j_driver, neo4j_session_config, [query_str])
        logging.critical('[create_edges] Creating edges done in %s secs.' % str(time.time() - timer_start))
    elif method == 'create':
        pass

    logging.critical('[create_edges] All done in %s secs.' % str(time.time() - timer_start))


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
    logging.critical('[create_init_cn] Starts.')
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
    logging.critical('[create_init_cn] %s nodes to be added in total.' % len(l_person_trait))
    query_str = '''unwind $rec as rec
                   create (n:PERSON {pid: rec.pid, hid: rec.hid, age: rec.age, age_group: rec.age_group, 
                   gender: rec.gender, fips: rec.fips, home_lat: rec.home_lat, home_lon: rec.home_lon, 
                   admin1: rec.admin1, admin2: rec.admin2, admin3: rec.admin3, admin4: rec.admin4})'''
    for i in range(0, len(l_person_trait), init_cn_batch_size):
        query_param = {'rec': l_person_trait[i : i + init_cn_batch_size]}
        execute_neo4j_queries(neo4j_driver, neo4j_session_config, [query_str], l_query_param=[query_param])
        logging.critical('[create_init_cn] Create %s nodes in %s secs.' % (len(l_person_trait[i : i + init_cn_batch_size]),
                                                                time.time() - timer_start))
    logging.critical('[create_init_cn] Creating nodes all done in %s secs.' % str(time.time() - timer_start))

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
                logging.critical('[create_init_cn] Created %s edges in %s secs.' % (total_cnt, time.time() - timer_start))
        if len(l_init_cn_batch) > 0:
            query_param = {'rec': l_init_cn_batch}
            execute_neo4j_queries(neo4j_driver, neo4j_session_config, [query_str], l_query_param=[query_param])
            total_cnt += len(l_init_cn_batch)
            logging.critical('[create_init_cn] Created %s edges in %s secs.' % (total_cnt, time.time() - timer_start))
    logging.critical('[create_init_cn] All done. Running time: %s ' % str(time.time() - timer_start_init))


def create_int_cn_edges_auto_search(neo4j_driver, search_folder, l_time_points, batch_size=1000000, method='apoc'):
    """
    Automatically search for intermediate contact network files and load into DB.
    :param
        l_time_points: list of int
            The list of time points in consideration. Considered intermediate contact networks will be loaded in.
    """
    logging.critical('[create_int_cn_edges_auto_search] Starts.')
    timer_start = time.time()

    # SEARCH FOR INT CN AND LOAD IN
    for (dirpath, dirname, filenames) in walk(search_folder):
        for filename in filenames:
            if re.match(g_int_cn_file_fmt, filename) is None:
                continue
            l_num_str = re.findall(r'[0-9]+', filename)
            if len(l_num_str) != 1:
                logging.error('[create_int_cn_edges_auto_search] Confusing file occurs: %s' % filename)
                continue
            time_point = int(l_num_str[0])
            if int(l_num_str[0]) not in l_time_points:
                continue
            logging.critical('[create_int_cn_edges_auto_search] Loading edges for time point %s starts.' % (time_point))
            create_edges(path.join(g_int_cn_folder, filename), time_point, neo4j_driver, batch_size, method)
            logging.critical('[create_int_cn_edges_auto_search] Loading edges for time point %s done in %s secs.'
                             % (time_point, time.time() - timer_start))

    logging.critical('[create_int_cn_edges_auto_search] All done in %s secs.' % str(time.time() - timer_start))


# def create_int_cn_auto_search(neo4j_driver, search_folder, int_cn_batch_size=1000000):
#     """
#     Search for intermediate files and load in.
#     """
#     logging.critical('[create_int_cn_auto_search] Starts.')
#
#     if not path.exists(search_folder):
#         logging.critical('[create_int_cn_auto_search] search_folder %s does not exist.' % search_folder)
#         return None
#
#     int_cn_file_fmt = 'network\[\d+\]'
#
#     timer_start_init = time.time()
#     timer_start = time.time()
#
#     # CONFIGURE NEO4J SESSION
#     neo4j_session_config = {'database': g_neo4j_db_name}
#
#     # LOAD IN INTERMEDIATE CONTACT NETWORKS
#     # occur_time_stamp = -1
#     query_str_fmt = '''unwind $rec as rec
#                        match (src:PERSON), (trg:PERSON)
#                        where src.pid=rec.sourcePID and trg.pid=rec.targetPID
#                        create (src)-[r:CONTACT {occur: %s, src_act:rec.sourceActivity, trg_act:rec.targetActivity,
#                        duration:rec.duration}]->(trg)
#                     '''
#
#     for (dirpath, dirname, filenames) in walk(search_folder):
#         for filename in filenames:
#             if re.match(int_cn_file_fmt, filename) is None:
#                 continue
#
#             # TEST ONLY STARTS
#             if filename != 'network[101]':
#                 continue
#             # TEST ONLY ENDS
#
#             l_num_str = re.findall(r'[0-9]+', filename)
#             if len(l_num_str) != 1:
#                 logging.error('[create_int_cn_auto_search] Confusing file occurs: %s' % filename)
#                 continue
#
#             int_cn_idx = int(l_num_str[0])
#
#             total_cnt_per_int_cn = 0
#             l_int_cn_batch = []
#             with open(path.join(dirpath, filename), 'r') as in_fd:
#                 csv_reader = csv.reader(in_fd, delimiter=',')
#                 for row_idx, row in enumerate(csv_reader):
#                     if row_idx == 0 or row_idx == 1:
#                         continue
#                     else:
#                         targetPID = int(row[0])
#                         targetActivity = row[1]
#                         sourcePID = int(row[2])
#                         sourceActivity = row[3]
#                         duration = int(row[4])
#                         l_int_cn_batch.append({'targetPID': targetPID,
#                                                'targetActivity': targetActivity,
#                                                'sourcePID': sourcePID,
#                                                'sourceActivity': sourceActivity,
#                                                'duration': duration})
#                     # CREATE EDGES FOR EACH BATCH
#                     if len(l_int_cn_batch) >= int_cn_batch_size:
#                         query_param = {'rec': l_int_cn_batch}
#                         execute_neo4j_queries(neo4j_driver, neo4j_session_config, [query_str_fmt % str(int_cn_idx)],
#                                               l_query_param=[query_param])
#                         total_cnt_per_int_cn += len(l_int_cn_batch)
#                         l_int_cn_batch = []
#                         logging.critical('[create_int_cn_auto_search] Int CN %s: Created %s edges in %s secs.'
#                               % (int_cn_idx, total_cnt_per_int_cn, time.time() - timer_start))
#                 if len(l_int_cn_batch) > 0:
#                     query_param = {'rec': l_int_cn_batch}
#                     execute_neo4j_queries(neo4j_driver, neo4j_session_config, [query_str_fmt % str(int_cn_idx)],
#                                           l_query_param=[query_param])
#                     total_cnt_per_int_cn += len(l_int_cn_batch)
#                     logging.critical('[create_int_cn_auto_search] Int CN %s: Created %s edges in %s secs.'
#                           % (int_cn_idx, total_cnt_per_int_cn, time.time() - timer_start))
#                 logging.critical('[create_int_cn_auto_search] Int CN %s: All done in %s secs.'
#                       % (int_cn_idx, time.time() - timer_start))
#
#     logging.critical('[create_int_cn_auto_search] All done. Running time: %s ' % str(time.time() - timer_start_init))


################################################################################
#   EPIHIPER OUTPUT PROCESSING
################################################################################
def create_epihiper_output_db():
    """
    Return True if successes, False otherwise.
    """
    logging.critical('[create_epihiper_output_db] Starts.')

    try:
        db_con = sqlite3.connect(g_epihiper_output_db_path)
    except Exception as e:
        logging.error('[create_epihiper_output_db] %s' % e)
        return False
    logging.critical('[create_epihiper_output_db] Database created.')

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
        logging.critical('[create_epihiper_output_db] Table created.')

    logging.critical('[create_epihiper_output_db] All done.')
    return True


def load_epihiper_output_to_db(batch_size=10000):
    """
    Return True if successes, False otherwise.
    TODO
        It may look neater refactoring the DB connection and closure to 'with' statement.
    """
    logging.critical('[load_epihiper_output_to_db] Starts.')
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
                    logging.critical('[load_epihiper_output_to_db] Committed %s recs in %s secs.'
                          % (out_id, time.time() - timer_start))
            except Exception as e:
                logging.error('[load_epihiper_output_to_db] row: %s, error: %s' % (row, e))
                err = True
        try:
            db_con.commit()
            logging.critical('[load_epihiper_output_to_db] Committed %s recs in %s secs.'
                  % (out_id, time.time() - timer_start))
        except Exception as e:
            logging.error('[load_epihiper_output_to_db] row: %s, error: %s' % (row, e))
            err = True

    db_con.close()

    if err:
        logging.critical('[load_epihiper_output_to_db] Return with errors in %s secs.' % str(time.time() - timer_start))
    else:
        logging.critical('[load_epihiper_output_to_db] All done in %s secs.' % str(time.time() - timer_start))
    return not err


def create_indexes_on_epihipter_output_db():
    """
    Return True if successes, False otherwise.
    """
    logging.critical('[create_indexes_on_epihipter_output_db] Starts.')
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
    logging.critical('[create_indexes_on_epihipter_output_db] All done in %s secs.' % str(time.time() - timer_start))
    return True


def load_epihiper_output(ds_path):
    logging.critical('[load_epihiper_output] Starts.')

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
    logging.critical('[load_epihiper_output] All done with %s output records.' % len(df_output))
    return df_output


def fetch_pids_by_exit_state(exit_state, out_path):
    """
    Return the list of PIDs over time for a given exit state.
    :return: pandas DataFrame
        Index: tick (int)
        Column: pid (list of int)
    """
    logging.critical('[fetch_pids_by_exit_state] Starts.')
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
    pd.to_pickle(df_pid_by_tick,  out_path)

    db_con.close()
    logging.critical('[fetch_pids_by_exit_state] All done in %s secs.' % str(time.time() - timer_start))
    return df_pid_by_tick


################################################################################
#   EXAMPLE QUERIES
################################################################################
def duration_distribution(neo4j_driver, df_output_pid_over_time, out_name_suffix, data_out_path, img_out_path,
                          save_img=True, l_t=None, mode='in_1nn'):
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
    logging.critical('[duration_distribution] Starts.')
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
    pd.to_pickle(df_dist, data_out_path)
    logging.critical('[duration_distribution] Output results.')

    # PLOT
    if save_img:
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
        plt.savefig(img_out_path, format='PNG')
        plt.show()
        plt.clf()
        plt.close()

    logging.critical('[duration_distribution] All done in %s secs.' % str(time.time() - timer_start))


# Query 1
# How many males between 18 and 24 years of age are newly infected
# (i.e., are just transitioned to state I or its variants [this could be a set of states])
# between times 5 and 15 inclusive? What are the PIDs of these males?
#
# Solution:
#   (1) Query EpiHiper SQLite DB for all PIDs with 'exit_state="I..."' between times 5 and 15 inclusively.
#   (2) Query Neo4j for 'n.pid' where 'n.age>=18 and n.age<=24' given the PIDs.
#
# Running Time:
#   0.4s
def query_1_sqlite_1(out_path):
    """
    Query for PIDs where 'exit_state' starts with 'I' and 'tick' between 5 and 15 inclusively.
    """
    logging.critical('[query_1_sqlite_1] Starts.')
    timer_start = time.time()

    try:
        db_con = sqlite3.connect(g_epihiper_output_db_path)
        db_cur = db_con.cursor()
    except Exception as e:
        logging.error(e)
        return None

    sql_str = '''PRAGMA case_sensitive_like=true'''
    try:
        db_cur.execute(sql_str)
    except Exception as e:
        logging.error('[query_1_sqlite_1] %s' % e)
        return None

    sql_str = '''select tick, pid from {0} where tick>=5 and tick<=15 and exit_state like "I%"'''\
        .format(g_epihiper_output_tb_name)
    try:
        db_cur.execute(sql_str)
        rows = db_cur.fetchall()
    except Exception as e:
        logging.error('[query_1_sqlite_1] %s' % e)
        return None

    l_pid_rec = []
    for row in rows:
        tick = int(row[0])
        pid = int(row[1])
        l_pid_rec.append((tick, pid))

    df_pid = pd.DataFrame(l_pid_rec, columns=['tick', 'pid'])
    pd.to_pickle(df_pid, out_path)

    db_con.close()
    logging.critical('[query_1_sqlite_1] All done in %s secs.' % str(time.time() - timer_start))


def query_1_neo4j_1(neo4j_driver, df_pid, out_path):
    """
    Query for PIDs where 'n.age>=18 and n.age<=24 and n.gender=2'.
    """
    logging.critical('[query_1_neo4j_1] Starts.')
    timer_start = time.time()

    neo4j_session_config = {'database': g_neo4j_db_name}

    l_infect_pid = list(set(df_pid['pid'].to_list()))

    query_str = '''unwind $infect_pid as infect_pid
                   match (n:PERSON {pid: infect_pid})
                   where n.age>=18 and n.age<=24 and n.gender=2
                   return infect_pid'''
    query_param = {'infect_pid': l_infect_pid}
    ret = execute_neo4j_queries(neo4j_driver, neo4j_session_config, [query_str], l_query_param=[query_param],
                                need_ret=True)
    df_ret = pd.DataFrame(ret[0], columns=['pid'])
    pd.to_pickle(df_ret, out_path)

    logging.critical('[query_1_neo4j_1] All done in %s secs.' % str(time.time() - timer_start))


# Query 2
# What are the states that are transitioned to by males or females that have the activity of shopping on their incoming
# edges?
#
# Solution:
#   (1) Query Neo4j for PIDs where for incoming edges 'r.trg_act="1:3"'.
#   (2) Query SQLite for states and group by the states given the PIDs.
#
# Running Time:
#   18s
def query_2_neo4j_1(neo4j_driver, out_path):
    """
    Query for n.pid with ()-[r]->(n) where r.trg_act="1:3" for all n.
    """
    logging.critical('[query_2_neo4j_1] Starts.')
    timer_start = time.time()

    neo4j_session_config = {'database': g_neo4j_db_name}

    query_str = '''match ()-[r:CONTACT]->(n)
                   where r.trg_act="1:3"
                   return distinct n.pid'''
    ret = execute_neo4j_queries(neo4j_driver, neo4j_session_config, [query_str], l_query_param=None, need_ret=True)
    df_ret = pd.DataFrame(ret[0], columns=['pid'])
    pd.to_pickle(df_ret, out_path)

    logging.critical('[query_2_neo4j_1] All done in %s secs.' % str(time.time() - timer_start))


def query_2_sqlite_1(df_pid, out_path):
    """
    Query for exit_state with group by given PIDs.
    """
    logging.critical('[query_2_sqlite_1] Starts.')
    timer_start = time.time()

    l_pid = df_pid['pid'].to_list()

    try:
        db_con = sqlite3.connect(g_epihiper_output_db_path)
        db_cur = db_con.cursor()
    except Exception as e:
        logging.error(e)
        return None

    sql_str = "select exit_state, count(*) from {0} where pid in ({1}) group by exit_state" \
        .format(g_epihiper_output_tb_name, ','.join(['?'] * len(l_pid)))
    try:
        db_cur.execute(sql_str, l_pid)
        rows = db_cur.fetchall()
    except Exception as e:
        logging.error('[query_2_sqlite_1] %s' % e)
        return None

    l_exit_state_rec = []
    for row in rows:
        exit_state = row[0]
        count = int(row[1])
        l_exit_state_rec.append((exit_state, count))

    df_exit_state = pd.DataFrame(l_exit_state_rec, columns=['exit_state', 'count'])
    pd.to_pickle(df_exit_state, out_path)

    db_con.close()
    logging.critical('[query_2_sqlite_1] All done in %s secs.' % str(time.time() - timer_start))


# Query 3
# What are the states that are transitioned to by males between 75 and 90 or by females 32 to 39
# where the other node of an edge, if there is an edge, is activity work?
#
# Solution:
#   (1) Query Neo4j for PIDs where (n.gender=2 and n.age>=71 and n.age<= 90) or (n.gender=1 and n.age>=32 and n.age<=39)
#       with incoming edges where r.src_act="1:2".
#   (2) Query SQLite for exit_state given PIDs.
#
# Running Time:
#   47s
def query_3_neo4j_1(neo4j_driver, out_path):
    """
    Query Neo4j for PIDs where (n.gender=2 and n.age>=71 and n.age<= 90) or (n.gender=1 and n.age>=32 and n.age<=39)
    with incoming edges where r.src_act="1:2".
    """
    logging.critical('[query_3_neo4j_1] Starts.')
    timer_start = time.time()

    neo4j_session_config = {'database': g_neo4j_db_name}

    query_str = '''match ()-[r:CONTACT]->(n)
                   where (r.src_act="1:2") and 
                         ((n.gender=2 and n.age>=71 and n.age<=90) 
                          or 
                          (n.gender=1 and n.age>=32 and n.age<=39))
                   return distinct n.pid'''
    ret = execute_neo4j_queries(neo4j_driver, neo4j_session_config, [query_str], l_query_param=None, need_ret=True)
    df_ret = pd.DataFrame(ret[0], columns=['pid'])
    pd.to_pickle(df_ret, out_path)

    logging.critical('[query_3_neo4j_1] All done in %s secs.' % str(time.time() - timer_start))


def query_3_sqlite_1(df_pid, out_path):
    """
    Query SQLite for exit_state given PIDs with group by.
    """
    logging.critical('[query_3_sqlite_1] Starts.')
    timer_start = time.time()

    l_pid = df_pid['pid'].to_list()

    try:
        db_con = sqlite3.connect(g_epihiper_output_db_path)
        db_cur = db_con.cursor()
    except Exception as e:
        logging.error(e)
        return None

    sql_str = "select exit_state, count(*) from {0} where pid in ({1}) group by exit_state" \
        .format(g_epihiper_output_tb_name, ','.join(['?'] * len(l_pid)))
    try:
        db_cur.execute(sql_str, l_pid)
        rows = db_cur.fetchall()
    except Exception as e:
        logging.error('[query_3_sqlite_1] %s' % e)
        return None

    l_exit_state_rec = []
    for row in rows:
        exit_state = row[0]
        count = int(row[1])
        l_exit_state_rec.append((exit_state, count))

    df_exit_state = pd.DataFrame(l_exit_state_rec, columns=['exit_state', 'count'])
    pd.to_pickle(df_exit_state, out_path)

    db_con.close()
    logging.critical('[query_3_sqlite_1] All done in %s secs.' % str(time.time() - timer_start))


# Query 4
# What people (individuals) are entering the recovered state (in an SIR epidemic) between times 5 and 15 inclusive,
# where the household income is between $86,000 and $114,000 annually?
#
# Question:
#   Where is the household income information?


# Query 5
# What are the household IDs of households where the number of people in the household is at least 6 and
# there is at least one child between 8 and 14?
#
# Solution:
#   (1) Only Neo4j is needed. Find all HIDs associated with more than 6 members, then for each HID check if it has
#       at least one member between 8 and 14.
def query_5_neo4j_1(neo4j_driver, out_path):
    logging.critical('[query_5_neo4j_1] Starts.')
    timer_start = time.time()

    neo4j_session_config = {'database': g_neo4j_db_name}

    query_str = '''match (n:PERSON)  
                   with distinct n.hid as trg_hid, count(n) as cnt
                   where cnt>=8
                   match (m:PERSON)
                   where m.hid=trg_hid and 8<=m.age<=14
                   return distinct m.hid'''
    ret = execute_neo4j_queries(neo4j_driver, neo4j_session_config, [query_str], l_query_param=None, need_ret=True)
    df_ret = pd.DataFrame(ret[0], columns=['hid'])
    pd.to_pickle(df_ret, out_path)

    logging.critical('[query_5_neo4j_1] All done in %s secs.' % str(time.time() - timer_start))


################################################################################
#   FROM NEO4J TO SNAP
################################################################################
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


def neo4j_query_to_ttables(neo4j_driver, neo4j_session_config, neo4j_query_str, query_params, out_folder,
                           out_suffix=None):
    """
    This function executes a query for a subgraph from Neo4j, and outputs the subgraph represented by the node and edge
    TTables.
    :param
        neo4j_query_str: str
            A Cypher query str.
            !!!CAUTION!!!
            This query should return all properties of nodes and edges.
    :param
        query_params: dict
            Each key of this dict corresponds to a parameter of the query.
    :param
        out_folder: str
            The folder for storing the output TTables.
    :param
        out_suffix: str
            The suffix for output TTable names. Note that this suffix typically should contain the ID for the output
            graph in a sense.
    :return
        - True: successful output.
        - False: nothing to output.
    """
    logging.critical('[neo4j_query_to_ttables] Starts.')

    if neo4j_query_str is None or neo4j_query_str == '':
        logging.error('[neo4j_query_to_ttables] neo4j_query_str is not valid: %s' % neo4j_query_str)

    if out_folder is None or out_folder == '':
        logging.error('[neo4j_query_to_ttables] out_folder is not valid: %s' % neo4j_query_str)

    logging.critical('[neo4j_query_to_ttables] neo4j_query_str = %s' % neo4j_query_str)
    timer_start = time.time()

    l_query_param = None
    if query_params is not None:
        l_query_param = [query_params]
    l_ret = execute_neo4j_queries(neo4j_driver,
                                  neo4j_session_config,
                                  [neo4j_query_str],
                                  l_query_param=l_query_param,
                                  need_ret=True)
    if len(l_ret[0]) <= 0:
        logging.critical('[neo4j_query_to_ttables] Nothing retrieved for the query: %s' % neo4j_query_str)
        return False

    # Construct a TNEANet network
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
        edge_occur = edge['occur']
        edge_duration = edge['duration']
        edge_src_act = edge['src_act']
        edge_trg_act = edge['trg_act']
        add_edge_to_snap_graph(tneanet_ins, src_pid, trg_pid, edge_duration, edge_src_act, edge_trg_act, edge_occur)

    # Extract node and edge TTables from the TNEANet network
    context = snap.TTableContext()
    node_ttable = snap.TTable.GetNodeTable(tneanet_ins, context)
    edge_ttable = snap.TTable.GetEdgeTable(tneanet_ins, context)

    if out_suffix is None:
        out_suffix = ''
    else:
        out_suffix = ''.join(['_', out_suffix])
    fd_out = snap.TFOut(path.join(out_folder, 'node_ttable%s.bin' % out_suffix))
    node_ttable.Save(fd_out)
    fd_out.Flush()
    fd_out = snap.TFOut(path.join(out_folder, 'edge_ttable%s.bin' % out_suffix))
    edge_ttable.Save(fd_out)
    fd_out.Flush()
    logging.critical('[neo4j_query_to_ttables] All done in %s secs.' % str(time.time() - timer_start))
    return True


def output_in_1nn_batch(neo4j_driver, df_output_pid_over_time, batch_size, out_folder, out_suffix):
    logging.critical('[output_in_1nn_batch] Starts.')
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
            neo4j_query_str = query_str_fmt % (skip, limit)
            ret = neo4j_query_to_ttables(neo4j_driver, neo4j_session_config, neo4j_query_str, query_param, out_folder,
                                         ''.join([out_suffix, '_', str(batch_cnt)]))
            if not ret:
                print('[output_in_1nn_batch] Done graph output with %s batches for tick %s' % (batch_cnt, tick))
                break
            skip += limit
            batch_cnt += 1
    logging.critical('[output_in_1nn_batch] All done in %s secs.' % str(time.time() - timer_start))


if __name__ == '__main__':
    ############################################################
    #   USAGE
    #   You need to set up the environment variable 'NEO4J_HOSTNAME' to the hostname of the machine on which
    #   the Neo4j server is running.
    #   !!!CAUTION!!!
    #   When DB has already been pretty large, DO NOT use 'purge_db' due to high complexity. Instead, it'd be much
    #   easier simply removing the entire DB.
    #   # PURGE DB
    #   > python neo4j_ops.py "neo4j_driver->purge_db"
    #   !!!CAUTION!!!
    #   The Community edition does not support constraints. So for Community we merely create indexes.
    #   # CREATE DB (for Enterprise)
    #   > python neo4j_ops.py "neo4j_driver->create_db->create_constraints->create_indexes"
    #   # CREATE DB (for Community)
    #   > python neo4j_ops.py "neo4j_driver->create_indexes"
    #   # CREATE NODES
    #   > python neo4j_ops.py "neo4j_driver->create_nodes"
    #   # CREATE INTERMEDIATE CONTACT NETWORKS
    #   > python neo4j_ops.py "neo4j_driver->build_int_cn"
    #   NOTE
    #   1. All commands should be linked by '->'.
    #   2. The order of commands matters.
    ############################################################
    logging.basicConfig(level=logging.ERROR)

    neo4j_hostname = os.getenv(g_neo4j_hostname_env_key)
    if neo4j_hostname is None or neo4j_hostname == '':
        logging.error('[main] The environment variable NEO4J_HOSTNAME is not set yet!')
        sys.exit(-1)
    g_neo4j_server_uri = g_neo4j_server_uri_fmt.format(neo4j_hostname)
    logging.critical('[main] g_neo4j_server_uri is set to %s' % g_neo4j_server_uri)

    cmd_pipeline = sys.argv[1]
    l_cmd = [cmd.strip().lower() for cmd in cmd_pipeline.split('->')]
    logging.critical('[main] Commands to be executed: %s' % l_cmd)

    neo4j_driver = None

    for cmd in l_cmd:
        if cmd == '':
            logging.critical('[main] Nothing to do.')

        # CONNECT TO NEO4J DRIVER
        elif cmd == 'neo4j_driver':
            logging.critical('[main] neo4j_driver starts.')
            neo4j_driver = connect_to_neo4j_driver(g_neo4j_server_uri, (g_neo4j_username, g_neo4j_password),
                                                   {'max_connection_lifetime': 1000})
            logging.critical('[main] neo4j_driver done.')

        # CREATE NEO4J DB
        # NOTE: Every query execution needs a session
        # NOTE: Neo4j does NOT support symbols well in database naming, and it is actually case-insensitive.
        #       These are different from their documents.
        elif cmd == 'create_db':
            logging.critical('[main] create_db starts.')
            if neo4j_driver is None:
                raise Exception('[main] neo4j_driver is None. Run "create_driver" first.')
            query_str = '''create database {0} if not exists'''.format(g_neo4j_db_name)
            execute_neo4j_queries(neo4j_driver, None, [query_str])
            logging.critical('[main] create_db done.')

        # CREATE CONSTRAINTS
        elif cmd == 'create_constraints':
            logging.critical('[main] create_constraints starts.')
            neo4j_session_config = {'database': g_neo4j_db_name}
            # NODE CONSTRAINTS
            # Existence of the 'pid' property of node
            query_str_constraint_n1 = '''create constraint pid_exist if not exists
                                                on (n:PERSON)
                                                assert n.pid is not null'''
            # Uniqueness of the 'pid' property of node
            query_str_constraint_n2 = '''create constraint pid_unique if not exists
                                                on (n:PERSON)
                                                assert n.pid is unique'''
            # Existence of the 'hid' property of node
            query_str_constraint_n3 = '''create constraint hid_exist if not exists
                                                                on (n:PERSON)
                                                                assert n.hid is not null'''
            # Existence of the 'age' property of node
            query_str_constraint_n4 = '''create constraint age_exist if not exists
                                                                on (n:PERSON)
                                                                assert n.age is not null'''
            # Existence of the 'age_group' property of node
            query_str_constraint_n5 = '''create constraint age_group_exist if not exists
                                                                on (n:PERSON)
                                                                assert n.age_group is not null'''
            # EDGE CONSTRAINTS
            # Existence of the 'duration' property of edge
            query_str_constraint_e1 = '''create constraint duration_exist if not exists
                                                on ()-[r:CONTACT]-()
                                                assert r.duration is not null'''
            # Existence of the 'src_act' property of edge (i.e. sourceActivity)
            query_str_constraint_e2 = '''create constraint src_act_exist if not exists
                                                on ()-[r:CONTACT]-()
                                                assert r.src_act is not null'''
            # Existence of the 'trg_act' property of edge (i.e. targetActivity)
            query_str_constraint_e3 = '''create constraint trg_act_exist if not exists
                                                on ()-[r:CONTACT]-()
                                                assert r.trg_act is not null'''
            # Existence of the 'occur' property of edge
            query_str_constraint_e4 = '''create constraint occur_exist if not exists
                                                on ()-[r:CONTACT]-()
                                                assert r.occur is not null'''

            execute_neo4j_queries(neo4j_driver, neo4j_session_config, [query_str_constraint_n1,
                                                                       query_str_constraint_n2,
                                                                       query_str_constraint_n3,
                                                                       query_str_constraint_n4,
                                                                       query_str_constraint_n5,
                                                                       query_str_constraint_e1,
                                                                       query_str_constraint_e2,
                                                                       query_str_constraint_e3,
                                                                       query_str_constraint_e4])
            logging.critical('[main] create_constraints done.')

        # CREATE INDEXES
        elif cmd == 'create_indexes':
            logging.critical('[main] create_indexes starts.')
            neo4j_session_config = {'database': g_neo4j_db_name}

            # NODE INDEXES
            # Index for 'age'
            query_str_idx_n1 = '''create btree index idx_age if not exists for (n:PERSON) on (n.age)'''
            # Index for 'age_group'
            query_str_idx_n2 = '''create index idx_age_group if not exists for (n:PERSON) on (n.age_group)'''
            # Index for 'gender'
            query_str_idx_n3 = '''create index idx_gender if not exists for (n:PERSON) on (n.age_group)'''
            # Index for 'fips'
            query_str_idx_n4 = '''create index idx_fips if not exists for (n:PERSON) on (n.fips)'''
            # !!!CAUTION!!!
            # When using Enterprise, constraints on 'pid' and 'hid' will be created, and the indexes on the two will
            # be automatically created then. Though, with Community, as no constraint is supported, these indexes need
            # to be created explicitly.
            # Index for 'pid'
            query_str_idx_n5 = '''create index idx_pid if not exists for (n:PERSON) on (n.pid)'''
            # Index for 'hid'
            query_str_idx_n6 = '''create index idx_hid if not exists for (n:PERSON) on (n.hid)'''

            # EDGE INDEXES
            # Index for 'duration'
            query_str_idx_e1 = '''create btree index idx_duration if not exists for ()-[r:CONTACT]-() on (r.duration)'''
            # Index for 'src_act'
            query_str_idx_e2 = '''create index idx_src_act if not exists for ()-[r:CONTACT]-() on (r.src_act)'''
            # Index for 'trg_act'
            query_str_idx_e3 = '''create index idx_trg_act if not exists for ()-[r:CONTACT]-() on (r.trg_act)'''
            # Index for 'occur'
            query_str_idx_e4 = '''create btree index idx_occur if not exists for ()-[r:CONTACT]->() on (r.occur)'''
            execute_neo4j_queries(neo4j_driver, neo4j_session_config, [query_str_idx_n1, query_str_idx_n2,
                                                                       query_str_idx_n3, query_str_idx_n4,
                                                                       query_str_idx_n5, query_str_idx_n6,
                                                                       query_str_idx_e1, query_str_idx_e2,
                                                                       query_str_idx_e3, query_str_idx_e4])
            logging.critical('[main] create_indexes done.')

        # DELETE EVERYTHING IN DB
        elif cmd == 'purge_db':
            logging.critical('[main] purge_db starts.')
            neo4j_session_config = {'database': g_neo4j_db_name}
            # Remove all edges and their end nodes
            query_str = '''match (s)-[r]->(t) delete r, s, t'''
            execute_neo4j_queries(neo4j_driver, neo4j_session_config, [query_str])
            # Remove the rest of nodes
            query_str = '''match (n) delete n'''
            execute_neo4j_queries(neo4j_driver, neo4j_session_config, [query_str])
            # Remove all constraints and some related indexes
            query_str = '''call apoc.schema.assert({}, {}, true)'''
            execute_neo4j_queries(neo4j_driver, neo4j_session_config, [query_str])
            # Remove all other indexes if any
            l_index_name = ['idx_duration', 'idx_src_act', 'idx_trg_act', 'idx_age', 'idx_occur', 'idx_age_group',
                            'idx_gender', 'idx_fips', 'idx_pid', 'idx_hid']
            query_str = '''drop index {0} if exists'''
            l_query_str = [query_str.format(index_name) for index_name in l_index_name]
            execute_neo4j_queries(neo4j_driver, neo4j_session_config, l_query_str)
            logging.critical('[main] purge_db done.')

        # CREATE NODES WITH PERSON TRAIT FOR ALL CONTACT NETWORKS
        elif cmd == 'create_nodes':
            logging.critical('[main] create_nodes starts.')
            batch_size = 100000
            method = 'apoc'
            create_nodes_for_init_cn(neo4j_driver, batch_size, method=method, task_carrier_type='thread')
            logging.critical('[main] create_nodes done.')

        # CREATE EDGES FOR INITIAL CONTACT NETWORK
        elif cmd == 'create_init_cn_edges':
            logging.critical('[main] create_init_cn_edges starts.')
            batch_size = 500000
            method = 'apoc'
            occur = -1
            create_edges(g_init_cn_file_name, occur, neo4j_driver, batch_size, method)
            logging.critical('[main] create_init_cn_edges done.')

        # CREATE EDGES FOR INTERMEDIATE CONTACT NETWORKS
        elif cmd == 'create_int_cn_edges':
            logging.critical('[main] create_int_cn_edges starts.')
            search_folder = path.join(g_init_cn_folder, g_int_cn_folder)
            l_time_points = [5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15]
            batch_size = 500000
            method = 'apoc'
            create_int_cn_edges_auto_search(neo4j_driver, search_folder, l_time_points, batch_size, method)
            logging.critical('[main] create_int_cn_edges done.')

        # CREATE EPIHIPER OUTPUT SQLITE DATABASE
        elif cmd == 'create_epihiper_output_db':
            logging.critical('[main] create_epihiper_output_db starts.')
            create_epihiper_output_db()
            logging.critical('[main] create_epihiper_output_db done.')

        # LOAD EPIHIPER OUTPUT DATA
        elif cmd == 'load_epihiper_output_data':
            logging.critical('[main] load_epihiper_output_data starts.')
            batch_size = 10000
            load_epihiper_output_to_db(batch_size)
            logging.critical('[main] load_epihiper_output_data done.')

        # CREATE INDEXES ON EPIHIPER OUTPUT DB
        elif cmd == 'create_epihiper_output_db_indexes':
            logging.critical('[main] create_epihiper_output_db_indexes starts.')
            create_indexes_on_epihipter_output_db()
            logging.critical('[main] create_epihiper_output_db_indexes done.')

        # FETCH PIDs OVER TIME FROM EPIHIPER OUTPUT DB FOR A GIVEN EXIT STATE
        elif cmd == 'fetch_pids_by_exit_state':
            logging.critical('[main] fetch_pids_by_exit_state starts.')
            exit_state = 'Isymp_s'
            out_path = path.join(g_epihiper_output_folder, g_int_cn_folder, 'pid_over_time_by_%s.pickle' % exit_state)
            fetch_pids_by_exit_state(exit_state, out_path)
            logging.critical('[main] fetch_pids_by_exit_state done.')

        # COMPUTE DISTRIBUTION OF DURATION OVER TIME
        elif cmd == 'duration_distribution':
            logging.critical('[main] duration_distribution starts.')
            exit_state = 'Isymp_s'
            pid_file_path = path.join(g_epihiper_output_folder, g_int_cn_folder,
                                      'pid_over_time_by_%s.pickle' % exit_state)
            df_output_pid_over_time = pd.read_pickle(pid_file_path)
            duration_distribution(neo4j_driver, df_output_pid_over_time, exit_state,
                                  path.join(g_epihiper_output_folder, g_int_cn_folder,
                                            'duration_dist_%s.pickle' % exit_state),
                                  path.join(g_epihiper_output_folder, g_int_cn_folder,
                                            'duration_distribution_%s.PNG' % exit_state))
            logging.critical('[main] duration_distribution done.')

        # OUTPUT BATCHED TNEANET GRAPHS TO FILES
        elif cmd == 'output_in_1nn_batch':
            logging.critical('[main] output_in_1nn_batch starts.')
            exit_state = 'Isymp_s'
            pid_file_path = path.join(g_epihiper_output_folder, g_int_cn_folder,
                                      'pid_over_time_by_%s.pickle' % exit_state)
            df_output_pid_over_time = pd.read_pickle(pid_file_path)
            batch_size = 1000
            out_folder = path.join(g_epihiper_output_folder, g_int_cn_folder)
            output_in_1nn_batch(neo4j_driver, df_output_pid_over_time, batch_size, out_folder, exit_state)
            logging.critical('[main] output_in_1nn done.')

        # QUERY INCOMING DEGREES OF INFECTED PEOPLE
        elif cmd == 'infect_in_deg_dist_at_t':
            logging.critical('[main] infect_in_deg_dist_at_t starts.')
            timer_start = time.time()
            t = 5
            neo4j_session_config = {'database': g_neo4j_db_name}
            df_output = load_epihiper_output(g_epihiper_output_path)
            df_output = df_output.set_index('tick')
            l_infect_pid = list(set(df_output.loc[t]['pid'].to_list()))
            logging.critical('Running time: %s' % str(time.time() - timer_start))
            query_str = '''unwind $infect_pid as infect_pid
                           match (n:PERSON {pid: infect_pid})
                           return infect_pid, apoc.node.degree(n, "<CONTACT")'''
            query_param = {'infect_pid': l_infect_pid}
            ret = execute_neo4j_queries(neo4j_driver, neo4j_session_config, [query_str], l_query_param=[query_param],
                                        need_ret=True)
            logging.critical('return:')
            logging.critical(ret[0])
            # logging.critical([item for item in ret[0] if item['apoc.node.degree(n, "<CONTACT")'] > 0])
            logging.critical('[main] infect_in_deg_dist_at_t done.')

        elif cmd == 'example_query_1':
            logging.critical('[main] example_query_1 starts.')
            timer_start = time.time()
            query_id = 1
            sqlite_out_name = 'sqlite_pid.pickle'
            sqlite_out_path = path.join(g_example_query_each_folder_fmt.format(str(query_id)), sqlite_out_name)
            neo4j_out_name = 'results.pickle'
            neo4j_out_path = path.join(g_example_query_each_folder_fmt.format(str(query_id)), neo4j_out_name)
            query_1_sqlite_1(sqlite_out_path)
            df_pid = pd.read_pickle(sqlite_out_path)
            query_1_neo4j_1(neo4j_driver, df_pid, neo4j_out_path)
            logging.critical('[main] example_query_1 done in %s secs.' % str(time.time() - timer_start))

        elif cmd == 'example_query_2':
            logging.critical('[main] example_query_2 starts.')
            timer_start = time.time()
            query_id = 2
            neo4j_out_name = 'neo4j_pid.pickle'
            neo4j_out_path = path.join(g_example_query_each_folder_fmt.format(str(query_id)), neo4j_out_name)
            sqlite_out_name = 'results.pickle'
            sqlite_out_path = path.join(g_example_query_each_folder_fmt.format(str(query_id)), sqlite_out_name)
            query_2_neo4j_1(neo4j_driver, neo4j_out_path)
            df_pid = pd.read_pickle(neo4j_out_path)
            query_2_sqlite_1(df_pid, sqlite_out_path)
            logging.critical('[main] example_query_2 done in %s secs.' % str(time.time() - timer_start))

        elif cmd == 'example_query_3':
            logging.critical('[main] example_query_3 starts.')
            timer_start = time.time()
            query_id = 3
            neo4j_out_name = 'neo4j_pid.pickle'
            neo4j_out_path = path.join(g_example_query_each_folder_fmt.format(str(query_id)), neo4j_out_name)
            sqlite_out_name = 'results.pickle'
            sqlite_out_path = path.join(g_example_query_each_folder_fmt.format(str(query_id)), sqlite_out_name)
            query_3_neo4j_1(neo4j_driver, neo4j_out_path)
            df_pid = pd.read_pickle(neo4j_out_path)
            query_3_sqlite_1(df_pid, sqlite_out_path)
            logging.critical('[main] example_query_3 done in %s secs.' % str(time.time() - timer_start))

        elif cmd == 'example_query_5':
            logging.critical('[main] example_query_5 starts.')
            timer_start = time.time()
            query_id = 5
            neo4j_out_name = 'results.pickle'
            neo4j_out_path = path.join(g_example_query_each_folder_fmt.format(str(query_id)), neo4j_out_name)
            query_5_neo4j_1(neo4j_driver, neo4j_out_path)
            logging.critical('[main] example_query_5 done in %s secs.' % str(time.time() - timer_start))

        # TEST FOR PARALLEL LOADING USING APOC
        elif cmd == 'parallel_apoc':
            logging.critical('[main] parallel_apoc starts.')
            timer_start = time.time()
            cn_split_file_name = sys.argv[2]
            if cn_split_file_name is None or cn_split_file_name == '' or len(cn_split_file_name) <= 0:
                raise Exception('[main] parallel_apoc: Failed to get cn_split_file_name!')
            logging.critical('[main] cn_split_file_name = %s' % cn_split_file_name)
            batch_size = 500000
            method = 'apoc'
            occur = -1
            create_edges(cn_split_file_name, occur, neo4j_driver, batch_size, method)
            logging.critical('[main] parallel_apoc done in %s secs.' % str(time.time() - timer_start))

        # TEST FOR SERIAL LOADING USING APOC WITH MULTIPLE PARTITIONS
        elif cmd == 'serial_apoc':
            logging.critical('[main] serial_apoc starts.')
            timer_start = time.time()
            batch_size = 500000
            method = 'apoc'
            occur = -1
            l_split_idx = [i for i in range(10)]
            for split_idx in l_split_idx:
                cn_split_file_name = 'wy_init_cn_split_' + str(split_idx)
                logging.critical('[main] serial_apoc: loading in %s' % cn_split_file_name)
                create_edges(cn_split_file_name, occur, neo4j_driver, batch_size, method)
            logging.critical('[main] serial_apoc done in %s secs.' % str(time.time() - timer_start))

        # TODO
        # THIS CMD SHOULD NOT BE USED OUTSIDE THE DEMO
        # GENERATE BOGUS DATA FOR DEMO
        elif cmd == 'bogus_data':
            logging.critical('[main] bogus_data starts.')
            try:
                db_con = sqlite3.connect(g_epihiper_output_db_path)
                db_cur = db_con.cursor()
            except Exception as e:
                logging.error(e)
                sys.exit(-1)

            sql_str = '''select tick, pid, exit_state, contact_pid, lid from %s where tick in (5, 6, 7, 8, 9)''' \
                      % (g_epihiper_output_tb_name)
            try:
                db_cur.execute(sql_str)
                rows = db_cur.fetchall()
            except Exception as e:
                logging.error('[main] %s' % e)
                sys.exit(-1)

            sql_str = '''insert into %s (out_id, tick, pid, exit_state, contact_pid, lid) values (?,?,?,?,?,?)''' \
                      % g_epihiper_output_tb_name

            batch_size = 1000
            out_id = 3000000
            for row in rows:
                tick = int(row[0])
                if tick not in [5, 6, 7, 8, 9]:
                    continue
                if tick == 5:
                    tick = 0
                elif tick == 6:
                    tick = 1
                elif tick == 7:
                    tick = 2
                elif tick == 8:
                    tick = 3
                elif tick == 9:
                    tick = 4
                pid = int(row[1])
                exit_state = row[2]
                if row[3] is None:
                    contact_pid = None
                else:
                    contact_pid = int(row[3])
                lid = row[4]
                try:
                    db_cur.execute(sql_str, (out_id, tick, pid, exit_state, contact_pid, lid))
                    out_id += 1
                    if out_id % batch_size == 0 and out_id >= batch_size:
                        db_con.commit()
                        logging.critical('[main] Committed %s recs.' % str(out_id - 3000000))
                except Exception as e:
                    logging.error('[main] row: %s, error: %s' % (row, e))
            try:
                db_con.commit()
                logging.critical('[main] Committed %s recs.' % str(out_id - 3000000))
            except Exception as e:
                logging.error('[main] final commit, error: %s' % e)
            db_con.close()
            logging.critical('[main] bogus_data done.')