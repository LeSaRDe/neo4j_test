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
"""

# import json
import logging
import csv
import os
import sys
import time
# import math
# import sqlite3
from os import path, walk
import re

# import pandas as pd
# import matplotlib.pyplot as plt
# import seaborn as sns
from neo4j import GraphDatabase
import snap


################################################################################
#   GLOBAL VARIABLES
################################################################################
g_neo4j_hostname_env_key = 'NEO4J_HOSTNAME'

g_init_cn_folder = '/project/biocomplexity/nssac/EpiHiperSynPop/v1.9.0/usa_va_2017_SynPop/'
# g_init_cn_folder = '/scratch/mf3jh/data/epihiper/'
g_int_cn_folder = '/project/bii_nssac/COVID-19_USA_EpiHiper/rivanna/20210328-ct_wsc/network_export/tau_0.063_ct_0/replicate_1/'
# g_int_cn_folder = '/home/mf3jh/workspace/data/epihiper'
g_epihiper_output_folder = '/home/mf3jh/workspace/data/epihiper/'

g_init_cn_file_name = 'va_contact_network_config_m_5_M_40_a_1000_m-contact_0_no_lid.txt'
# g_init_cn_file_name = 'sample_init_cn.txt'
g_person_trait_file_name = 'va_persontrait_epihiper.txt'
# g_person_trait_file_name = 'sample_person_trait.txt'

g_init_cn_path = ''.join([g_init_cn_folder, g_init_cn_file_name])
g_int_cn_path_fmt = ''.join([g_int_cn_folder, 'network_[{0}]'])
g_person_trait_path = ''.join([g_init_cn_folder, g_person_trait_file_name])
g_epihiper_output_path = ''.join([g_epihiper_output_folder, 'output.csv'])
g_epihiper_output_db_path = ''.join([g_epihiper_output_folder, 'output.db'])
g_int_cn_cnt = 10
g_l_int_cn_path = [g_int_cn_path_fmt.format(str(i)) for i in range(g_int_cn_cnt)]

g_neo4j_server_uri = None
g_neo4j_server_uri_fmt = 'neo4j://{0}:7687'

# If the Neo4j server is set with authentication disabled, the username and password are not necessary.
# Though, in this case, we don't have to change anything in code.
g_neo4j_username = 'neo4j'
g_neo4j_password = 'michal'

# TODO
# We may need a better DB name.
g_neo4j_db_name = 'samplecontactnetwork'
g_epihiper_output_tb_name = 'epihiper_output'


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

    # print('[execute_neo4j_query] All done in %s secs.' % str(time.time() - timer_start))
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
    print('[create_init_cn] %s nodes to be added in total.' % len(l_person_trait))
    query_str = '''unwind $rec as rec
                   create (n:PERSON {pid: rec.pid, hid: rec.hid, age: rec.age, age_group: rec.age_group, 
                   gender: rec.gender, fips: rec.fips, home_lat: rec.home_lat, home_lon: rec.home_lon, 
                   admin1: rec.admin1, admin2: rec.admin2, admin3: rec.admin3, admin4: rec.admin4})'''
    for i in range(0, len(l_person_trait), init_cn_batch_size):
        query_param = {'rec': l_person_trait[i : i + init_cn_batch_size]}
        execute_neo4j_queries(neo4j_driver, neo4j_session_config, [query_str], l_query_param=[query_param])
        print('[create_init_cn] Create %s nodes in %s secs.' % (len(l_person_trait[i : i + init_cn_batch_size]),
                                                                time.time() - timer_start))
    print('[create_init_cn] Creating nodes all done in %s secs.' % str(time.time() - timer_start))

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


def create_int_cn_auto_search(neo4j_driver, search_folder, int_cn_batch_size=1000000):
    """
    Search for intermediate files and load in.
    """
    logging.error('[create_int_cn_auto_search] Starts.')

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

            # TEST ONLY STARTS
            if filename != 'network[101]':
                continue
            # TEST ONLY ENDS

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
                        logging.error('[create_int_cn_auto_search] Int CN %s: Created %s edges in %s secs.'
                              % (int_cn_idx, total_cnt_per_int_cn, time.time() - timer_start))
                if len(l_int_cn_batch) > 0:
                    query_param = {'rec': l_int_cn_batch}
                    execute_neo4j_queries(neo4j_driver, neo4j_session_config, [query_str_fmt % str(int_cn_idx)],
                                          l_query_param=[query_param])
                    total_cnt_per_int_cn += len(l_int_cn_batch)
                    logging.error('[create_int_cn_auto_search] Int CN %s: Created %s edges in %s secs.'
                          % (int_cn_idx, total_cnt_per_int_cn, time.time() - timer_start))
                logging.error('[create_int_cn_auto_search] Int CN %s: All done in %s secs.'
                      % (int_cn_idx, time.time() - timer_start))

    logging.error('[create_int_cn_auto_search] All done. Running time: %s ' % str(time.time() - timer_start_init))


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


def neo4j_query_to_ttables(neo4j_query_str, query_params, out_folder, out_suffix=None):
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
    """
    print('[neo4j_query_to_ttables] Starts.')

    if neo4j_query_str is None or neo4j_query_str == '':
        logging.error('[neo4j_query_to_ttables] neo4j_query_str is not valid: %s' % neo4j_query_str)

    if out_folder is None or out_folder == '':
        logging.error('[neo4j_query_to_ttables] out_folder is not valid: %s' % neo4j_query_str)

    print('[neo4j_query_to_ttables] neo4j_query_str = %s' % neo4j_query_str)
    timer_start = time.time()

    l_query_param = None
    if query_params is not None:
        l_query_param = [query_params]
    l_ret = execute_neo4j_queries(neo4j_driver,
                                  neo4j_session_config,
                                  [query_str],
                                  l_query_param=l_query_param,
                                  need_ret=True)
    if len(l_ret[0]) <= 0:
        logging.error('[neo4j_query_to_ttables] Nothing retrieved for the query: %s' % neo4j_query_str)
        return None

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
    fd_out = snap.TFOut(''.join([out_folder, 'node_ttable%s.bin' % out_suffix]))
    node_ttable.Save(fd_out)
    fd_out.Flush()
    fd_out = snap.TFOut(''.join([out_folder, 'edge_ttable%s.bin' % out_suffix]))
    edge_ttable.Save(fd_out)
    fd_out.Flush()
    print('[neo4j_query_to_ttables] All done in %s secs.' % str(time.time() - timer_start))


if __name__ == '__main__':
    ############################################################
    #   USAGE
    #   You need to set up the environment variable 'NEO4J_HOSTNAME' to the hostname of the machine on which
    #   the Neo4j server is running.
    #   # PURGE DB
    #   > python neo4j_ops.py "neo4j_driver->purge_db"
    #   # CREATE DB
    #   > python neo4j_ops.py "neo4j_driver->create_db->create_constraints->create_indexes"
    #   # CREATE INITIAL CONTACT NETWORK
    #   > python neo4j_ops.py "neo4j_driver->build_init_cn"
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
    print('[main] g_neo4j_server_uri is set to %s' % g_neo4j_server_uri)

    cmd_pipeline = sys.argv[1]
    l_cmd = [cmd.strip().lower() for cmd in cmd_pipeline.split('->')]
    print('[main] Commands to be executed: %s' % l_cmd)

    neo4j_driver = None

    for cmd in l_cmd:
        if cmd == '':
            print('[main] Nothing to do.')

        # CONNECT TO NEO4J DRIVER
        elif cmd == 'neo4j_driver':
            logging.error('[main] neo4j_driver starts.')
            neo4j_driver = connect_to_neo4j_driver(g_neo4j_server_uri, (g_neo4j_username, g_neo4j_password),
                                                   {'max_connection_lifetime': 1000})
            logging.error('[main] neo4j_driver done.')

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
            print('[main] create_constraints done.')

        # CREATE INDEXES
        elif cmd == 'create_indexes':
            print('[main] create_indexes starts.')
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
                                                                       query_str_idx_e1, query_str_idx_e2,
                                                                       query_str_idx_e3, query_str_idx_e4])
            print('[main] create_indexes done.')

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

        # BUILD INITIAL CONTACT NETWORK WITH PRESON TRAIT
        elif cmd == 'build_init_cn':
            print('[main] build_init_cn starts.')
            create_init_cn(neo4j_driver)
            print('[main] build_init_cn done.')

        elif cmd == 'build_int_cn':
            print('[main] build_int_cn starts.')
            search_folder = g_int_cn_folder
            create_int_cn_auto_search(neo4j_driver, search_folder)
            print('[main] build_int_cn done.')

        elif cmd == 'pure_test':
            print('[main] This is a pure test.')