"""
NOTE
    Data Structure for Contact Networks Stored in Neo4j
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
"""

import logging
import csv
import sys
import numpy as np


# g_out_folder = '/home/mf3jh/workspace/data/epihiper/'
g_out_folder = '/scratch/mf3jh/data/epihiper/'


def gen_bogus_person_trait(num_people, out_path):
    logging.debug('[gen_bogus_person_trait] Start.')

    header = ['pid', 'hid', 'age', 'age_group', 'gender','county_fips', 'home_latitude', 'home_longitude',
              'admin1', 'admin2', 'admin3', 'admin4']

    l_bogus_rec = []
    for pid in range(num_people):
        hid = np.random.randint(low=0, high=num_people, size=1)[0]
        age = int(np.abs(np.random.normal(loc=35, scale=35, size=1)[0]))
        if 0 <= age <= 4:
            age_group = 'p'
        elif 5 <= age <= 17:
            age_group = 's'
        elif 18 <= age <= 49:
            age_group = 'a'
        elif 50 <= age <= 64:
            age_group = 'o'
        elif age >= 65:
            age_group = 'g'
        else:
            logging.error('[gen_bogus_person_trait] Invalid age %s for pid %s' % (age, pid))
            age_group = 'a'
        gender = int(np.random.binomial(n=1, p=0.5, size=1)) + 1
        fips = str(np.random.randint(low=10000, high=99999, size=1)[0])
        home_lat = float(np.random.uniform(low=-90, high=90, size=1))
        home_lon = float(np.random.uniform(low=-180, high=180, size=1))
        admin1 = ''.join([str(np.random.randint(low=0, high=5, size=1)[0]),
                          str(np.random.randint(low=0, high=9, size=1)[0])])
        admin2 = str(np.random.randint(low=0, high=9, size=1)[0])
        admin3 = str(np.random.randint(low=100000, high=999999, size=1)[0])
        admin4 = str(np.random.randint(low=0, high=9, size=1)[0])
        l_bogus_rec.append([pid, hid, age, age_group, gender, fips, home_lat, home_lon, admin1, admin2, admin3, admin4])
    logging.debug('[gen_bogus_person_trait] Data is ready.')

    with open(out_path, 'w+') as csvfile:
        csv_writer = csv.writer(csvfile)
        csv_writer.writerow(header)
        csv_writer.writerows(l_bogus_rec)
    logging.debug('[gen_bogus_person_trait] All done.')


def gen_bogus_contact_network(num_edges, num_people, out_path):
    logging.debug('[gen_bogus_person_trait] Start.')

    header = ['targetPID', 'targetActivity', 'sourcePID', 'sourceActivity', 'duration']

    l_bogus_rec = []
    for i in range(int(num_edges / 2)):
        trg_pid = np.random.randint(low=0, high=num_people-1, size=1)[0]
        src_pid = np.random.randint(low=0, high=num_people-1, size=1)[0]
        if trg_pid == src_pid:
            src_pid = (src_pid + 1) % num_people
        trg_act = ''.join(['1:', str(np.random.randint(low=1, high=10, size=1)[0])])
        src_act = ''.join(['1:', str(np.random.randint(low=1, high=10, size=1)[0])])
        duration = np.random.randint(low=1, high=100000, size=1)[0]
        l_bogus_rec.append([trg_pid, trg_act, src_pid, src_act, duration])
        l_bogus_rec.append([src_pid, src_act, trg_pid, trg_act, duration])
    logging.debug('[gen_bogus_contact_network] Data is ready.')

    with open(out_path, 'w+') as csvfile:
        csv_writer = csv.writer(csvfile)
        csv_writer.writerow(header)
        csv_writer.writerows(l_bogus_rec)
    logging.debug('[gen_bogus_contact_network] All done.')


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)

    cmd = sys.argv[1]

    if cmd == 'gen_bogus_person_trait':
        num_people = 548605
        out_path = ''.join([g_out_folder, 'bogus_person_trait.csv'])
        gen_bogus_person_trait(num_people, out_path)

    elif cmd == 'gen_bogus_initial_network':
        num_edges = 371888622
        num_people = 548605
        out_path = ''.join([g_out_folder, 'bogus_initial_network.csv'])
        gen_bogus_contact_network(num_edges, num_people, out_path)

    elif cmd == 'gen_bogus_intermediate_network':
        num_edges = 251422230
        num_people = 548605
        time_point = 0
        out_path = ''.join([g_out_folder, 'bogus_intermediate_network_%s.csv' % str(time_point)])
        gen_bogus_contact_network(num_edges, num_people, out_path)