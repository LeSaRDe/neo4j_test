import logging
import csv
import random

g_contact_network_int_file_fmt = '/project/bii_nssac/COVID-19_USA_EpiHiper/rivanna/20211020-network_query/wy' \
                                 '/replicate_0/network[{0}]'
g_sample_contact_network_int_file_fmt = '/scratch/mf3jh/data/epihiper/network_{0}'

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)

    sample_size = 1000000
    num_int = 10
    header = None
    for int_idx in range(num_int):
        logging.debug('Start with %s' % int_idx)
        ln_cnt = 0
        # READ IN CSV
        with open(g_contact_network_int_file_fmt.format(str(int_idx)), 'r') as in_fd:
            csv_reader = list(csv.reader(in_fd, delimiter=','))
            header = csv_reader[1]
            l_sample = random.choices(csv_reader[2:], k=sample_size)
        # WRITE OUT
        with open(g_sample_contact_network_int_file_fmt.format(str(int_idx)), 'w+') as out_fd:
            csv_writer = csv.writer(out_fd, delimiter=',')
            csv_writer.writerow(header)
            csv_writer.writerows(l_sample)
    logging.debug('All done.')
