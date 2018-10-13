from collections import defaultdict
import sys


def get_avg(files):
    request_deltas = list()
    for file in files:
        requests = defaultdict(dict)
        with open(file, 'r') as f:
            for line in f.readlines():
                if line[0] == '!' and 'search request' in line:
                    data = line.split()
                    requests[data[0][1]].update({data[-1][1:-1]:data[1][1:-1]})
        for request in requests.values():
            request_deltas.append(int(request['end']) - int(request['start']))
    return sum(request_deltas) / len(request_deltas)

config = sys.argv[1]

one_client_avg = get_avg(['{}/1/55010_client.log'.format(config)])

two_clients_avg = get_avg(["{}/2/55{:03d}_client.log".format(config, i) for i in range(10, 12)])

three_clients_avg = get_avg(["{}/3/55{:03d}_client.log".format(config, i) for i in range(10, 13)])

four_clients_avg = get_avg(["{}/4/55{:03d}_client.log".format(config, i) for i in range(10, 14)])

five_clients_avg = get_avg(["{}/5/55{:03d}_client.log".format(config, i) for i in range(10, 15)])

six_clients_avg = get_avg(["{}/6/55{:03d}_client.log".format(config, i) for i in range(10, 16)])

seven_clients_avg = get_avg(["{}/7/55{:03d}_client.log".format(config, i) for i in range(10, 17)])

eight_clients_avg = get_avg(["{}/8/55{:03d}_client.log".format(config, i) for i in range(10, 18)])

nine_clients_avg = get_avg(["{}/9/55{:03d}_client.log".format(config, i) for i in range(10, 19)])

ten_clients_avg = get_avg(["{}/10/55{:03d}_client.log".format(config, i) for i in range(10, 20)])

print one_client_avg, two_clients_avg, three_clients_avg, four_clients_avg, five_clients_avg, six_clients_avg, seven_clients_avg, eight_clients_avg, nine_clients_avg, ten_clients_avg