from subprocess import call, Popen, PIPE
from sys import argv
from multiprocessing import Process
import os
import sys
import time


os.chdir("../src/")


def run_peer(id_, config_dir):
    cmd = ['./super_peer', id_, config_dir]
    Popen(cmd, stdin=PIPE, stdout=open(os.devnull, 'w'))

peers = [(str(i), '../config/{}.cfg'.format(sys.argv[2])) for i in range(10)]

for peer in peers:
    p = Process(target=run_peer, args=peer)
    p.start()


time.sleep(5)


def run_node(id_, config_dir, files_dir, does_queries):
    cmd = ['./leaf_node', id_, config_dir, files_dir]
    node = Popen(cmd, stdin=PIPE if does_queries else None, stdout=open(os.devnull, 'w'))

    time.sleep(5)
    if does_queries:
        for _ in range(200):
            node.stdin.write('s\na.txt\n')
            node.stdin.flush()
    time.sleep(120)

nodes = [(str(i), '../config/{}.cfg'.format(sys.argv[2]), 'nodes/n{}/'.format(i), i < int(sys.argv[1])) for i in range(19)]

for node in nodes:
    p = Process(target=run_node, args=node)
    p.start()


time.sleep(120)

os.system('killall leaf_node')
os.system('killall super_peer')

os.system('mv logs/leaf_nodes/* ../evaluation/{}/{}/'.format(sys.argv[2], sys.argv[1]))
os.system('rm logs/super_peers/*')
