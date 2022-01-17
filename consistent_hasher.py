from bisect import bisect, bisect_left, bisect_right
import random
import hashlib

class ConsistentHasher:
'''
Library to help map a set of physical nodes in a cluster to a consistent hash ring so that keys of a distributed key-value or wide-column store can be hashed to one of these nodes. It also supports virtual nodes so that rebalancing impact and costs are minimal.
'''
    def __init__(self, nodes = None, capacity = 100):
        '''
        - capacity: number of partitions needed in the key-value store. For simplicity, this implementation chooses number of partitions at the outset. This will have problems if we get this number wrong :).
        - vnodes: list of all virtual nodes that a key can hash to. Multiple vnodes will map to one physical node. vnodes length is fixed and is set based on capacity.
        - nodes: list of all physical nodes that are in the topology. It is possible to add/remove node.
        '''
        self.nodes = []
        self.capacity = capacity
        self.vnodes = [-1] * capacity
        if nodes:
            self.initialize(nodes, capacity)
        return
    
    def initialize(self, nodes, capacity):
        nodes_count = len(nodes)
        init_vnodes = capacity // nodes_count
        init_vnodes_rem = capacity - (init_vnodes * nodes_count)

        for i, node in enumerate(nodes):
            if i == nodes_count - 1:
                init_vnodes += init_vnodes_rem
            self.add_node(node, init_vnodes)

        print(self.nodes)
        print(self.vnodes)
        return

    def get_hash(self, key):
        hsh = hashlib.sha256()
        hsh.update(bytes(key.encode('utf-8')))
        return int(hsh.hexdigest(), 16) % self.capacity

    def add_node(self, node, init_vnodes = 0):
        print(node.host_name, init_vnodes, len([i for i in range(self.capacity) if self.vnodes[i] == -1]))
        if len(self.nodes) == self.capacity:
            return 'Trifledb operating with max. possible nodes. Unable to add a new one. Consider removing an existing node if necessary.'
        
        if node.vnodes > self.capacity:
            return 'Vnode count not supported.'

        #add node to nodes collection
        node_hash = self.get_hash(node.host_name)
        self.nodes.append((node_hash, node))
        node_number = len(self.nodes) - 1

        #map node to vnodes
        if init_vnodes == 0:
            vnodes_selected = random.sample(population=range(self.capacity), k=node.vnodes)
        else:
            vnodes_selected = random.sample(population=[i for i in range(self.capacity) if self.vnodes[i] == -1], k=init_vnodes)
        print(len(vnodes_selected), vnodes_selected)
        self.vnodes = [node_number if i in vnodes_selected else self.vnodes[i] for i in range(self.capacity)]

        # rebalance TODO
        return

    def remove_node(self, node):
        #todo
        return

    def find_node(self, key):
        key_hash = self.get_hash(key)
        index = bisect_right(self.vnodes, key_hash) % self.capacity
        node = self.nodes[self.vnodes[index]][1]
        print(key, key_hash, index, node)
        return node