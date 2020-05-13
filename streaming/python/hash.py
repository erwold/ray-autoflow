import xxhash
import logging

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# Used to choose output channel in case of hash-based shuffling
def _hash(value):
    if isinstance(value, int):
        return xxhash.xxh32_intdigest(str(value))
    else:
        return xxhash.xxh32_intdigest(value)

class Hash(object):
    def __init__(self, nodes, virtual_num):
        self.nodes = nodes
        self.vnum = virtual_num
        self.vnodes = {}
        for node in self.nodes:
            for i in range(self.vnum):
                v_id = "{}-{}".format(node, i)
                self.vnodes[_hash(v_id)] = (v_id, node)
        self.sorted_keys = sorted(self.vnodes.keys())
        logger.info("[LPQINFO] keys {}".format(self.sorted_keys))

    def get(self, key):
        key = _hash(key)
        #logger.info("get key {}".format(key))
        position = self.sorted_keys[0]
        for k in self.sorted_keys:
            if key < k:
                return self.vnodes[k]
        return self.vnodes[position]

    # change routing table
    def set(self, v_id, dst_node):
        self.vnodes[_hash(v_id)] = (v_id, dst_node)
