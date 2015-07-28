import pickle
from kazoo.client import KazooClient
from kazoo.exceptions import (
    ZookeeperError,
    NodeExistsError,
    NoNodeError
)
from concord.computation import (
    serve_computation
)

import logging
import logging.handlers

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class ConcordDecorator:
    def __init__(self, computation_name, zookeeper_url, test_id, node_id):
        assert(metadat != None)
        assert(zookeepr_url != None)
        assert(test_id != None)
        assert(node_id != None)

        self.computation_name = computation_name
        self.zookeeper_url = zookeeper_url
        self.test_id = test_id
        self.node_id = node_id
        self.zk_path = '/bolt/testing/%s/%s/%s' % (
            self.test_id, self.computation_name, self.node_id)
        logger.info("Initialized decorator: ", self)
        self.__connect_zookeeper()


    def __connect_zookeeper(self):
        try:
            logger.info('Attempting to connect to: %s' % self.zookeeper_url)
            self.zk = KazooClient(hosts=self.zookeeper_url)
            self.zk.start(timeout=10)
            logger.info('Zookeeper: ', self.zk)
        except Exception as exception:
            logger.error('Failed to connect to zk')
            logger.fatal(exception)


    def __str__(self):
        return json.dumps(self, default=lambda o: o.__dict__, indent=4)

    def __del__(self):
        self.zk.stop()

    def publish(self, key, data):
        assert(key != None)
        assert(data != None)
        path = self.zk_path + "/%s" + key
        logger.info("Creating data in: %s", path)
        try:
            if not self.zk.exists(path):
                logger.info("Path does not exist in zk, creating... %s", path)
                self.zk.create(path, makepath=True)

            self.zk.set(path, value=picke.dumps(data))
        except Exception as exception:
            logger.error('Error setting data in ' % path)
            logger.fatal(exception)


def tryGetEnv(key):
    try:
        return os.environ[key]
    except Exception as e:
        logger.error('Error getting os.environ[%s]' % key)
        logger.fatal(exception)

def serve_test_computation(handler):
    logger.info("About to serve computation and service")
    zookeeper_url = tryGetEnv('integration_test_zookeeper_url')
    test_id = tryGetEnv('integration_test_id')
    node_id = tryGetEnv('integration_test_node_id')

    handler.__concord = ConcordDecorator(handler.metadata().name,
                                         zookeeper_url,
                                         test_id,
                                         node_id)
    logger.info("Defering further init: concord.computation.serve_computation")
    serve_computation(handler)
