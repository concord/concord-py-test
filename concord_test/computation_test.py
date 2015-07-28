import pickle
from kazoo.client import KazooClient
from kazoo.exceptions import (
    ZookeeperError,
    NodeExistsError,
    NoNodeError
)
from concord.computation import (
    ComputationServiceWrapper,
    Metadata,
)

import logging
import logging.handlers

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

class ComputationTestServiceWrapper(ComputationServiceWrapper):
    """
    """

    def __init__(self, handler, zookeeper_addr, test_id, node_id):
        ComputationServiceWrapper.__init__(self, handler)
        self.test_id = test_id
        self.node_id = node_id
        self.computation_name = None
        try:
            logger.info('Attempting to connect to: %s' % zookeeper_addr)
            self.zk = KazooClient(hosts=zookeeper_addr)
            self.zk.start(timeout=10)
        except Exception as exception:
            logger.error('Failed to connect to zk')
            logger.fatal(exception)

    def __del__(self):
        self.zk.stop()

    def context_type(self):
        return self

    def zk_path(self):
        # boltMetadata will be called before any method, assigning computation_name
        assert(self.computation_name != None)
        zk_path = '/bolt/testing/%s/%s/%s' % (self.test_id, self.computation_name, self.node_id)
        return '%s/%s' % (base_path, self.computation_name)

    def zk_record(self, data):
        zk_path = self.zk_path()
        try:
            self.zk.set(zk_path, value=data)
            logger.info('Writing init data to path %s' % zk_path)
        except NoNodeError as exception:
            logger.error('NoNodeError for path %s' % zk_path)
            logger.fatal(exception)

    def produce_record(self, stream, key, value):
        self.zk_record('inside of produce_record!')

    def set_timer(self, key, time):
        self.zk_record('inside of set_timer!')

    def init(self):
        transaction = ComputationServiceWrapper.init(self)
        self.zk_record('inside of init!')
        return transaction

    def boltProcessRecord(self, record):
        transaction = ComputationServiceWrapper.boltProcessRecord(self, record)
        self.zk_record('inside of processRecord!')
        return transaction

    def boltProcessTimer(self, key, time):
        transaction = ComputationServiceWrapper.boltProcessTimer(self, key, time)
        self.zk_record('inside of processTimer!')
        return transaction

    def boltMetadata(self):
        metadata = ComputationServiceWrapper.boltMetadata(self)
        logger.info('Inside bolt metadata!')
        self.computation_name = metadata.name
        try:
            zk_path = '/bolt/testing/%s/%s' % (self.test_id, self.computation_name)
            logger.info('Writing to path %s' % zk_path)
            new_path = self.zk.create(zk_path, value=pickle.dumps(metadata))
            logger.info('Created %s' % new_path)
        except NodeExistsError as exception:
            logger.error('NodeExistsError for path %s' % zk_path)
            logger.fatal(exception)
        return metadata

def serve_test_computation(handler):
    """Helper function. Parses environment variables and starts a thrift service
        wrapping the user-defined computation.
    :param handler: The user computation.
    :type handler: Computation.
    """
    logger.info("About to serve computation and service")

    def address_str(address):
        host, port = address.split(':')
        return (host, int(port))

    _, listen_port = address_str(
        os.environ[kConcordEnvKeyClientListenAddr])
    proxy_host, proxy_port = address_str(
        os.environ[kConcordEnvKeyClientProxyAddr])

    zookeeper_addr = os.environ['integration_test_zookeeper_addr']
    test_id = os.environ['integration_test_id']
    node_id = os.environ['integration_test_node_id']
    comp = ComputationTestServiceWrapper(handler,
                                         zookeeper_addr,
                                         test_id,
                                         node_id)
    processor = ComputationService.Processor(comp)
    transport = TSocket.TServerSocket(port=listen_port)
    server = TNonblockingServer.TNonblockingServer(processor, transport)

    def thrift_service():
        logger.info("Starting python service port: %d", listen_port)
        server.serve()
        logger.error("Exciting service")

    try:
        logger.info("registering with framework at: %s:%d",
                            proxy_host, proxy_port)
        comp.set_proxy_address(proxy_host, proxy_port)
        thrift_service()
    except Exception as exception:
        logger.fatal(exception)
        logger.error("Exception in python client")
        server.stop()
        raise exception
