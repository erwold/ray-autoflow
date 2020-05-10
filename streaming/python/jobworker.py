import logging
import pickle
import threading

import ray
import ray.streaming._streaming as _streaming
from ray.streaming.config import Config
from ray.function_manager import FunctionDescriptor
from ray.streaming.communication import DataInput, DataOutput
import ray.streaming.runtime.transfer as transfer

logger = logging.getLogger(__name__)


@ray.remote
class JobWorker:
    """A streaming job worker.

    Attributes:
        worker_id: The id of the instance.
        input_channels: The input gate that manages input channels of
        the instance (see: DataInput in communication.py).
        output_channels (DataOutput): The output gate that manages output
         channels of the instance (see: DataOutput in communication.py).
        the operator instance.
    """

    def __init__(self, worker_id, operator_chain, input_channels, output_channels):
        self.env = None
        self.worker_id = worker_id
        self.operator_chain = operator_chain
        #processor_name = operator.processor_class.__name__
        #processor_instance = operator.processor_class(operator)
        self.processor_name = operator_chain.type
        #self.processor_instance = processor_instance
        self.input_channels = input_channels
        self.output_channels = output_channels
        self.input_gate = None
        self.output_gate = None
        self.reader_client = None
        self.writer_client = None
        self.actor_id = None

    def init(self, env):
        """init streaming actor"""
        env = pickle.loads(env)
        self.env = env
        self.actor_id = ray.worker.global_worker.actor_id
        logger.info("init operator instance %s", self.processor_name)

        if env.config.channel_type == Config.NATIVE_CHANNEL:
            core_worker = ray.worker.global_worker.core_worker
            reader_async_func = FunctionDescriptor(
                __name__, self.on_reader_message.__name__,
                self.__class__.__name__)
            reader_sync_func = FunctionDescriptor(
                __name__, self.on_reader_message_sync.__name__,
                self.__class__.__name__)
            self.reader_client = _streaming.ReaderClient(
                core_worker, reader_async_func, reader_sync_func)
            writer_async_func = FunctionDescriptor(
                __name__, self.on_writer_message.__name__,
                self.__class__.__name__)
            writer_sync_func = FunctionDescriptor(
                __name__, self.on_writer_message_sync.__name__,
                self.__class__.__name__)
            self.writer_client = _streaming.WriterClient(
                core_worker, writer_async_func, writer_sync_func)
        if len(self.input_channels) > 0:
            self.input_gate = DataInput(env, self.input_channels)
            if self.operator_chain.is_join:
                self.input_gate.set_join(self.operator_chain.operator_list[0])
            self.input_gate.init()
        if len(self.output_channels) > 0:
            self.output_gate = DataOutput(
                env, self.output_channels,
                self.operator_chain.partitioning_strategies)
            self.output_gate.init()
        if self.operator_chain.is_source or self.operator_chain.is_event_source:
            self.operator_chain.set_instance_id(self.worker_id[1])
        if self.operator_chain.is_eventkeyby:
            self.operator_chain.set_num_input(len(self.input_channels))
        if self.operator_chain.is_stateful:
            self.input_gate.set_stateful()
            self.migrater = transfer.StateMigrater(self.actor_id)
            self.operator_chain.set_migrater(self.migrater) 
            self.operator_chain.set_actor_id(self.actor_id)
            self.probe_writer = transfer.ProbeWriter(self.get_scheduler_id())
            self.operator_chain.set_probe_writer(self.probe_writer)
        if self.operator_chain.is_scheduler:
            self.operator_chain.set_stateful_op(self.env.execution_graph.stateful_op)
            self.prober = transfer.FlowProber()
            self.operator_chain.set_prober(self.prober)
        logger.info("init operator instance %s succeed", self.processor_name)
        return True

    # Starts the actor
    def start(self):
        self.t = threading.Thread(target=self.run, daemon=True)
        self.t.start()
        # actor_id = ray.worker.global_worker.actor_id
        logger.info("%s %s started, actor id %s", self.__class__.__name__,
                    self.processor_name, self.actor_id)

    def run(self):
        self.operator_chain.init(self.input_gate, self.output_gate)
        logger.info("%s start running", self.processor_name)
        self.operator_chain.run()
        logger.info("%s finished running", self.processor_name)
        self.close()

    def close(self):
        if self.input_gate:
            self.input_gate.close()
        if self.output_gate:
            self.output_gate.close()

    def is_finished(self):
        return not self.t.is_alive()

    def on_reader_message(self, buffer: bytes):
        """used in direct call mode"""
        self.reader_client.on_reader_message(buffer)

    def on_reader_message_sync(self, buffer: bytes):
        """used in direct call mode"""
        if self.reader_client is None:
            return b" " * 4  # special flag to indicate this actor not ready
        result = self.reader_client.on_reader_message_sync(buffer)
        return result.to_pybytes()

    def on_writer_message(self, buffer: bytes):
        """used in direct call mode"""
        self.writer_client.on_writer_message(buffer)

    def on_writer_message_sync(self, buffer: bytes):
        """used in direct call mode"""
        if self.writer_client is None:
            return b" " * 4  # special flag to indicate this actor not ready
        result = self.writer_client.on_writer_message_sync(buffer)
        return result.to_pybytes()

    def get_scheduler_id(self):
        return self.env.execution_graph.get_actor(0, 0)._ray_actor_id
