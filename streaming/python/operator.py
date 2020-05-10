import enum
import logging

import cloudpickle

logger = logging.getLogger(__name__)
logger.setLevel("DEBUG")


# Stream partitioning schemes
class PScheme:
    def __init__(self, strategy, partition_fn=None):
        self.strategy = strategy
        self.partition_fn = partition_fn

    def __repr__(self):
        return "({},{})".format(self.strategy, self.partition_fn)


# Partitioning strategies
class PStrategy(enum.Enum):
    Forward = 0  # Default
    Shuffle = 1
    Rescale = 2
    RoundRobin = 3
    Broadcast = 4
    Custom = 5
    ShuffleByKey = 6
    # ...


# Operator types
class OpType(enum.Enum):
    Source = 0
    Map = 1
    FlatMap = 2
    Filter = 3
    TimeWindow = 4
    KeyBy = 5
    Sink = 6
    WindowJoin = 7
    Inspect = 8
    ReadTextFile = 9
    Reduce = 10
    Sum = 11
    LocalReduce = 12
    Join = 13
    EventTimeWindow = 14
    TimeWindowJoin = 15
    Scheduler = 16
    EventSource = 17
    EventKeyBy = 18
    EventReduce = 19
    # ...


# A logical dataflow operator
class Operator:
    def __init__(self,
                 id,
                 op_type,
                 processor_class,
                 name="",
                 logic=None,
                 num_instances=1,
                 other=None,
                 state_actor=None,
                 right_stream=None,
                 left_operator_id=None,
                 right_operator_id=None):
        self.id = id
        self.type = op_type
        self.processor_class = processor_class
        self.name = name
        self._logic = cloudpickle.dumps(logic)  # The operator's logic
        self.num_instances = num_instances
        # One partitioning strategy per downstream operator (default: forward)
        self.partitioning_strategies = {}
        self.other_args = other  # Depends on the type of the operator
        self.state_actor = state_actor  # Actor to query state
        self.right_stream = right_stream   # For join operator
        self.left_operator_id = left_operator_id
        self.right_operator_id = right_operator_id
        # run time variables below
        self.instance_id = None  # For source operator
        self.num_input = None   # For EventKeyby operator
        self.stateful_op = []   # For scheduler
        self.migrater = None   # For stateful operator
        self.actor_id = None
        self.probe_writer = None
        self.prober = None

    # Sets the partitioning scheme for an output stream of the operator
    def _set_partition_strategy(self,
                                stream_id,
                                partitioning_scheme,
                                dest_operator=None):
        self.partitioning_strategies[stream_id] = (partitioning_scheme,
                                                   dest_operator)

    # Retrieves the partitioning scheme for the given
    # output stream of the operator
    # Returns None is no strategy has been defined for the particular stream
    def _get_partition_strategy(self, stream_id):
        return self.partitioning_strategies.get(stream_id)

    # Cleans metatada from all partitioning strategies that lack a
    # destination operator
    # Valid entries are re-organized as
    # 'destination operator id -> partitioning scheme'
    # Should be called only after the logical dataflow has been constructed
    def _clean(self):
        strategies = {}
        for _, v in self.partitioning_strategies.items():
            strategy, destination_operator = v
            if destination_operator is not None:
                strategies.setdefault(destination_operator, strategy)
        self.partitioning_strategies = strategies

    def print(self):
        log = "Operator<\nID = {}\nName = {}\nprocessor_class = {}\n"
        log += "Logic = {}\nNumber_of_Instances = {}\n"
        log += "Partitioning_Scheme = {}\nOther_Args = {}>\n"
        logger.debug(
            log.format(self.id, self.name, self.processor_class, self.logic,
                       self.num_instances, self.partitioning_strategies,
                       self.other_args))

    def set_instance_id(self, id):
        self.instance_id = id

    def set_num_input(self, num_input):
        self.num_input = num_input

    def set_stateful_op(self, stateful_op):
        self.stateful_op = stateful_op

    def set_migrater(self, migrater):
        self.migrater = migrater

    def set_actor_id(self, actor_id):
        self.actor_id = actor_id

    def set_probe_writer(self, probe_writer):
        self.probe_writer = probe_writer

    def set_prober(self, prober):
        self.prober = prober

    @property
    def logic(self):
        return cloudpickle.loads(self._logic)

class OperatorChain:
    def __init__(self, operator_list):
        self.operator_list = operator_list
        self.head_processor = None
        # todo: set num_instances to that of the last operator
        self.id = operator_list[0].id
        self.last_id = operator_list[-1].id
        self.num_instances = operator_list[-1].num_instances
        self.partitioning_strategies = operator_list[-1].partitioning_strategies

        self.is_join = True if (operator_list[0].name == "Join" or
                                operator_list[0].name == "TimeWindowJoin") else False
        self.is_source = True if operator_list[0].name == "Source" else False
        self.is_event_source = True if operator_list[0].name == "EventSource" else False
        self.is_eventkeyby = True if operator_list[-1].name == "EventKeyBy" else False
        self.is_stateful = True if operator_list[0].name == "EventReduce" else False
        self.is_scheduler = True if operator_list[0].name == "Scheduler" else False
        self.type = "{"
        for operator in operator_list:
            self.type += " " + operator.name
        self.type += "}"

    # only for source and event_source operator
    def set_instance_id(self, id):
        #assert self.is_source is True
        self.operator_list[0].set_instance_id(id)

    # only for event key by
    def set_num_input(self, num_input):
        assert self.is_eventkeyby is True
        self.operator_list[-1].set_num_input(num_input)

    # only for scheduler
    def set_stateful_op(self, stateful_op):
        assert self.is_scheduler is True
        self.operator_list[0].set_stateful_op(stateful_op)

    # only for stateful operators
    def set_migrater(self, migrater):
        assert self.is_stateful is True
        self.operator_list[0].set_migrater(migrater)

    def set_actor_id(self, actor_id):
        assert self.is_stateful is True
        self.operator_list[0].set_actor_id(actor_id)

    def set_probe_writer(self, probe_writer):
        assert self.is_stateful is True
        self.operator_list[0].set_probe_writer(probe_writer)

    def set_prober(self, prober):
        assert self.is_scheduler is True
        self.operator_list[0].set_prober(prober)

    def init(self, input_gate, output_gate):
        next_processor = None
        # reversely initialize each processor
        #todo: sink operator shouldn't have an output_gate
        if len(self.operator_list) == 1:
            operator = self.operator_list.pop()
            self.head_processor = operator.processor_class(operator)
            self.head_processor.set_chaining(None, input_gate, output_gate)
            return

        while True:
            operator = self.operator_list.pop()
            processor = operator.processor_class(operator)
            if len(self.operator_list) == 0:
                processor.set_chaining(next_processor, input_gate, None)
                break
            elif next_processor is None:
                processor.set_chaining(None, None, output_gate)
            else:
                processor.set_chaining(next_processor, None, None)
            next_processor = processor
        self.head_processor = processor

    def run(self):
        while True:
            if self.head_processor.process(None) is False:
                return
