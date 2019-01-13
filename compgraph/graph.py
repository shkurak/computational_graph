import itertools
from operator import itemgetter
from typing import Iterable, Tuple, Dict, List, Callable, Union


def join_dicts(product: Iterable[Tuple[Dict, Dict]], keys: Iterable[str]) -> Iterable[Dict]:
    for left, right in product:
        result = dict(left)
        for right_key in right:
            if right_key not in left:
                result[right_key] = right[right_key]
            elif right_key not in keys:
                raise KeyError("Same keys in left and right table beyond join keys")
        yield result


class ComputationalNode(object):
    def __init__(self, operation_stack, inputs):
        self._queue = operation_stack
        self._inputs = inputs

    def _compute(self):
        cur_generator = self._inputs[0]._compute()
        for operation in self._queue:
            cur_generator = self._do_operation(operation, cur_generator)
        yield from cur_generator

    def _do_operation(self, operation, generator):
        if operation["type"] == "map":
            return self._run_map(operation["mapper"], generator)
        elif operation["type"] == "reduce":
            return self._run_reduce(operation["reducer"], operation["keys"], generator)
        elif operation["type"] == "sort":
            return self._run_sort(operation["keys"], generator, operation["reverse"])
        elif operation['type'] == "fold":
            return self._run_fold(operation["folder"], generator)
        elif operation["type"] == "join":
            return self._run_join(operation["index"], operation["keys"], operation["join_type"], generator)
        else:
            return generator

    def _run_join(self, index, keys, join_type, generator):
        key_func = itemgetter(*keys) if keys else lambda x: ()
        left = itertools.groupby(sorted(self._inputs[index]._compute(), key=key_func), key=key_func)
        right = itertools.groupby(sorted(generator, key=key_func), key=key_func)
        default_value = (None, None)
        key_left, group_left = next(left, default_value)
        key_right, group_right = next(right, default_value)
        while group_left is not None and group_right is not None:
            if key_left == key_right:
                yield from join_dicts(itertools.product(group_left, group_right), keys)
                key_left, group_left = next(left, default_value)
                key_right, group_right = next(right, default_value)
            elif key_left < key_right:
                if join_type == "left" or join_type == "outer":
                    yield from group_left
                key_left, group_left = next(left, default_value)
            else:
                if join_type == "right" or join_type == "outer":
                    yield from group_right
                key_right, group_right = next(right, default_value)

        while group_left is not None and (join_type == "outer" or join_type == "left"):
            yield from group_left
            key_left, group_left = next(left, default_value)

        while group_right is not None and (join_type == "outer" or join_type == "right"):
            yield from group_right
            key_right, group_right = next(right, default_value)

    def _run_reduce(self, reducer, keys, generator):
        for key, group in itertools.groupby(generator, itemgetter(*keys)):
            yield from reducer(group)

    def _run_map(self, mapper, generator):
        for line in generator:
            yield from mapper(line)

    def _run_fold(self, folder, generator):
        state = None
        for line in generator:
            state = folder(state, line)
        yield state

    def _run_sort(self, keys, generator, reverse):
        generator = list(generator)
        yield from sorted(generator, key=itemgetter(*keys), reverse=reverse)


class ComputeGraph(object):
    def __init__(self, input):
        self._inputs = [input]
        self._queue = [{"type": "input",
                        "input_index": 0}]
        self._kwargs = None
        self._inputs_used = None
        self.times_used = 0
        self.saved = None
        self.processed_inputs = None

    def _go_deeper(self, inputs_used):
        self.processed_inputs = []
        for i in range(len(self._inputs)):
            if isinstance(self._inputs[i], ComputeGraph):
                self._inputs[i]._go_deeper(inputs_used)
                self.processed_inputs.append(self._inputs[i])
            elif isinstance(self._inputs[i], str):
                self.processed_inputs.append(inputs_used[self._inputs[i]])
            self.processed_inputs[-1].times_used += 1

    def run(self, **kwargs: Iterable[Dict]) -> List[Dict]:
        """
        Computes ComputeGraph over inputs.
        :param kwargs: inputs of ComputeGraph. All nicknames should be resolved
        :return: list - result table
        """
        inputs_used = {key: Opener(value) for key, value in kwargs.items()}
        self._go_deeper(inputs_used)
        return list(self._compute())

    def _compute(self):
        if self.saved is None:
            node = ComputationalNode(self._queue, self.processed_inputs)
            cur_generator = node._compute()
            if self.times_used > 1:
                self.saved = list(cur_generator)
                yield from self.saved
            else:
                yield from cur_generator
        else:
            self.times_used -= 1
            result = self.saved
            if self.times_used == 0:
                del self.saved
            yield from result

    def map(self, mapper: Callable[[Dict], Iterable[Dict]]) -> "ComputeGraph":
        """
        Method of ComputeGraph that add map operation to computational graph. Apply mapper to every line in table
        :param mapper: generator that takes one line from table and returns
        form 1 to any number of lines of result table
        :return: ComputeGraph
        """
        vertex = {"type": "map",
                  "mapper": mapper}
        self._queue.append(vertex)
        return self

    def reduce(self, reducer: Callable[[Iterable[Dict]], Iterable[Dict]], keys: Iterable[str]) -> "ComputeGraph":
        """
        Method of ComputeGraph that add reduce operation to computational graph. Apply reducer to every subtable
        with same keys
        :param reducer: generator that takes lines from table with sames keys and returns
        form 1 to any number of lines of result table
        :param keys: tuple of strings
        :return: ComputeGraph
        """
        if isinstance(keys, str):
            keys = [keys]
        vertex = {"type": "reduce",
                  "reducer": reducer,
                  "keys": keys}
        self._queue.append(vertex)
        return self

    def sort(self, *keys: Union[str, Iterable[str]], reverse: bool = False) -> "ComputeGraph":
        """
        Method of ComputeGraph that sort table by keys
        :param keys: any number of strings
        :param reverse: bool True if result table should be reversed
        :return: ComputeGraph
        """
        if len(keys) == 1 and not isinstance(keys[0], str):
            keys = keys[0]
        vertex = {"type": "sort",
                  "keys": keys,
                  "reverse": reverse}
        self._queue.append(vertex)
        return self

    def fold(self, folder: Callable[[Union[Dict, None], Dict], Dict]) -> "ComputeGraph":
        """
        Method of ComputeGraph that add fold operation to ComputeGraph. Apply folder function to each line in table
        :param folder:
        :return: ComputeGraph
        """
        vertex = {"type": "fold",
                  "folder": folder}
        self._queue.append(vertex)
        return self

    def join(self, other_input: Union[Iterable[Dict], "ComputeGraph"],
             keys: Union[Iterable[str], str, None] = None,
             type: str = "inner") -> "ComputeGraph":
        """
        Method of ComputeGraph that do join of current table and other table or input
        :param other_input: other ComputeGraph or str (nickname of input).
        If str is given, it should be resolved in run method
        :param keys: string of tuple of strings on which do join
        :param type: type of join "inner", "left", "right", "outer"
        :return: ComputeGraph
        """
        if isinstance(keys, str):
            keys = (keys, )
        if not keys:
            keys = ()
        vertex = {"type": "join",
                  "index": len(self._inputs),
                  "keys": keys,
                  "join_type": type}
        self._queue.append(vertex)
        self._inputs.append(other_input)
        return self


class Opener(object):
    def __init__(self, input):
        self.iterator = input
        self.times_used = 0
        self.saved = None

    def _compute(self):
        if self.saved is None:
            if self.times_used > 1:
                self.saved = list(self.iterator)
                yield from self.saved
            else:
                yield from self.iterator
        else:
            self.times_used -= 1
            result = self.saved
            if self.times_used == 0:
                del self.saved
            yield from result
