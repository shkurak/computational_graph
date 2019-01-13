from operator import itemgetter
from compgraph.graph import ComputeGraph
import algorithms


def test_map():
    docs = [
        {'doc_id': 1, 'text': 'hello, my little WORLD'},
        {'doc_id': 2, 'text': 'Hello, my little little hell'}
    ]

    etalon = [
        {'count': 1, 'doc_id': 2, 'word': 'hell'},
        {'count': 1, 'doc_id': 1, 'word': 'world'},
        {'count': 1, 'doc_id': 1, 'word': 'hello'},
        {'count': 1, 'doc_id': 2, 'word': 'hello'},
        {'count': 1, 'doc_id': 1, 'word': 'my'},
        {'count': 1, 'doc_id': 2, 'word': 'my'},
        {'count': 1, 'doc_id': 1, 'word': 'little'},
        {'count': 1, 'doc_id': 2, 'word': 'little'},
        {'count': 1, 'doc_id': 2, 'word': 'little'}
    ]
    graph = ComputeGraph('docs').map(algorithms.emit_words)
    assert isinstance(graph, ComputeGraph)
    result = graph.run(docs=docs)
    assert sorted(etalon,
                  key=itemgetter('count', 'doc_id', 'word')) == sorted(result,
                                                                       key=itemgetter('count', 'doc_id', 'word'))


def test_sort():
    docs = [
        {'count': 1, 'doc_id': 2, 'word': 'hell'},
        {'count': 1, 'doc_id': 1, 'word': 'world'},
        {'count': 1, 'doc_id': 1, 'word': 'hello'},
        {'count': 1, 'doc_id': 2, 'word': 'hello'},
        {'count': 1, 'doc_id': 1, 'word': 'my'},
        {'count': 1, 'doc_id': 2, 'word': 'my'},
        {'count': 1, 'doc_id': 1, 'word': 'little'},
        {'count': 1, 'doc_id': 2, 'word': 'little'},
        {'count': 1, 'doc_id': 2, 'word': 'little'}
    ]
    graph = ComputeGraph('docs').sort("word")
    assert isinstance(graph, ComputeGraph)
    result = graph.run(docs=docs)
    assert sorted(docs, key=itemgetter('word')) == result


def test_reduce():
    docs = [
        {'count': 1, 'doc_id': 2, 'word': 'hell'},
        {'count': 1, 'doc_id': 1, 'word': 'world'},
        {'count': 1, 'doc_id': 1, 'word': 'hello'},
        {'count': 1, 'doc_id': 2, 'word': 'hello'},
        {'count': 1, 'doc_id': 1, 'word': 'my'},
        {'count': 1, 'doc_id': 2, 'word': 'my'},
        {'count': 1, 'doc_id': 1, 'word': 'little'},
        {'count': 1, 'doc_id': 2, 'word': 'little'},
        {'count': 1, 'doc_id': 2, 'word': 'little'}
    ]
    etalon = [{'text': 'hell', 'count': 1},
              {'text': 'hello', 'count': 2},
              {'text': 'little', 'count': 3},
              {'text': 'my', 'count': 2},
              {'text': 'world', 'count': 1}]
    graph = ComputeGraph('docs').reduce(algorithms.collect_counts, "word")
    assert isinstance(graph, ComputeGraph)
    result = graph.run(docs=sorted(docs, key=itemgetter('word')))
    assert etalon == result


def test_fold():
    def word_counter(state, record):
        if state:
            return {"words": state["words"] + 1}
        return {"words": 1}
    docs = [
        {'count': 1, 'doc_id': 2, 'word': 'hell'},
        {'count': 1, 'doc_id': 1, 'word': 'world'},
        {'count': 1, 'doc_id': 1, 'word': 'hello'},
        {'count': 1, 'doc_id': 2, 'word': 'hello'},
        {'count': 1, 'doc_id': 1, 'word': 'my'},
        {'count': 1, 'doc_id': 2, 'word': 'my'},
        {'count': 1, 'doc_id': 1, 'word': 'little'},
        {'count': 1, 'doc_id': 2, 'word': 'little'},
        {'count': 1, 'doc_id': 2, 'word': 'little'}
    ]
    etalon = [{"words": 9}]
    graph = ComputeGraph('docs').fold(word_counter)
    assert isinstance(graph, ComputeGraph)
    result = graph.run(docs=docs)
    assert etalon == result


employee_table = [{"LastName": "Rafferty", "DepartmentID": 31},
                  {"LastName": "Jones", "DepartmentID": 33},
                  {"LastName": "Heisenberg", "DepartmentID": 33},
                  {"LastName": "Robinson", "DepartmentID": 34},
                  {"LastName": "Smith", "DepartmentID": 34},
                  {"LastName": "Williams", "DepartmentID": 0}]
department_table = [{"DepartmentID": 31, "DepartmentName": "Sales"},
                    {"DepartmentID": 33, "DepartmentName": "Enginnering"},
                    {"DepartmentID": 34, "DepartmentName": "Clerical"},
                    {"DepartmentID": 35, "DepartmentName": "Marketing"}]


def test_join_same_keys():
    graph_error = ComputeGraph('employee_table').join("department_table", type="inner")
    assert isinstance(graph_error, ComputeGraph)
    try:
        graph_error.run(employee_table=employee_table, department_table=department_table)
    except KeyError as exp:
        assert exp.args[0] == "Same keys in left and right table beyond join keys"


def test_join_inner():
    etalon_inner = [{'DepartmentID': 31, 'DepartmentName': 'Sales', 'LastName': 'Rafferty'},
                    {'DepartmentID': 33, 'DepartmentName': 'Enginnering', 'LastName': 'Jones'},
                    {'DepartmentID': 33, 'DepartmentName': 'Enginnering', 'LastName': 'Heisenberg'},
                    {'DepartmentID': 34, 'DepartmentName': 'Clerical', 'LastName': 'Robinson'},
                    {'DepartmentID': 34, 'DepartmentName': 'Clerical', 'LastName': 'Smith'}]

    graph_inner = ComputeGraph('employee_table').join("department_table", keys="DepartmentID", type="inner")
    result = graph_inner.run(employee_table=employee_table, department_table=department_table)
    assert etalon_inner == result


def test_join_left():
    etalon_left = [{'DepartmentID': 31, 'DepartmentName': 'Sales', 'LastName': 'Rafferty'},
                   {'DepartmentID': 33, 'DepartmentName': 'Enginnering', 'LastName': 'Jones'},
                   {'DepartmentID': 33, 'DepartmentName': 'Enginnering', 'LastName': 'Heisenberg'},
                   {'DepartmentID': 34, 'DepartmentName': 'Clerical', 'LastName': 'Robinson'},
                   {'DepartmentID': 34, 'DepartmentName': 'Clerical', 'LastName': 'Smith'},
                   {'DepartmentID': 35, 'DepartmentName': 'Marketing'}]
    graph_left = ComputeGraph('employee_table').join("department_table", keys="DepartmentID", type="left")
    result = graph_left.run(employee_table=employee_table, department_table=department_table)
    assert etalon_left == result


def test_join_right():
    etalon_right = [{'LastName': 'Williams', 'DepartmentID': 0},
                    {'DepartmentID': 31, 'DepartmentName': 'Sales', 'LastName': 'Rafferty'},
                    {'DepartmentID': 33, 'DepartmentName': 'Enginnering', 'LastName': 'Jones'},
                    {'DepartmentID': 33, 'DepartmentName': 'Enginnering', 'LastName': 'Heisenberg'},
                    {'DepartmentID': 34, 'DepartmentName': 'Clerical', 'LastName': 'Robinson'},
                    {'DepartmentID': 34, 'DepartmentName': 'Clerical', 'LastName': 'Smith'}]
    graph_right = ComputeGraph('employee_table').join("department_table", keys="DepartmentID", type="right")
    result = graph_right.run(employee_table=employee_table, department_table=department_table)
    assert etalon_right == result


def test_join_outer():
    etalon_outer = [{'LastName': 'Williams', 'DepartmentID': 0},
                    {'DepartmentID': 31, 'DepartmentName': 'Sales', 'LastName': 'Rafferty'},
                    {'DepartmentID': 33, 'DepartmentName': 'Enginnering', 'LastName': 'Jones'},
                    {'DepartmentID': 33, 'DepartmentName': 'Enginnering', 'LastName': 'Heisenberg'},
                    {'DepartmentID': 34, 'DepartmentName': 'Clerical', 'LastName': 'Robinson'},
                    {'DepartmentID': 34, 'DepartmentName': 'Clerical', 'LastName': 'Smith'},
                    {'DepartmentID': 35, 'DepartmentName': 'Marketing'}]
    graph_outer = ComputeGraph('employee_table').join("department_table", keys="DepartmentID", type="outer")
    result = graph_outer.run(employee_table=employee_table, department_table=department_table)
    assert etalon_outer == result
