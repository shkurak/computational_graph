# ComputeGraph Homework
Library for map-reduce computations over tables
Table is sequence of dict-like objects (python dictionaries or json objects), an every dictionary is row of table and key of a dictionary is a column of table.
Keys should be same for every dictionary in table. It's possible to sustain culculations over a table. The library provide interface for definition of 
operations over the table by generating computational graph over any table. 
# The process of computations
1) Build computational graph for some kind of table using operations map, reduce, join, fold and sort.
2) Run the graph on any table.

## Graph building
Example:
```
ComputeGraph(input_stream).map(emit_words).sort("word").reduce(collect_counts, "word").sort("count")
```
# Operations
### Map
Example of mapper:
```
def tokenizer_mapper(r):
  """
     splits rows with 'text' field into set of rows with 'token' field
    (one for every occurence of every word in text)
  """

  tokens = r['text'].split()

  for token in tokens:
    yield {
      'doc_id' : r['doc_id'],
      'word' : token,
    }
```
### Sort
Sort a table over some key

### Fold
Example of folder:
```
def sum_columns_folder(state, record):
    for column in state:
        state[column] += record[column]
    return state
```

### Reduce
Example of reduce
```
def term_frequency_reducer(records):
    word_count = Counter()

    for r in records:
        word_count[r['word']] += 1

    total  = sum(word_count.values)
    for w, count in word_count.items():
        yield {
            'doc_id' : r['doc_id'],
            'word' : w,
            'tf' : count / total
        }
```
        
### Join
Join works as join in SQL. There are 4 kinds of joins available inner, left, right and outer.