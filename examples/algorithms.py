from operator import itemgetter
import math
import datetime
import string
import heapq
from compgraph.graph import ComputeGraph


def _filter_punctuation(txt):
    p = set(string.punctuation)
    return "".join(c for c in txt if c not in p)


def extract_words(text):
    for word in _filter_punctuation(text).split():
        if word:
            yield word.lower().strip()


def emit_words(record):
    for word in extract_words(record["text"]):
        yield {'doc_id': record['doc_id'], 'word': word, 'count': 1}


def collect_counts(records):
    count_sum = 0
    for record in records:
        count_sum += record["count"]
    yield {'text': record['word'], "count": count_sum}


def collect_words(records):
    count_sum = 0
    for record in records:
        count_sum += record["count"]
    yield {'word': record['word'], "count": count_sum}


def count_rows(state, record):
    if state is None:
        state = {"docs_count": 0}
    return {'docs_count': state['docs_count'] + 1}


def unique(records):
    for record in records:
        yield {"doc_id": record["doc_id"], "word": record["word"]}
        break


def calc_idf(records):
    records = list(records)
    for record in records:
        yield {"doc_id": record["doc_id"], "word": records[0]["word"], "idf": records[0]["docs_count"]/len(records)}


def tf(records):
    word_counts = {}
    records = list(records)
    for record in records:
        word_counts[record["word"]] = word_counts.get(record["word"], 0) + 1
    for word, count in word_counts.items():
        yield {"word": word, "tf": count/len(records), "doc_id": record["doc_id"]}


def tf_with_sift(records):
    word_counts = {}
    for record in records:
        if len(record["word"]) > 4:
            word_counts[record["word"]] = word_counts.get(record["word"], 0) + 1
    total_count = 0
    for word in word_counts:
        if word_counts[word] > 1:
            total_count += word_counts[word]

    for word, count in word_counts.items():
        if count > 1:
            yield {"word": word, "tf": count/total_count, "doc_id": record["doc_id"],
                   "count_in_doc": count, "total_count_in_doc": total_count}


def culc_tf_idf(record):
    return {"text": record["word"], "doc_id": record["doc_id"], "tf_idf": record["tf"] * math.log(record["idf"])}


def invert_index(records):
    yield from heapq.nlargest(3, (culc_tf_idf(record) for record in records), key=itemgetter("tf_idf"))


def frequency_of_word_in_doc(records):
    word_count = 0
    records = list(records)
    for record in records:
        word_count += record["count"]
    yield {""}


def sum_words(state, record):
    if state is None:
        state = {"words_count": 0}
    state["words_count"] += record["count_in_doc"]
    return state


def calc_pmi(records):
    for record in records:
        yield {"doc_id": record["doc_id"], "text": record["word"],
               "pmi": math.log(record["tf"]*record["words_count"]/record["count"])}


def collect_words_pmi(records):
    count_sum = 0
    for record in records:
        count_sum += record["count_in_doc"]
    yield {'word': record['word'], "count": count_sum}


def get_top_10(records):
    yield from sorted(records, key=lambda x: x["pmi"], reverse=True)[:10]


def build_word_count_graph(input_stream, text_column='text', count_column='count'):
    return ComputeGraph(input_stream).map(emit_words).sort("word").reduce(collect_counts, "word").sort(count_column)


def build_inverted_index_graph(input_stream, doc_column='doc_id', text_column='text'):
    """For every pair (word, document) tf - idf is
    TFIDF(word_i, doc_i) = (frequency of word_i in doc_i) * log((total number of docs) / (docs where word_i is present))
    Result should look like {'term': 'word', 'index': [(doc_id_1, tf_idf_1)...]}"""
    split_word = ComputeGraph(input_stream).map(emit_words)
    count_docs = ComputeGraph(input_stream).fold(count_rows)
    count_idf = ComputeGraph(split_word).sort("doc_id", "word").reduce(unique, keys=("doc_id", "word"))\
        .join(count_docs, type='inner').sort('word').reduce(calc_idf, keys=('word'))
    calc_index = ComputeGraph(split_word).sort('doc_id').reduce(tf, keys='doc_id')\
        .join(count_idf, keys=('word', "doc_id"), type='left').sort('word').reduce(invert_index, keys='word')

    return calc_index


def build_pmi_graph(input_stream, doc_column='doc_id', text_column='text'):
    word_count_pre_doc = ComputeGraph(input_stream).map(emit_words).sort("doc_id").reduce(tf_with_sift, "doc_id")

    total_word_count = ComputeGraph(word_count_pre_doc).fold(sum_words)

    word_count = ComputeGraph(word_count_pre_doc).sort("word").reduce(collect_words_pmi, "word")

    calc_index = ComputeGraph(word_count_pre_doc).join(total_word_count)\
        .join(word_count, keys="word", type="left").sort(("word", "doc_id"))\
        .reduce(calc_pmi, keys=("word", "doc_id")).sort("doc_id").reduce(get_top_10, "doc_id")

    return calc_index


def build_yandex_maps_graph(input_stream, input_stream_length):
    lengths = ComputeGraph(input_stream_length).map(add_distance)
    speed = ComputeGraph(input_stream).map(add_weekday).map(add_hour).map(add_delta_time).join(lengths, keys="edge_id")\
        .sort("weekday", "hour").reduce(culc_speed, keys=("weekday", "hour"))
    return speed


def parse_date(date):
    return datetime.datetime.strptime(date, "%Y%m%dT%H%M%S.%f")


def get_day(datetime):
    return datetime.strftime('%a')


def date_range(start_date, end_date):
    start_date = parse_date(start_date)
    end_date = parse_date(end_date)
    for ordinal in range(start_date.toordinal(), end_date.toordinal()):
        yield datetime.datetime.fromordinal(ordinal + 1)
    yield end_date


def cucl_deltatime(start_time, end_time):
    delta_time = end_time - start_time
    return delta_time.total_seconds()


def add_weekday(record):
    enter_time = parse_date(record["enter_time"])
    leave_time = parse_date(record["leave_time"])
    delta_time = cucl_deltatime(enter_time, leave_time)
    for leave_time in date_range(record["enter_time"], record["leave_time"]):
        yield {**record, "enter_time": enter_time, "leave_time": leave_time,
               "weekday": get_day(enter_time), "part": cucl_deltatime(enter_time, leave_time) / delta_time}


def add_hour(record):
    enter_time = record["enter_time"]
    leave_time = record["leave_time"]
    delta_time = cucl_deltatime(enter_time, leave_time)
    middle_time = enter_time
    while middle_time < leave_time:
        middle_time += datetime.timedelta(hours=1)
        if middle_time > leave_time:
            middle_time = leave_time
        yield {**record, "enter_time": enter_time, "leave_time": middle_time, "hour": enter_time.hour,
               "part": record["part"] * cucl_deltatime(enter_time, middle_time) / delta_time}
        enter_time = middle_time


def add_delta_time(record):
    enter_time = record["enter_time"]
    leave_time = record["leave_time"]
    delta_time = cucl_deltatime(enter_time, leave_time) / 3600
    yield {**record, "time": delta_time}


def add_distance(record):
    yield {"edge_id": record["edge_id"], "distance": distance(record["start"], record["end"])}


def distance(origin, destination):
    """
    Calculate the Haversine distance.

    Parameters
    ----------
    origin : tuple of float
        (lat, long)
    destination : tuple of float
        (lat, long)

    Returns
    -------
    distance_in_km : float

    """
    lon1, lat1 = origin
    lon2, lat2 = destination
    radius = 6371
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (math.sin(dlat / 2) * math.sin(dlat / 2) +
         math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) *
         math.sin(dlon / 2) * math.sin(dlon / 2))
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    d = radius * c
    return d


def culc_speed(records):
    distance = 0
    time = 0
    for record in records:
        distance += record["distance"] * record["part"]
        time += record["time"] * record["part"]
    yield {"hour": record["hour"], "speed": distance/time, "weekday": record["weekday"]}
