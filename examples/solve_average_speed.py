import json
from compgraph import algorithms
from .solve_word_count import file_generator


def main():
    word_count_solver = algorithms.build_yandex_maps_graph("times", "length")
    with open("../resource/results/average_speeds.txt", "w") as result_file:
        result_file.writelines(json.dumps(result_line)+"\n" for result_line in
                               word_count_solver.run(times=file_generator("../resource/travel_times.txt"),
                                                     length=file_generator("../resource/graph_data.txt")))


if __name__ == "__main__":
    main()
