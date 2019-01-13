import json
from compgraph import algorithms
from .solve_word_count import file_generator


def main():
    word_count_solver = algorithms.build_inverted_index_graph("text_corpus")
    with open("../resource/results/invert_index.txt", "w") as result_file:
        result_file.writelines(json.dumps(result_line)+"\n" for result_line in
                               word_count_solver.run(text_corpus=file_generator("../resource/text_corpus.txt")))


if __name__ == "__main__":
    main()
