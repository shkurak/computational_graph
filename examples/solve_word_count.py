import json
from compgraph import algorithms


def file_generator(file_name):
    with open(file_name, "r") as file:
        for line in file:
            yield json.loads(line, encoding="utf-8")


def main():
    word_count_solver = algorithms.build_word_count_graph("text_corpus")
    with open("../resource/results/word_count.txt", "w") as result_file:
        result_file.writelines(json.dumps(result_line)+"\n" for result_line in
                               word_count_solver.run(text_corpus=file_generator("../resource/text_corpus.txt")))


if __name__ == "__main__":
    main()
