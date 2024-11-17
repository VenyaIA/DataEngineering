def read_file():
    with open("data/first_task.txt", encoding="utf_8") as file:
        return file.readlines()


def text_to_words(lines):
    words = []
    sentence_count = []
    for line in lines:
        result_line = (line
                       .replace("'", "")
                       .replace("?", "")
                       .replace("!", "")
                       .replace(".", "")
                       .replace(",", "")
                       .replace("-", " ")
                       .lower().strip())
        words += result_line.split(" ")

        arr = ".!?"
        count = 0
        for ch in line:
            if ch in arr:
                count += 1
        sentence_count.append(count)

    avg_sentence = sum(sentence_count) / len(sentence_count)
    return words, avg_sentence


def calc_freq(words):
    word_freq = {}
    for word in words:
        if word in word_freq:
            word_freq[word] += 1
        else:
            word_freq[word] = 1

    return sorted(word_freq.items(), key=lambda x: x[1], reverse=True)


def write_to_file(stat):
    with open("first_task_result1.txt", "w", encoding="utf_8") as file:
        for key, val in stat:
            file.write(f'{key}:{val}\n')


def write_avg_to_file(res):
    with open("first_task_result2.txt", "w", encoding="utf_8") as file:
        file.write(f'{res}')


lines = read_file()
words, avg_sentence = text_to_words(lines)
word_freq = calc_freq(words)
write_to_file(word_freq)
write_avg_to_file(avg_sentence)