def read_file():
    with open("data/second_task.txt", encoding="utf_8") as file:
        lines = file.readlines()
        table = []
        for line in lines:
            words = line.strip().split()
            table.append(list(map(int, words)))

        return table


def get_list_sum_and_avg(table):
    result = []
    for row in table:
        line_abs = map(abs, filter(lambda x: x**2 < 100_000, row))
        sum_line = sum(line_abs)
        result.append(sum_line)
    avg_column = sum(result) / len(result)
    return result, avg_column


def write_to_file(column, avg_sum):
    with open("second_task_result.txt", "w", encoding="utf_8") as file:
        for num in column:
            file.write(f'{num}\n')
        file.write(f'\n{avg_sum}')


table = read_file()
list_sum, avg_column = get_list_sum_and_avg(table)
write_to_file(list_sum, avg_column)
