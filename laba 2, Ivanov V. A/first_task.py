import json
import numpy as np

matrix = np.load('70/first_task.npy')

size = matrix.size

matrix_props = {
    'sum': 0,
    'avr': 0,
    'sumMD': 0,
    'avrMD': 0,
    'sumSD': 0,
    'avrSD': 0,
    'max': matrix[0][0],
    'min': matrix[0][0]
}

for i in range(matrix.shape[0]):
    for j in range(matrix.shape[1]):
        el = matrix[i][j]
        matrix_props['sum'] += el
        if i == j:
            matrix_props['sumMD'] += el
        if j == matrix.shape[1] - i - 1:
            matrix_props['sumSD'] += el

        if matrix_props['max'] < el:
            matrix_props['max'] = el
        if matrix_props['min'] > el:
            matrix_props['min'] = el

matrix_props['avr'] = matrix_props['sum'] / size
matrix_props['avrMD'] = matrix_props['sumMD'] / matrix.shape[0]
matrix_props['avrSD'] = matrix_props['sumSD'] / matrix.shape[0]

for key in matrix_props.keys():
    matrix_props[key] = float(matrix_props[key])

with open('first_task.json', 'w', encoding='utf-8') as f:
    json.dump(matrix_props, f)


norm_matrix = matrix / matrix_props['sum']
np.save('first_task.npy', norm_matrix)
