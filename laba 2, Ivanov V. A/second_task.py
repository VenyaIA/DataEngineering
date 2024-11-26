import os.path

import numpy as np

matrix = np.load('70/second_task.npy')

x = []
y = []
z = []

for i in range(matrix.shape[0]):
    for j in range(matrix.shape[1]):
        if matrix[i][j] > 570:
            x.append(i)
            y.append(j)
            x.append(matrix[i][j])


np.savez('second_task.npz', x=x, y=y, z=z)
np.savez_compressed('second_task_compress.npz', x=x, y=y, z=z)

first_size = os.path.getsize('second_task.npz')
second_size = os.path.getsize('second_task_compress.npz')

print(f"savez = {first_size}")
print(f"savez_compressed = {second_size}")
print(f"diff = {first_size - second_size}")
