import json
import random
import time

def generate_data(counter):
    data = {
        "id": counter,
        "data1": random.choice(["A", "B", "C"]),
        "data2": random.choice(["X", "Y", "Z"]),
        "numeric_data": random.uniform(0, 1),
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
    }
    return data


# Функція для запису даних у файл JSON
def write_data_to_json_file(data, filename):
    with open(filename, "a") as file:
        json.dump(data, file)
        file.write("\n")


# Генерація даних та запис у файли
counter = 1
while True:
    data = generate_data(counter)
    counter += 1
    write_data_to_json_file(data, "stream2/data_stream2.json")
    time.sleep(1)  # Затримка для імітації потоку даних
