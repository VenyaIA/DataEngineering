import json
from operation import calculate_stats, calculate_frequency
from bs4 import BeautifulSoup


def handle_file(path):
    with open(path, 'r', encoding='utf-8') as file:
        xml_content = file.read()

    star = BeautifulSoup(xml_content, 'xml').star

    item = {}
    for el in star:
        if el.name is None:
            continue
        item[el.name] = el.get_text().strip()

    item['radius'] = int(item['radius'])

    return item


items = []
for i in range(1, 119):
    items.append(handle_file(f'70/3/{i}.xml'))

items.sort(key=lambda x: x['name'])
with open('third_results/third_sort.json', 'w', encoding='utf-8') as f:
    json.dump(items, f, ensure_ascii=False, indent=4)

filtered_items = list(filter(lambda x: x['radius'] > 350_000_000, items))
with open('third_results/third_filtered.json', 'w', encoding='utf-8') as f:
    json.dump(filtered_items, f, ensure_ascii=False, indent=4)

radius_stat = calculate_stats('radius', items)
with open('third_results/third_stat.json', 'w', encoding='utf-8') as f:
    json.dump(radius_stat, f, ensure_ascii=False, indent=4)

constellation_stat = calculate_frequency('constellation', items)
with open('third_results/third_frequency.json', 'w', encoding='utf-8') as f:
    json.dump(constellation_stat, f, ensure_ascii=False, indent=4)

