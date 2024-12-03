import json
from operation import calculate_stats, calculate_frequency
from bs4 import BeautifulSoup


def handle_file(path):
    with open(path, 'r', encoding='utf-8') as file:
        xml_content = file.read()

    clothings = BeautifulSoup(xml_content, 'xml').find_all('clothing')
    items = []

    for clothing in clothings:
        item = {}
        item['id'] = int(clothing.id.get_text().strip())
        item['name'] = clothing.find_all('name')[0].get_text().strip()
        item['category'] = clothing.category.get_text().strip()
        item['size'] = clothing.size.get_text().strip()
        item['color'] = clothing.color.get_text().strip()
        item['material'] = clothing.material.get_text().strip()
        item['price'] = float(clothing.price.get_text().strip())
        item['rating'] = float(clothing.rating.get_text().strip())
        item['reviews'] = int(clothing.reviews.get_text().strip())
        if clothing.sporty is not None:
            item['sporty'] = clothing.sporty.get_text().strip() == 'yes'
        if clothing.new is not None:
            item['new'] = clothing.new.get_text().strip() == '+'
        if clothing.exclusive is not None:
            item['exclusive'] = clothing.exclusive.get_text().strip() == 'yes'
        items.append(item)

    return items


items = []
for i in range(1, 119):
    items.extend(handle_file(f'70/4/{i}.xml'))

items.sort(key=lambda x: x['id'])
with open('fourth_results/fourth_sort.json', 'w', encoding='utf-8') as f:
    json.dump(items, f, ensure_ascii=False, indent=4)

filtered_items = list(filter(lambda x: x['price'] > 400_000, items))
with open('fourth_results/fourth_filtered.json', 'w', encoding='utf-8') as f:
    json.dump(filtered_items, f, ensure_ascii=False, indent=4)

price_stat = calculate_stats('reviews', items)
with open('fourth_results/fourth_stat.json', 'w', encoding='utf-8') as f:
    json.dump(price_stat, f, ensure_ascii=False, indent=4)

material_stat = calculate_frequency('material', items)
with open('fourth_results/fourth_frequency.json', 'w', encoding='utf-8') as f:
    json.dump(material_stat, f, ensure_ascii=False, indent=4)