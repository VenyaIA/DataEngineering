import json
from operation import calculate_stats, calculate_frequency
from bs4 import BeautifulSoup


def handle_file(path):
    with open(path, 'r', encoding='utf-8') as file:
        html_content = file.read()

    soup = BeautifulSoup(html_content, 'html.parser')
    products = soup.find_all('div', attrs={'class': 'product-item'})
    items = []

    for product in products:
        item = {}
        item['id'] = int(product.a['data-id'])
        item['link'] = product.find_all('a')[1]['href']
        item['img'] = product.img['src']
        item['title'] = product.span.get_text().strip()
        item['price'] = float(product.price.get_text().replace('₽', '').replace(' ', '').strip())
        item['bonus'] = int(product.strong.get_text()
                            .replace('+ начислим', '')
                            .replace('бонусов', '')
                            )
        properties = product.ul.find_all('li')
        for prop in properties:
            item[prop['type']] = prop.get_text().strip()

        items.append(item)

    return items


items = []
for i in range(1, 44):
    items.extend(handle_file(f'70/2/{i}.html'))

items.sort(key=lambda x: x['id'])
with open('second_results/second_sort.json', 'w', encoding='utf-8') as f:
    json.dump(items, f, ensure_ascii=False, indent=4)

filtered_items = list(filter(lambda x: x['price'] > 350000, items))
with open('second_results/second_filtered.json', 'w', encoding='utf-8') as f:
    json.dump(filtered_items, f, ensure_ascii=False, indent=4)

bonus_stat = calculate_stats('bonus', items)
with open('second_results/second_stat.json', 'w', encoding='utf-8') as f:
    json.dump(bonus_stat, f, ensure_ascii=False, indent=4)

matrix_stat = calculate_frequency('matrix', items)
with open('second_results/second_frequency.json', 'w', encoding='utf-8') as f:
    json.dump(matrix_stat, f, ensure_ascii=False, indent=4)
