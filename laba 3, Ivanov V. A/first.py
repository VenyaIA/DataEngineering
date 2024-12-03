import json
from operation import calculate_stats, calculate_frequency
from bs4 import BeautifulSoup


def handle_file(path):
    with open(path, 'r', encoding='utf-8') as file:
        html_content = file.read()

    soup = BeautifulSoup(html_content, 'html.parser')

    book = soup.find_all('div', attrs={'class': 'book-wrapper'})[0]
    item = {}
    item['category'] = book.find_all("span")[0].get_text().split(':')[1].strip()
    item['title'] = book.h1.get_text().strip()
    item['author'] = book.p.get_text().strip()
    item['pages'] = int(book.find_all('span', attrs={'class': 'pages'})[0].get_text().split()[1].strip())
    item['year'] = int(book.find_all('span', attrs={'class': 'year'})[0].get_text().split('Издано в')[1])
    spans = book.find_all('span', attrs={'class': ''})
    item['ISBN'] = spans[1].get_text().split(':')[1].strip()
    item['rating'] = float(spans[2].get_text().split(':')[1])
    item['views'] = int(spans[3].get_text().split(':')[1])
    item['description'] = book.find_all('p')[1].get_text().split('Описание')[1].strip()
    item['img'] = book.img['src']

    return item


items = []
for i in range(2, 53):
    items.append(handle_file(f'70/1/{i}.html'))

items.sort(key=lambda x: x['pages'])
with open('first_results/first_sort.json', 'w', encoding='utf-8') as f:
    json.dump(items, f, ensure_ascii=False, indent=4)

filtered_items = list(filter(lambda x: x['year'] > 1950, items))
with open('first_results/first_filtered.json', 'w', encoding='utf-8') as f:
    json.dump(filtered_items, f, ensure_ascii=False, indent=4)

pages_stat = calculate_stats('pages', items)
with open('first_results/first_stat.json', 'w', encoding='utf-8') as f:
    json.dump(pages_stat, f, ensure_ascii=False, indent=4)

category_stat = calculate_frequency('category', items)
with open('first_results/first_frequency.json', 'w', encoding='utf-8') as f:
    json.dump(category_stat, f, ensure_ascii=False, indent=4)
