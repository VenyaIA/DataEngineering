import json
from operation import calculate_stats, calculate_frequency
from bs4 import BeautifulSoup

# данные взяты с сайта: https://x-catalog.ru/krasnoufimsk/krasnoufimskoe-rayonnoe-potrebitelyskoe-obschestvo-krasnoufimsk/


def handle_file(path):
    with open(path, 'r', encoding='utf-8') as file:
        html_content = file.read()

    soup = BeautifulSoup(html_content, 'html.parser')
    upholstered_furniture = soup.find_all('div', attrs={'class': 'rec__item item itemfor3'})
    items = []

    for furniture in upholstered_furniture:
        item = {}
        item['id'] = int(furniture.find('div', attrs={'class': "book__wrapper"})['data-idtov'])
        item['name'] = ' '.join(furniture.find('div', attrs={'itemprop': "name"}).get_text().strip().split())
        item['current_price'] = float(furniture.find('meta', attrs={'itemprop': "price"})['content'])
        item['previous_price'] = float(furniture.find('p', attrs={'class': "rec__item--price--prev"})
                                       .get_text().strip().replace(" руб", ""))
        item['url'] = furniture.find('a', attrs={'itemprop': "url"})['href']
        item['image_url'] = furniture.find('img', attrs={'itemprop': "image"})['src']
        item['availability'] = furniture.find(itemprop="availability")['href'].split('/')[-1]
        item['availability'] = furniture.find('link', attrs={'itemprop': "availability"})['href'].split('/')[-1]

        items.append(item)

    return items


items = []
for i in range(1, 14):
    items.extend(handle_file(f'70/data_for_fifth_several_objects/{i}.html'))

items.sort(key=lambda x: x['id'])
with open('fifth_results_several_objects/fifth_sort.json', 'w', encoding='utf-8') as f:
    json.dump(items, f, ensure_ascii=False, indent=4)

filtered_items = list(filter(lambda x: x['current_price'] > 30000, items))
with open('fifth_results_several_objects/fifth_filtered.json', 'w', encoding='utf-8') as f:
    json.dump(filtered_items, f, ensure_ascii=False, indent=4)

price_stat = calculate_stats('current_price', items)
with open('fifth_results_several_objects/fifth_stat.json', 'w', encoding='utf-8') as f:
    json.dump(price_stat, f, ensure_ascii=False, indent=4)

material_stat = calculate_frequency('name', items)
with open('fifth_results_several_objects/fifth_frequency.json', 'w', encoding='utf-8') as f:
    json.dump(material_stat, f, ensure_ascii=False, indent=4)