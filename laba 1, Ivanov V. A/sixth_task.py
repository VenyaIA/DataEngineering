import requests
from bs4 import BeautifulSoup

url = "https://api.spacexdata.com/v3/landpads"

json_data = requests.get(url).json()
print(json_data)

soup = BeautifulSoup("", "html.parser")

items_list = soup.new_tag('ul', id='items')

for item in json_data:
    print(item)
    for key_item in item.keys():
        if key_item == 'location':
            ul = soup.new_tag('ul')
            for key, value in item['location'].items():
                li = soup.new_tag('li')
                li.string = f"{key}: {value}"
                ul.append(li)
            items_list.append(ul)
        else:
            li = soup.new_tag('li')
            li.string = f"{key_item}: {item[key_item]}"
            items_list.append(li)
    br = soup.new_tag('br')
    items_list.append(br)

with open('sixth_task.html', 'w', encoding='utf-8') as f:
    f.write(items_list.prettify())
