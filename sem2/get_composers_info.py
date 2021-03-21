''' Gets info about composers from website and prints it in  JSON format ''' 

import requests
from lxml import html
import json

ADDRESS = 'https://www.classic-music.ru'

resp = requests.get(ADDRESS + '/comp.html')
tree = html.fromstring(resp.text)

composers = tree.xpath('//ul/li')[6:] # skip menu lists
composers_links = [c.xpath('.//a/@href')[0] for c in composers]
composers_pages = [requests.get(ADDRESS + link).text for link in composers_links]

composers_trees = [html.fromstring(p) for p in composers_pages]

composers_full = [{
    'full_name': t.xpath('//h1/text()')[0],
    'birth_date': t.xpath('//tr/td/p/text()')[0],
    'death_date': t.xpath('//tr/td/p/text()')[1],
    'country': t.xpath('//tr/td/p/text()')[-2],
    'description': t.xpath('//div[@id="page_main_content"]/p/text()')[0],
} for t in composers_trees]


print(json.dumps(composers_full, indent=4, ensure_ascii=False))
