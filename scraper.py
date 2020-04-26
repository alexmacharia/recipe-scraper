import requests
from bs4 import BeautifulSoup
import json
from time import sleep
import numpy as np


def fetch_url(url, headers):
    try:
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            html = response.text
            return html
    except Exception as e:
        print(str(e))


def fetch_recipe():
    pass


def sleep_random():
    options = [2, 5, 10, 13, 1, 15, 7, 4]
    sleep(np.random.choice(options))


def parse_url(html, headers):
    soup = BeautifulSoup(html, 'lxml')
    url_list = soup.findAll('a', attrs={'class':
                                        'fixed-recipe-card__title-link'})
    for item in url_list:
        recipe_url = item['href']
        recipe_title = item.find('span').text
        print('{}     {}'.format(recipe_title, recipe_url))
        recipe_html = fetch_url(recipe_url, headers)
        sleep_random()


if __name__ == '__main__':
    url = "https://www.allrecipes.com/recipes/88/bbq-grilling/"
    headers = {'user-agent': '''Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6)
        AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87
               Safari/537.36''', 'Pragma': 'no-cache'}

    html = fetch_url(url, headers)
    parse_url(html, headers)
