from time import sleep
from json import dumps
import requests
import numpy as np
from bs4 import BeautifulSoup
from kafka import KafkaProducer


def fetch_url(url, headers):
    try:
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            html = response.text
            return html
        else:
            print(response.status_code)
    except Exception as ex:
        print(str(ex))


def sleep_random():
    options = [2, 5, 10, 13, 1, 15, 7, 4]
    sleep(np.random.choice(options))


def parse_url(html, headers, producer_instance):
    soup = BeautifulSoup(html, 'lxml')
    url_list = soup.findAll('a', attrs={'class':
                                        'fixed-recipe-card__title-link'})
    for item in url_list:
        recipe_url = item['href']
        recipe_title = item.find('span').text
        print('{}     {}'.format(recipe_title, recipe_url))
        recipe_html = fetch_url(recipe_url, headers)
        data = {recipe_title: recipe_html}
        publish_message(producer_instance, data)
        sleep_random()


def connect_kafka_producer():
    try:
        _producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                                  value_serializer=lambda x:
                                  dumps(x).encode('utf-8'))
    except Exception as ex:
        print(str(ex))
    finally:
        return _producer


def publish_message(producer_instance, data):
    try:
        producer_instance.send('allrecipe_data', value=data)
        producer_instance.flush()
        print('Message published successfully')
    except Exception as ex:
        print(str(ex))


if __name__ == '__main__':

    url_template = "https://www.allrecipes.com/recipes/88/bbq-grilling/?page="
    headers = {'User-Agent': '''Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.181 Safari 537.36''',
                             'Pragma': 'no-cache'
               }
    for i in range(30, 40):
        url = url_template + str(i)
        print(url)
        html = fetch_url(url, headers)
        producer_instance = connect_kafka_producer()
        parse_url(html, headers, producer_instance)
