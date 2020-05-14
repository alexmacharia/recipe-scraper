from json import loads, dumps
from kafka import KafkaConsumer, KafkaProducer
from bs4 import BeautifulSoup


def connect_kafka_consumer():
    '''Creates the kafka consumer connection'''
    try:
        _consumer = KafkaConsumer(
                         'allrecipe_data',
                         bootstrap_servers=['localhost:9092'],
                         auto_offset_reset='earliest',
                         enable_auto_commit=True,
                         group_id='recipe-group',
                         value_deserializer=lambda x: loads(x.decode('utf-8'))
            )
    except Exception as ex:
        print(str(ex))
    finally:
        return _consumer


def connect_kafka_producer():
    '''Creates the kafka producer connection'''
    try:
        _producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                                  value_serializer=lambda x:
                                  dumps(x).encode('utf-8'))
    except Exception as ex:
        print(str(ex))
    finally:
        return _producer


def publish_message(producer_instance, data):
    '''Publishes message to a kafka topic'''
    try:
        producer_instance.send('parsed_recipes', value=data)
        producer_instance.flush()
        print('Message published successfully')
    except Exception as ex:
        print(str(ex))


def parse_page(data):
    '''Parses  the pages consumed from the raw recipe pages'''
    html = [i for i in data.values()][0]
    title = [j for j in data][0]

    soup = BeautifulSoup(html, 'lxml')

    recipe = dict()

    print(title)
    print(soup)

    steps_div = soup.find('div', attrs={'class': 'directions--section__steps'})
    steps_list = steps_div.findAll('li', attrs={'class': 'step'})

    steps = []

    for step in steps_list:
        steps.append(step.find('span').text)
    recipe['steps'] = steps

    ingr_list = None
    ingr_ul = soup.findAll('ul', attrs={'class': 'checklist'})
    for item in ingr_ul:
        ingr = item.findAll('li', attrs={'class': 'checkList__line'})
        if not ingr_list:
            ingr_list = ingr
        else:
            ingr_list.extend(ingr)
    ingredients = [item.find('span').text for item in ingr_list]
    recipe['ingredients'] = ingredients

    submitter_desc = soup.find('div', attrs={'class': 'submitter_description'})
    recipe['description'] = submitter_desc

    calories = soup.find('span', attrs={'class': 'calorie-count'}).text
    recipe['calories'] = calories

    reviews = soup.find('span', attrs={'class': 'review-count'}).text
    recipe['reviews'] = reviews

    urls_list = soup.findAll('a', attrs={'class': 'video-play'})

    img_url = []
    vid_url = []

    for item in urls_list:
        vid_link = item['href']
        img_link = item.find('img')['src']
        vid_url.append(vid_link)
        img_url.append(img_link)
    recipe['video_url'] = vid_url
    recipe['image_url'] = img_url

    return recipe


if __name__ == '__main__':
    consumer = connect_kafka_consumer()

    for message in consumer:
        message = message.value  # Decode message
        parsed = parse_page(message)  # Call parse_page() return json obj
        print(parsed)
        # Publish message to parsed_recipe topic
