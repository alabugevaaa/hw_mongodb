# coding: utf-8
import csv
import re
from datetime import datetime
from pymongo import MongoClient


def read_data(csv_file, db):
    """
    Загрузить данные в бд из CSV-файла
    """
    with open(csv_file, encoding='utf-8-sig') as csvfile:
        # прочитать файл с данными и записать в коллекцию
        reader = csv.DictReader(csvfile, delimiter=';')
        tickets = []
        for concert in reader:
            date = datetime.strptime(concert['date'], '%d.%m.%Y')
            ticket = {'artist': concert['artist'], 'name': concert['name'], 'date': date,
                      'price': int(concert['price'])}
            tickets.append(ticket)
        result = db.tickets.insert_many(tickets)
        return result


def find_cheapest(db):
    """
    Отсортировать билеты из базы по возрастанию цены
    Документация: https://docs.mongodb.com/manual/reference/method/cursor.sort/
    """
    return db.tickets.find().sort('price')


def find_by_name(name, db):
    """
    Найти билеты по имени исполнителя (в том числе – по подстроке, например "Seconds to"),
    и вернуть их по возрастанию цены
    """

    regex = re.compile(r'%s' % name, re.IGNORECASE)
    return db.tickets.find({'artist': regex}).sort('price')


def find_nearest(db):
    """
    Отсортировать билеты из базы по дате мероприятия
    """
    return db.tickets.find().sort('date')


if __name__ == '__main__':
    client = MongoClient()
    db = client['concert_tickets']
    tickets_collection = db['tickets']

    tickets = read_data('concerts.csv', db)

    print('По возрастанию цены:')
    for t in find_cheapest(db):
        print(t)

    print('Поиск по исполнителю:')
    for t in find_by_name('Иглесиас', db):
        print(t)

    print('По дате мероприятия:')
    for t in find_nearest(db):
        print(t)

