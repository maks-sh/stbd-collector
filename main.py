# -*- coding: utf-8 -*-
import urllib.request
import pandas as pd
import re
import gc
import os
import dateutil.relativedelta
from datetime import datetime
from progress_bar import print_progress
from datetime import date


class Table(object):
    def __init__(self, month, year, id, cols, names, bankir=True):
        self.month = month
        self.year = year
        '''
        Список всех id:
            banki.ru:
                капитал:
                    На banki.ru капитал представлен в двух id - 25 и 20.
                    25 доступен начиная с 01.09.2014
                    20 доступен до 31.12.2014
                    Поэтому при создании объекта - если id==0, то это капитал.
                расчетные счета: 410
                средства ПиО: 500
                Вклады ф/л: 60

            bankir.ru:
                активы: 19
                ценные бумаги: 27
                обороты средств в банкоматах: 34
                кредиты предприятиям: 21
                потребительские кредиты: 24
        '''
        if id == 0 and (prev >= date(2014, 9, 1)):
            self.id = 25
        elif id == 0 and d >= date(2014, 9, 1):
            self.id = id # осталвяем id=0 для случая когда дата запрашиваемого периода больше 01/09/2014, а предыдущая дата prev - меньше
        elif id == 0:
            self.id = 20
        else:
            self.id = id

        self.cols = cols
        self.bankir = bankir
        self.names = names

        def get_url(self):
            if self.bankir:
                url = u'http://bankir.ru/rating/export/csv/' + str(self.month) + '/' + str(self.year) + '/' + str(self.id)
            else:
                url = u'http://www.banki.ru/banks/ratings/export.php?PROPERTY_ID=' + str(self.id) + '&date1=' + str(self.year) + '-' + str(self.month) + '-01&date2=' +  str(year_prev) + '-' + str(month_prev)+ '-01'
            return url

        self.url = get_url(self)

        def get_data(self):
            if self.bankir:
                data = pd.read_csv(urllib.request.urlopen(self.url), ";", skiprows=0, header=0, encoding='utf-8', quotechar="'", usecols=self.cols, names=('Лиц', 'Банк', 'city', 'a'))
                data.a /= 1000 # переводим из тысяч в миллионы
                data.city = data.city.apply(lambda row: row.capitalize())
            else:
                if self.id == 0:
                    self.url = u'http://www.banki.ru/banks/ratings/export.php?PROPERTY_ID=25&date1=' + str(
                        self.year) + '-' + str(self.month) + '-01&date2=' + str(year_prev) + '-' + str(
                        month_prev) + '-01'
                    url_for_prev = u'http://www.banki.ru/banks/ratings/export.php?PROPERTY_ID=20&date1=' + str(
                        year_prev) + '-' + str(month_prev) + '-01&date2=2008-03-01'
                    print(url_for_prev)
                    data = pd.read_csv(urllib.request.urlopen(self.url), ";", names=('Лиц', 'a'), skiprows=4, usecols=[3, 5], thousands=' ', decimal=',', na_values='-', encoding='windows-1251')
                    data_for_prev = pd.read_csv(urllib.request.urlopen(url_for_prev), ";", names=('Лиц', 'b'), skiprows=4, usecols=[3, 5], thousands=' ', decimal=',', na_values='-', encoding='windows-1251')
                    data = data.merge(data_for_prev, how='outer', on='Лиц')
                    print(data)
                else:
                    data = pd.read_csv(urllib.request.urlopen(self.url), ";", names=('Лиц', 'a', 'b'), skiprows=4, usecols=self.cols, thousands=' ', decimal=',', na_values='-', encoding='windows-1251')
                data.a /= 1000 # переводим из тысяч в миллионы
                data.b /= 1000 # переводим из тысяч в миллионы
            data.columns = self.names
            return data

        self.data = get_data(self)

    @property
    def getData(self):
        return self.data

    @property
    def getBankir(self):
        return self.bankir

    def __repr__(self):
        return str(self)

    @staticmethod
    def parser():

        def chunks(l, n):
            list = []
            for i in range(0, len(l), n):
                list.append(l[i:i+n])
            return list

        url = 'http://www.banki.ru/banks/memory/?PAGEN_1='
        site = urllib.request.urlopen(url)
        html = site.read().decode('windows-1251')
        n_pages = round(int(re.findall(r'totalItems:(.*?);', str(html), re.S)[0]) / 50.0 + 0.48) + 1
        table = re.findall(r'<table class="standard-table standard-table--row-highlight">(.*?)</table>', str(html), re.S)
        thead = re.findall(r'<th>(.*?)(?: <nobr>.*?</nobr>)?</th>', str(table))
        i = 0
        for th in thead:
            thead[i] = th.replace(' ', '_')
            i += 1
        trs = re.findall(r'<td[^>]*>(?:<strong><a[^>]*>)?(.*?)(?:</a></strong>)?</td>', str(table))
        print_progress(0, n_pages, prefix='Формирую файл с отозванными банками:', suffix='Выполнено', barLength=40)
        for i in range(2, n_pages):
            cur_url = url + str(i)
            site = urllib.request.urlopen(cur_url)
            html = site.read().decode('windows-1251')
            table = re.findall(r'<table class="standard-table standard-table--row-highlight">(.*?)</table>', str(html), re.S)
            trs.extend(re.findall(r'<td[^>]*>(?:<strong><a[^>]*>)?(.*?)(?:</a></strong>)?</td>', str(table)))
            print_progress(i, n_pages, prefix='Формирую файл с отозванными банками:', suffix='Выполнено', barLength=40)
        defunct = pd.DataFrame(data=chunks(trs, len(thead)), columns=thead)
        return defunct

    @staticmethod
    def my_merge():
        skip_banki = 0
        skip_bankir = 0
        banki = pd.DataFrame()
        bankir = pd.DataFrame()
        for obj in gc.get_objects():
            if isinstance(obj, Table):
                if obj.bankir:
                    if skip_bankir == 0:
                        bankir = obj.getData
                        skip_bankir = 1
                    else:
                        bankir = bankir.merge(obj.getData, how='outer', on=['Лиц', 'Банк', 'Город'])
                else:
                    if skip_banki == 0:
                        banki = obj.getData
                        skip_banki = 1
                    else:
                        banki = banki.merge(obj.getData, how='outer', on='Лиц')
        df = bankir.merge(banki, how='outer', on='Лиц')
        return df

if __name__ == '__main__':
    month = 11
    year = 2015
    period = 1
    d = date(year, month, 1)
    prev = d - dateutil.relativedelta.relativedelta(months=period)
    next = d + dateutil.relativedelta.relativedelta(months=period)
    print(prev)
    print(next)

    month_prev = prev.month
    year_prev = prev.year
    month_next = next.month
    year_next = next.year
    filename = str(month) + '.' + str(year) + '.period=' + str(period) +'.xlsx'

    assets_prev = Table(month_prev, year_prev, 19, (1, 2, 3, 4), ('Лиц', 'Банк', 'Город', 'Активы_пред_(млн_руб)'))
    assets = Table(month, year, 19, (1, 2, 3, 4), ('Лиц', 'Банк', 'Город', 'Активы_(млн_руб)'))
    business = Table(month, year, 21, (1, 2, 3, 5), ('Лиц', 'Банк', 'Город', 'Кредиты_преприятиям_(млн_руб)'))
    consumers = Table(month, year, 24, (1, 2, 3, 5), ('Лиц', 'Банк', 'Город', 'Потребительские_кредиты_(млн_руб)'))
    atm = Table(month, year, 34, (1, 2, 3, 5), ('Лиц', 'Банк', 'Город', 'Оборот_средств_в_банкоматах_(млн_руб)'))
    accounts = Table(month, year, 410, (3, 5, 7), ('Лиц', 'Р/С_ф/л_(млн_руб)', 'Изменение_Р/С_ф/л_(млн_руб)'), False)
    money = Table(month, year, 500, (3, 5, 7), ('Лиц', 'Средства_ПиО_(млн_руб)', 'Изменение_Средства_ПиО_(млн_руб)'), False)
    deposits = Table(month, year, 60, (3, 5, 7), ('Лиц', 'Вклады_ф/л_(млн_руб)', 'Изменение_вклады_ф/л_(млн_руб)'), False)
    securities = Table(month, year, 70, (3, 5, 7), ('Лиц', 'Ценные_бумаги_(млн_руб)', 'Изменение_ценные_бумаги_(млн_руб)'), False)
    capital = Table(month, year, 0, (3, 5, 7), ('Лиц', 'Капитал_(млн_руб)', 'Изменение_капитал_(млн_руб)'), False)

    main = Table.my_merge()

    # print(main)
    # print(assets_prev.data.dropna(axis=0, how='any').shape)
    # print(assets.data.dropna(axis=0, how='any').shape)
    # print(business.data.dropna(axis=0, how='any').shape)
    # print(consumers.data.dropna(axis=0, how='any').shape)
    # print(atm.data.dropna(axis=0, how='any').shape)
    # print(accounts.data.dropna(axis=0, how='any').shape)
    # print(money.data.dropna(axis=0, how='any').shape)
    # print(deposits.data.dropna(axis=0, how='any').shape)
    # print(securities.data.dropna(axis=0, how='any').shape)
    # print(capital.data.dropna(axis=0, how='any').shape)
    # print(capital.url)

    ########################### произвожу определенную работу со столбцами
    main['Изменение_активы_(млн_руб)'] = main['Активы_(млн_руб)'] - main['Активы_пред_(млн_руб)']
    main = main.drop(labels='Активы_пред_(млн_руб)', axis=1)
    main['Обороты_средств_в_банкоматах/Капитал'] = main['Оборот_средств_в_банкоматах_(млн_руб)']/main['Капитал_(млн_руб)']
    main = main.drop(labels='Оборот_средств_в_банкоматах_(млн_руб)', axis=1)
    main['Кредиты_преприятиям/Капитал'] = main['Кредиты_преприятиям_(млн_руб)'] / main['Капитал_(млн_руб)']
    main = main.drop(labels='Кредиты_преприятиям_(млн_руб)', axis=1)
    main['Потребительские_кредиты/Капитал'] = main['Потребительские_кредиты_(млн_руб)'] / main['Капитал_(млн_руб)']
    main = main.drop(labels='Потребительские_кредиты_(млн_руб)', axis=1)
    main['Ценные_бумаги/Капитал'] = main['Ценные_бумаги_(млн_руб)'] / main['Капитал_(млн_руб)']
    main = main.drop(labels='Ценные_бумаги_(млн_руб)', axis=1)

    ########################## добавляю столбец отзыв, для этого собираю данные с сайта banki.ru

        ##### для того, чтобы каждый раз не парсить сайт, проверяю как давно был изменен файл: если более 7 дней, то парсим
    if os.path.exists('withdraw.csv') and \
                    datetime.fromtimestamp(os.path.getmtime('withdraw.csv')-datetime.today().timestamp()).day < 7:
        defunct = pd.read_csv('withdraw.csv')
    else:
        defunct = Table.parser()
        defunct.to_csv('withdraw.csv')

    defunct[['дата_отзыва']] = defunct[['дата_отзыва']].apply(pd.to_datetime)
    defunct = defunct[(defunct.причина == 'отозв.') & (defunct.дата_отзыва <= pd.datetime(year_next, month_next, 1)) & (defunct.дата_отзыва >= pd.datetime(year, month, 1))]
    defunct = defunct[['номер_лицензии', 'причина']]
    defunct.columns = ['Лиц', 'Отзыв']
    defunct['Лиц'] = defunct[['Лиц']].apply(lambda row: int(re.sub(r'[^\d]', '', str(row.Лиц))), axis=1)
    defunct['Лиц'] = defunct['Лиц'].astype(float)
    defunct['Отзыв'] = 1

    ########################### после добавления столбца "отзыв" делаю
    def handle(row):
        if row.Отзыв == 1:
            row.fillna(value=0, inplace=True)
        return row

    main = defunct.merge(main, how='outer', on='Лиц')
    main.dropna(axis=0, how='any', subset=['Банк', 'Город'], inplace=True)
    main.Отзыв = main.Отзыв.fillna(0)
    main = main.apply(lambda row: handle(row), axis=1)
    main.dropna(axis=0, how='any', inplace=True)

    ########################### заменяю на правильные имена столбцов
    final_names = []
    for name in list(main.columns):
        final_names.append(re.sub('_', ' ', name))
    main.columns = final_names

    main.sort_values(by='Активы (млн руб)', ascending=False, axis=0, inplace=True, kind='quicksort')
    main.to_excel(filename, index=False)
    # print(main)
    print(main.shape)
