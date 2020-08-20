from bs4 import BeautifulSoup
import urllib.request
import json
import datetime
from dateutil.relativedelta import relativedelta
import csv
import os
import requests
import time

import pandas
pandas.set_option('display.max_rows', 50)
pandas.set_option('display.max_columns', 10)
pandas.set_option('display.width', 2000)

from nltk.tokenize import sent_tokenize
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

def get_past_date(str_days_ago):
    TODAY = datetime.date.today()
    splitted = str_days_ago.split()
    if len(splitted) == 1 and splitted[0].lower() == 'today':
        return str(TODAY.isoformat())
    elif len(splitted) == 1 and splitted[0].lower() == 'yesterday':
        date = TODAY - relativedelta(days=1)
        return str(date.isoformat())
    elif splitted[1].lower() in ['hour', 'hours', 'hr', 'hrs', 'h']:
        date = datetime.datetime.now() - relativedelta(hours=int(splitted[0]))
        return str(date.date().isoformat())
    elif splitted[1].lower() in ['day', 'days', 'd']:
        date = TODAY - relativedelta(days=int(splitted[0]))
        return str(date.isoformat())
    elif splitted[1].lower() in ['wk', 'wks', 'week', 'weeks', 'w']:
        date = TODAY - relativedelta(weeks=int(splitted[0]))
        return str(date.isoformat())
    elif splitted[1].lower() in ['mon', 'mons', 'month', 'months', 'm']:
        date = TODAY - relativedelta(months=int(splitted[0]))
        return str(date.isoformat())
    elif splitted[1].lower() in ['yrs', 'yr', 'years', 'year', 'y']:
        date = TODAY - relativedelta(years=int(splitted[0]))
        return str(date.isoformat())
    else:
        return "Wrong Argument format"

def get_links(stock_ticker):
    link = []
    main_url = 'https://www.nasdaq.com/market-activity/stocks/' + stock_ticker + '/news-headlines'
    title = []
    date = []

    print()
    print('Fetching data...')

    main_url = urllib.request.urlopen(main_url)

    if main_url.getcode() == 200:
        main_data = main_url.read()
        soup = BeautifulSoup(main_data, 'html.parser')

        for days_ago in soup.findAll('span', class_ = 'quote-news-headlines__date'):
            date.append(get_past_date(days_ago.text))

        for headline in soup.findAll('p', class_ = 'quote-news-headlines__item-title'):
            title.append(headline.text)
        
        for article_link in soup.findAll('a', href = True, class_ = 'quote-news-headlines__link'):
            link.append('http://' + 'nasdaq.com' + article_link['href'])
        
        print('Data imported.')

    else:
        print('Connection error.')
    
    return link, title, date

def article_collection(link):
    print()
    print("Collecting artciles...")

    articles = []

    for url in link:
        temp_url = urllib.request.urlopen(str(url))

        if temp_url.getcode() == 200:
            temp_url_data = temp_url.read()
            soup = BeautifulSoup(temp_url_data, 'html.parser')
            temp_text = ''

            for body in soup.findAll('div', class_ = 'body__content'):
                for text in body.findAll('p'):
                    temp_text += '' + text.text

            articles.append(temp_text)
        
        else:
            print('Connection error.')
    
    else:
        print('Articles successfully collected.')
    
    return articles

def sentiment_analysis(articles):
    print()
    print('Analyzing text...')

    sentiment_list = []

    for text in articles:
        tokenized_sentences = sent_tokenize(text)
        analyzer = SentimentIntensityAnalyzer()
        article_sentiment_list = []

        for sentence in tokenized_sentences:
            article_sentiment_list.append(analyzer.polarity_scores(sentence))
    
        sentiment_list.append(article_sentiment_list)
    
    return sentiment_list
    
def sentiment_aggregation(sentiment_list):

    aggregated_sentiments = []

    for article in sentiment_list:
        temp_sum_neg = 0
        temp_sum_neu = 0
        temp_sum_pos = 0
        temp_sum_compound = 0

        for sentence in article:
            temp_sum_neg += sentence['neg']
            temp_sum_neu += sentence['neu']
            temp_sum_pos += sentence['pos']
            temp_sum_compound += sentence['compound']
        
        temp_avg_neg = temp_sum_neg / len(article)
        temp_avg_neu = temp_sum_neu / len(article)
        temp_avg_pos = temp_sum_pos / len(article)
        temp_avg_compound = temp_sum_compound / len(article)

        aggregated_sentiments.append((temp_avg_neg, temp_avg_neu, temp_avg_pos, temp_avg_compound))
    
    print('Analysis complete.')

    return aggregated_sentiments

def compile_dataframe(date, title, link, aggregated_sentiments, closing_price):
    print()
    print('Compiling dataframe...')

    dataframe = pandas.DataFrame()

    negative_sentiment = []
    neutral_sentiment = []
    positive_sentiment = []
    compound_sentiment = []
    script_run_date = []
    price = []

    for sentiment in aggregated_sentiments:
        negative_sentiment.append(sentiment[0])
        neutral_sentiment.append(sentiment[1])
        positive_sentiment.append(sentiment[2])
        compound_sentiment.append(sentiment[3])
        script_run_date.append(datetime.date.today())
        price.append(closing_price)


    dataframe['Script Run Date'] = script_run_date
    dataframe['Publish Date'] = date
    dataframe['Article Title'] = title
    dataframe['Link'] = link
    dataframe['Negative Sentiment Score'] = negative_sentiment
    dataframe['Neutral Sentiment Score'] = neutral_sentiment
    dataframe['Positive Sentiment Score'] = positive_sentiment
    dataframe['Compound Sentiment Score'] = compound_sentiment
    dataframe['Closing Price on Script Run Date'] = price

    dataframe_filter = dataframe['Publish Date'] == str(datetime.date.today())
    dataframe = dataframe[dataframe_filter]

    if len(dataframe.index) == 0:
        dataframe['Script Run Date'] = [datetime.date.today()]
        dataframe['Publish Date'] = [None]
        dataframe['Article Title'] = [None]
        dataframe['Link'] = [None]
        dataframe['Negative Sentiment Score'] = [None]
        dataframe['Neutral Sentiment Score'] = [None]
        dataframe['Positive Sentiment Score'] = [None]
        dataframe['Compound Sentiment Score'] = [None]
        dataframe['Closing Price on Script Run Date'] = [closing_price]

    print('Dataframe compiled.')
    print()

    print(dataframe)

    return dataframe

def get_closing_price(stock_ticker):
    print()
    print('Fetching stock price...')

    url = 'https://ca.finance.yahoo.com/quote/' + stock_ticker
    url = urllib.request.urlopen(url)

    if url.getcode() == 200:
        soup = BeautifulSoup(url, 'html.parser')
        price = soup.find('span', class_ = 'Trsdu(0.3s)').text

    print('Stock price imported.')

    return price

def main():
    stock = ['aapl', 'msft', 'tsla', 'evlo', 'imgn']

    for stock_ticker in stock:
        print(f'------------------------- {stock_ticker.upper()} -------------------------')

        link, title, date = get_links(stock_ticker)
        articles = article_collection(link)
        sentiment_list = sentiment_analysis(articles)
        aggregated_sentiments = sentiment_aggregation(sentiment_list)
        closing_price = get_closing_price(stock_ticker)
        
        dataframe = compile_dataframe(date, title, link, aggregated_sentiments, closing_price)

        # output_file = open(f'Exported Data\\{stock_ticker}.csv', 'w', newline = '')
        dataframe.to_csv(f'Exported Data\\{stock_ticker}.csv', sep = ',', mode = 'a', header = False, index = False)

        print()
        print('Execution complete.')
        print()

if __name__ == '__main__':
    main()

