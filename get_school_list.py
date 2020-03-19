import time
import mysql.connector
import requests
from mysql.connector import errorcode
import cfscrape
from bs4 import BeautifulSoup


def main():
    print('get page number')
    page_number = get_page(school_type='ortaokul', city='istanbul')
    print('page_count', page_number)
    print('scraping in progress...')
    stats = scrape(school_type='ortaokul', city='istanbul', endpage=page_number)
    print('scraping complete')
    print('connection database')
    connection = create_connection_db()
    print('adding data...')
    for stat in stats:
        data_base = connection.cursor()
        data_base.execute("""INSERT INTO crawler_school (name,type,city,town,sub_town,detail_link,is_premium)
        VALUES (%s, %s, %s, %s, %s, %s, %s);""",
                          (stat['name'].strip('\n'), stat['type'].strip('\n'), stat['city'].strip('\n'),
                           stat['town'].strip('\n'), stat['sub_town'].strip('\n'), stat['detail_link'].strip('\n'),
                           stat['is_premium']))
        connection.commit()
    print('data saved')

def create_connection_db():
    try:
        connection = mysql.connector.connect(
            user='root',
            password='',
            database='content_management',
            host='127.0.0.1',
            port='3306'
        )
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Something is wrong with your user name or password")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Database does not exist")
        else:
            print(err)
    finally:
        if connection is not None:
            return connection


def scrape(school_type, city, endpage):
    try:
        scraped_stats = []
        school = {}
        session = requests.session()
        session.headers = ...
        for page in range(endpage):
            scraper = cfscrape.create_scraper(sess=session, delay=15)
            print('scrapping_ page: ', page)
            url = """https://okul.com.tr/{0}/{1}?page={2}""".format(school_type, city.replace('İ','i').strip('\n  '), str(page).strip(' '))
            html_content = scraper.get(url).content
            soup = BeautifulSoup(html_content, 'html.parser')
            school_list = soup.find_all('div', {'class': 'col-md-12'})
            for list in school_list:
                name = list.find('a', {'class': 'title'}).contents
                address = list.find('a', {'class': 'address'}).contents
                split_address = address[0].split("/")
                city = split_address[0]
                town = split_address[1]
                sub_town = split_address[2]
                school_detail = list.find('a', {'class': 'url-link'})
                link = school_detail.get('href')
                if len(name) == 3:
                    school['is_premium'] = 1
                else:
                    school['is_premium'] = 0
                school['type'] = school_type
                school['name'] = name[0]
                school['city'] = city
                school['town'] = town
                school['sub_town'] = sub_town
                school['detail_link'] = link
                scraped_stats.append(school.copy())
            time.sleep(120)
    except Exception as e:
        print(e)
    finally:
        return scraped_stats


def get_page(school_type, city):
    try:
        url = 'https://okul.com.tr/{}/{}'.format(school_type, city)
        session = requests.session()
        session.headers = ...
        scraper = cfscrape.create_scraper(sess=session, delay=15)
        html_content = scraper.get(url).content
        soup = BeautifulSoup(html_content, 'html.parser')
        soup_list = soup.find_all('a', {'class': 'page-link'})
        find_end_page = len(soup_list)-2
        end_page = soup_list[find_end_page].contents
        end_page_number = int(end_page[0])
    except Exception as e:
        print(e)
    finally:
        return end_page_number

if __name__ == "__main__":
    main()