import time
import datetime
import cfscrape
from bs4 import BeautifulSoup
import requests
import mysql.connector
from mysql.connector import errorcode
import logging

logging.basicConfig(filename='app.log', filemode='a+', format='%(name)s - %(levelname)s - %(message)s')

def main():
    try:
        print('getting schools...')
        records = get_records()
        school = []
        print('getting school details...')
        logging.warning('process started - {}'.format(datetime.datetime.now()))
        # records = [['19', 'https://okul.com.tr/ilkokul/istanbul/ozel-yakacik-balkanlar-koleji-ilkokulu-4060'], ['20', 'https://okul.com.tr/ilkokul/istanbul/ozel-atasehir-geo-koleji-ilkokulu-13692']]
        for row in records:
            school.append(scrape_school_detail(id=row[0], url=row[1]))
        print(datetime.datetime.now(), 'done')
        print('adding data...')
        print(datetime.datetime.now())
        print('connection database')
        connection = create_connection_db()
        for data in school:
            data_base = connection.cursor()
            data_base.execute("""INSERT INTO crawler_schooldetail (school_id,features,about_title,about_content,facilities)
                    VALUES (%s, %s, %s, %s, %s);""",
                              (data['school_id'], data['features'], data['about_title'],
                               data['about_content'], data['facilities']))
            connection.commit()
        print('data saved')
        logging.warning('process completed - {}'.format(datetime.datetime.now()))
    except Exception as e:
        logging.error('{} - school id: {}'.format(e, row[0]))

def scrape_school_detail(id, url):
    try:
        session = requests.session()
        session.headers = ...
        school_detail = {}
        features = []
        facilities = []
        scraper = cfscrape.create_scraper()
        html_content = scraper.get(url).content
        soup = BeautifulSoup(html_content, 'html.parser')
        block_features = soup.find_all('span', {'class': 'detail-icon-text'})
        block_about = soup.find('div', {'id': 'about'})
        if block_about is not None:
            about_title = block_about.find('h2', {'class': 'title'}).text
            about_content = block_about.find('p', {'id': 'about_p'}).text
            school_detail['about_title'] = about_title
            school_detail['about_content'] = about_content
        else:
            school_detail['about_title'] = None
            school_detail['about_content'] = None
            logging.warning('About block is none - school id: {}'.format(id))
        block_facilities = soup.find_all('span', {'class': 'block mb-5 pl-20 fs-14 f-wei-4 detailStatus type-2 fc-5'})
        if not block_facilities:
            block_facilities = soup.find_all('span', {'class': 'block mb-5 pl-20 fs-14 f-wei-6 detailStatus type-2 fc-5'})
        for feature in block_features:
            features.append(feature.text.strip('\n'))
        for facility in block_facilities:
            facilities.append(facility.text.strip('\n'))
        school_detail['school_id'] = id
        school_detail['features'] = str(features)
        school_detail['facilities'] = str(facilities)
        logging.warning('scrape is completed - school id: {} - {}'.format(id, datetime.datetime.now()))
        time.sleep(60)
    except Exception as e:
        logging.error('{} - school id: {}'.format(e, id))
    finally:
        return school_detail

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
            logging.error('Something is wrong with your user name or password')
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            logging.error('Database does not exist')
        else:
            logging.error(err)
    finally:
        if connection is not None:
            return connection


def get_records():
    try:
        connection = create_connection_db()
        cursor = connection.cursor()
        cursor.execute("SELECT id, detail_link FROM crawler_school Where id > '4000' AND id < '4501'")
        records = cursor.fetchall()
    except Exception as e:
        logging.error('cursor execute error - {]'.format(e))
    finally:
        return records

if __name__ == "__main__":
    main()