import requests
from bs4 import BeautifulSoup


def crawl_data(url):

    headers = {}
    f = requests.get(url, headers = headers)

    soup = BeautifulSoup(f.content, 'lxml')
    movies = soup.find_all('article', { 'class': 'item movies'})
    movies_info = []

    for anchor in movies:
        movie_item = {}
    
        movie_status = anchor.find('div', {'class': 'trangthai'}).text
        movie_footer = anchor.find('div', {'class': 'data'})
        movie_link = movie_footer.find('a')['href']
        vn_name = movie_footer.find('a').text
        en_name = movie_footer.find('span').text
        img_link = anchor.find('img')
        poster_image_link = img_link['src']
        movie_name = img_link['alt']
        
        movie_item["id"] = anchor["id"].split('-')[1]
        movie_item["movie_link"] = movie_link.replace("'", "")
        movie_item["poster_image_link"] = poster_image_link.replace("'", "")
        movie_item["movie_name"] = movie_name.replace("'", "")
        movie_item["movie_status"] = movie_status.replace("'", "")
        movie_item["en_name"] = en_name.replace("'", "")
        movie_item["vn_name"] = vn_name.replace("'", "")
        movie_item["category"] = url.split("/")[-1]

        movies_info.append(movie_item)

    return movies_info


def load_data_to_db_using_insert_value(connection, table_name, data_field, list_data):
    # step 1: check table_name exists
    # step 2: data cleaning, checking
    # step 3: insert data to table
    # step 4: close connection
    pass


import json
def load_data_to_db_using_json_file(connection, table_name, data_field, list_data):
    # step 1: check table_name exists
    # step 2: data cleaning, checking
    # step 3: insert data to table
    # step 4: close connection
    
    with connection.cursor() as cur:

        #TODO using file_path to insert data
        with open('file_path.json') as my_file:
            data = json.load(my_file)

            #TODO execute create table
            cur.execute(""" create table if not exists json_table(
                p_id integer, first_name text, last_name text, p_attribute jsonb,
                quote_content text) """)
            
            #TODO execute insert data to table from json file
            query_sql = """ insert into json_table
                select * from json_populate_recordset(NULL::json_table, %s) """
            cur.execute(query_sql, (json.dumps(data),))


def crawl_box_office(url: str, date = '2000-01-01'):
    import datetime
    process_date = datetime.datetime.strptime(date, '%Y-%m-%d').strftime('%Y-%m-%d')
    headers = {}
    if not url.endswith('/'):
        url = url + '/'
    url = f'{url}{process_date}'
    f = requests.get(url, headers = headers)
    
    def get_title_id(release_id: str):
        url = f'https://www.boxofficemojo.com/release/{release_id}/'
        headers = {}
        f = requests.get(url, headers = headers)
        try:
            soup = BeautifulSoup(f.content, 'lxml')
            main = soup.find('main')\
                .find('div', {'class': 'a-section mojo-body aok-relative'})\
                .find('div', {'class': 'a-section mojo-title-release-refiner'})\
                .find('div', {'id': 'title-summary-refiner'})\
                .find('a')
            
            lin = main['href']
            return lin.split('/')[2]
        except Exception as e:
            return None

    soup = BeautifulSoup(f.content, 'lxml')
    main = soup.find('main')\
        .find('div', {'class': 'a-section mojo-body aok-relative'})\
        .find('div', {'class': 'a-section a-spacing-none'})\
        .find('div', {'id': 'table'})\
        .find('div', {'class': 'a-section imdb-scroll-table-inner'})\
        .find('table', {'class': 'a-bordered a-horizontal-stripes a-size-base a-span12 mojo-body-table mojo-table-annotated mojo-body-table-compact'})
        
    movies_info = []

    for indx, anchor in enumerate(main):
        if indx > 0:
            movie_item = {}
            
            rank_in_today = anchor.contents[0].text
            rank_in_yesterday = anchor.contents[1].text
            _film = anchor.contents[2]\
                .find('a', {'class': 'a-link-normal'})
            movie_name = _film.text
            movie_link = _film['href']
            movie_release_id = movie_link.split('/')[2]
            movie_title_id = get_title_id(movie_release_id)
            daily_gross = anchor.contents[3].text
            day_gross_change = anchor.contents[4].text
            week_gross_change = anchor.contents[5].text
            number_of_theater = anchor.contents[6].text
            avg_per_theater = anchor.contents[7].text
            gross_to_date = anchor.contents[8].text
            num_of_day_release = anchor.contents[9].text
            distributor = anchor.contents[10].text
            
            movie_item['rank_in_today'] = rank_in_today
            movie_item['rank_in_yesterday'] = rank_in_yesterday
            movie_item['movie_name'] = movie_name
            movie_item['movie_link'] = movie_link
            movie_item['movie_release_id'] = movie_release_id
            movie_item['movie_title_id'] = movie_title_id
            movie_item['daily_gross'] = daily_gross
            movie_item['day_gross_change'] = day_gross_change
            movie_item['week_gross_change'] = week_gross_change
            movie_item['number_of_theater'] = number_of_theater
            movie_item['avg_per_theater'] = avg_per_theater
            movie_item['gross_to_date'] = gross_to_date
            movie_item['num_of_day_release'] = num_of_day_release
            movie_item['distributor'] = distributor
            movie_item['date_crawled'] = process_date

            movies_info.append(movie_item)

    return movies_info
