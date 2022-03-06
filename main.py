import requests
from requests.adapters import HTTPAdapter, Retry
import logging
import json
from fake_useragent import UserAgent
import sqlite3


logging.basicConfig(level=logging.INFO)

cities = ['aarhus', 'adelaide', 'albuquerque', 'almaty', 'amsterdam', 'anchorage', 'andorra', 'ankara', 'asheville', 'asuncion', 'athens', 'atlanta', 'auckland', 'austin', 'baku', 'bali', 'baltimore', 'bangkok', 'barcelona', 'beijing', 'beirut', 'belfast', 'belgrade', 'belize-city', 'bengaluru', 'bergen', 'berlin', 'bern', 'bilbao', 'birmingham', 'birmingham-al', 'bogota', 'boise', 'bologna', 'bordeaux', 'boston', 'boulder', 'bozeman', 'bratislava', 'brighton', 'brisbane', 'bristol', 'brno', 'brussels', 'bucharest', 'budapest', 'buenos-aires', 'buffalo', 'cairo', 'calgary', 'cambridge', 'cape-town', 'caracas', 'cardiff', 'casablanca', 'charleston', 'charlotte', 'chattanooga', 'chennai', 'chiang-mai', 'chicago', 'chisinau', 'christchurch', 'cincinnati', 'cleveland', 'cluj-napoca', 'cologne', 'colorado-springs', 'columbus', 'copenhagen', 'cork', 'curitiba', 'dallas', 'dar-es-salaam', 'delhi', 'denver', 'des-moines', 'detroit', 'doha', 'dresden', 'dubai', 'dublin', 'dusseldorf', 'edinburgh', 'edmonton', 'eindhoven', 'eugene', 'florence', 'florianopolis', 'fort-collins', 'frankfurt', 'fukuoka', 'gaillimh', 'gdansk', 'geneva', 'gibraltar', 'glasgow', 'gothenburg', 'grenoble', 'guadalajara', 'guatemala-city', 'halifax', 'hamburg', 'hannover', 'havana', 'helsinki', 'ho-chi-minh-city', 'hong-kong', 'honolulu', 'houston', 'hyderabad', 'indianapolis', 'innsbruck', 'istanbul', 'jacksonville', 'jakarta', 'johannesburg', 'kansas-city', 'karlsruhe', 'kathmandu', 'kiev', 'kingston', 'knoxville', 'krakow', 'kuala-lumpur', 'kyoto', 'lagos', 'la-paz', 'las-palmas-de-gran-canaria', 'las-vegas', 'lausanne',
          'leeds', 'leipzig', 'lille', 'lima', 'lisbon', 'liverpool', 'ljubljana', 'london', 'los-angeles', 'louisville', 'luxembourg', 'lviv', 'lyon', 'madison', 'madrid', 'malaga', 'malmo', 'managua', 'manchester', 'manila', 'marseille', 'medellin', 'melbourne', 'memphis', 'mexico-city', 'miami', 'milan', 'milwaukee', 'minneapolis-saint-paul', 'minsk', 'montevideo', 'montreal', 'moscow', 'mumbai', 'munich', 'nairobi', 'nantes', 'naples', 'nashville', 'new-orleans', 'new-york', 'nice', 'nicosia', 'oklahoma-city', 'omaha', 'orlando', 'osaka', 'oslo', 'ottawa', 'oulu', 'oxford', 'palo-alto', 'panama', 'paris', 'perth', 'philadelphia', 'phnom-penh', 'phoenix', 'phuket', 'pittsburgh', 'portland-me', 'portland-or', 'porto', 'porto-alegre', 'prague', 'providence', 'quebec', 'quito', 'raleigh', 'reykjavik', 'richmond', 'riga', 'rio-de-janeiro', 'riyadh', 'rochester', 'rome', 'rotterdam', 'saint-petersburg', 'salt-lake-city', 'san-antonio', 'san-diego', 'san-francisco-bay-area', 'san-jose', 'san-juan', 'san-luis-obispo', 'san-salvador', 'santiago', 'santo-domingo', 'sao-paulo', 'sarajevo', 'saskatoon', 'seattle', 'seoul', 'seville', 'shanghai', 'singapore', 'skopje', 'sofia', 'st-louis', 'stockholm', 'stuttgart', 'sydney', 'taipei', 'tallinn', 'tampa-bay-area', 'tampere', 'tartu', 'tashkent', 'tbilisi', 'tehran', 'tel-aviv', 'the-hague', 'thessaloniki', 'tokyo', 'toronto', 'toulouse', 'tunis', 'turin', 'turku', 'uppsala', 'utrecht', 'valencia', 'valletta', 'vancouver', 'victoria', 'vienna', 'vilnius', 'warsaw', 'washington-dc', 'wellington', 'winnipeg', 'wroclaw', 'yerevan', 'zagreb', 'zurich']


def scrape():
    ua = UserAgent()
    s = requests.Session()
    retries = Retry(total=5, backoff_factor=1,
                    status_forcelist=[500, 501, 502, 503, 504])
    s.mount('https://', HTTPAdapter(max_retries=retries))

    def scrape_json(resource_uri, target):
        failed = {}
        for resource, uri in resource_uri.items():
            try:
                res = s.get(uri.format(target), headers={'user-agent': ua.random,
                                                         "dnt": "1", "referer": "https://teleport.org/",
                                                         "origin": "https://teleport.org",
                                                         "authority": "search.internal.teleport.org",
                                                         "accept": "*/*; version=3",
                                                         "accept-language": "en-GB,en-US;q=0.9,en;q=0.8",
                                                         "accept-encoding": "gzip, deflate, br"})
                if not res.ok:
                    failed[resource] = res.text
                else:
                    with open(f'data-{resource}/{target}.json', 'w') as f:
                        f.write(res.text)
            except Exception as e:
                failed[resource] = str(e)
        return failed

    failed = {}
    for city in cities:
        err = scrape_json({
            "salary": "https://eightball.internal.teleport.org/api/salary-all-professions/slug:{}/",
            "costs": "https://search.internal.teleport.org/api/urban_areas/slug:{}/subpage/cost_of_living/"}, city)
        if err:
            failed[city] = err

    if failed:
        print(f"failed: {len(failed)}")
        print(json.dumps(failed))

def dumpsql():
    db = sqlite3.connect("x.db")
    cur = db.cursor()
    cur.executescript( """
        DROP TABLE IF EXISTS city;
        DROP TABLE IF EXISTS cost_house;
        DROP TABLE IF EXISTS salary;
        CREATE TABLE city (id TEXT PRIMARY KEY, name TEXT, continent TEXT, country TEXT, currency TEXT);
        CREATE TABLE cost_house (id TEXT PRIMARY KEY, min0 real, mean0 real, max0 real, min1 real, mean1 real, max1 real, min2 real, mean2 real, max2 real);
        CREATE TABLE salary (id TEXT PRIMARY KEY, p25 real, p50 real, p75 real);
        """)

    city_dat = []
    costhouse_dat = []
    salary_dat = []
    for city in cities:
        with open(f'data-costs/{city}.json', 'r') as f:
            costjs = json.load(f)
            geojs = costjs['ua']
            city_dat.append((city, geojs['display_label'], geojs['continent'], geojs['country'], geojs['currency_code'],))

            housejs = costjs['data']['housing']
            costhouse_dat.append((city, 
                housejs[0]['rent']['min'], housejs[0]['rent']['mean'], housejs[0]['rent']['max'],
                housejs[1]['rent']['min'], housejs[1]['rent']['mean'], housejs[1]['rent']['max'],
                housejs[2]['rent']['min'], housejs[2]['rent']['mean'], housejs[2]['rent']['max']))
        
        with open(f'data-salary/{city}.json', 'r') as f:
            salaryjs = json.load(f)
            d = [city]
            for dat in salaryjs['salaries']:
                if dat['profession']['id'] == 'SOFTWARE-ENGINEER':
                    d.extend([
                        dat['percentiles'][0]['salary'],
                        dat['percentiles'][1]['salary'],
                        dat['percentiles'][2]['salary']])
                    break
            if len(d) == 4:
                salary_dat.append(tuple(d))
    cur.executemany("INSERT INTO city (id, name, continent, country, currency) VALUES (?,?,?,?,?)", city_dat)
    cur.executemany("INSERT INTO cost_house (id, min0, mean0, max0, min1, mean1, max1, min2, mean2, max2) VALUES (?,?,?,?,?,?,?,?,?,?)", costhouse_dat)
    cur.executemany("INSERT INTO salary (id, p25, p50, p75) VALUES (?,?,?,?)", salary_dat)
    db.commit()
    db.close()

if __name__ == "__main__":
    dumpsql()