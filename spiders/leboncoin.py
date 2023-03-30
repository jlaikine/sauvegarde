import scrapy
import datetime
import json
import time
from nordvpn_switcher import initialize_VPN,rotate_VPN,terminate_VPN
from scrapy.http import HtmlResponse
import mysql.connector
import requests
import random
from random import randint

#pip install nordvpn-switcher
#Doc python nordvpn : https://pypi.org/project/nordvpn-switcher/

class InfoSpider(scrapy.Spider):
    name = "leboncoin"

    #VPN
    #initialize_VPN(save=1,area_input=['France'])

    next_page = ""

    if_none_match_list = ['10r1dpqc7wr8z9d','lar3s6sdie9e7x''gny5yg9yr97b9','lqd3hqklsz91ae']
    etag_list = ['731lvrg13593ud','gny5yg9yr97b9','lqd3hqklsz91ae','13p57oqbrv29dtw']
    x_amz_cf_pop_list = ['BRU50-C1','FCO50-P2','FCO50-P2','FCO50-P2']
    set_cookie_list = ['datadome=2mjQFU_C2c0YrU1_dOcNUlkwCx~TbiteSD3X7xGwPeNvxlvd53nCEQ4fhnmtcYMCK4epjag5bvbSkCs78WAK1iGKmHO0o65BtacAuei-hHBnu~LrKV2rStQoDlBfqWgn; Max-Age=31536000; Domain=.leboncoin.fr; Path=/; Secure; SameSite=Lax','datadome=49a76xtxX893cDRwUeTuvp3O~Dvzv9~~ESeliRbcAD7-RatLzhv5KJjnGlIJH-NaRbei2D8H0TH6wjcnN~9oe5OOjrOQa~LQ9S6EZztwNfBbmVv_s8arQ9cwdfU__z51; Max-Age=31536000; Domain=.leboncoin.fr; Path=/; Secure; SameSite=Lax','datadome=5TwDC-6DhKAM15MC9l0I45pwlbGlqKGDrjIv7UFj1xdmJ-RHoeipErCTs6H13qMBMLQFlEKJ5kaythACjhRoeI-nwJZKRz_ngPXdjlZK6ATfcc5_PJVzmoOi9kDB2UM1; Max-Age=31536000; Domain=.leboncoin.fr; Path=/; Secure; SameSite=Lax','datadome=15tcYH5puVkYwKbHL32GgjYuMqy1zwSgQQB0xGjdZ3wrL00xfnSwqDJizWnRs4g0uuSwFayGnZGFWoFl16V2o-W5LPXDEy4jcf-aTPTe7IMcBAdtV2bfQ7CMwCiiOAWq; Max-Age=31536000; Domain=.leboncoin.fr; Path=/; Secure; SameSite=Lax']
    via_list = ['1.1 0e3d5915b30e289999d244786c9a2560.cloudfront.net (CloudFront)','1.1 943ac91773c1131d216a6b461db5b85e.cloudfront.net (CloudFront)','1.1 87b051fb2febd3f078ef2ce16da0dd3c.cloudfront.net (CloudFront)','1.1 87b051fb2febd3f078ef2ce16da0dd3c.cloudfront.net (CloudFront)']
    x_amz_cf_id_list = ['hlb9PlMYQBSYTHiOYJthPZ6K4r9CnNLbRbbfjiv8cxPKSHmsVkEPHQ==','pefSWVyqNh53pZhgz61RMbKoa3BjmNqlqzEOAfmTd2cTzWAVtvsbJg==','t8JCbI7uT47cGhUiPB7EVq98W4qIDHF6MWcCHbO8CgupUb7T1bkgcQ==','DQwer5F_YdS570TLRNpqf9N1e4qWC4Jvq7Lln3LmXu4-hrqiZ1DpQQ==']
    
    if_none_match = if_none_match_list[0]
    etag = etag_list[0]
    x_amz_cf_pop = x_amz_cf_pop_list[0]
    set_cookie = set_cookie_list[0]
    via = via_list[0]
    x_amz_cf_id = x_amz_cf_id_list[0]

    headers =  {
        'User-Agent' : 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/111.0',
        'Accept' : 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
        'Accept-Encoding' : 'gzip, deflate, br',
        'Accept-Language' : 'fr,fr-FR;q=0.8,en-US;q=0.5,en;q=0.3',
        'Connection' : 'keep-alive',
        'Host' : 'www.leboncoin.fr',
        'Sec-Fetch-Dest' : 'document',
        'Sec-Fetch-Mode' : 'navigate',
        'Sec-Fetch-Site' : 'none',
        'Sec-Fetch-User' : '?1',
        'Upgrade-Insecure-Requests' : 1,

        'accept-ch' : 'Sec-CH-UA,Sec-CH-UA-Mobile,Sec-CH-UA-Platform,Sec-CH-UA-Arch,Sec-CH-UA-Full-Version-List,Sec-CH-UA-Model,Sec-CH-Device-Memory',
        'cache-control' : 'public',
        'content-encoding' : 'gzip',
        'content-security-policy' : 'frame-ancestors *.leboncoin.fr *.leboncoin.io *.leboncoin.ci; report-uri https://api.leboncoin.fr/api/csp-report/v1/report/;',
        'content-security-policy-report-only' : 'object-src *.leboncoin.fr *.leboncoin.io *.leboncoin.ci; frame-ancestors *.leboncoin.fr *.leboncoin.io *.leboncoin.ci; report-uri https://api.leboncoin.fr/api/csp-report/v1/report/;',
        'content-type' : 'text/html; charset=utf-8',
        'referrer-policy' : 'no-referrer-when-downgrade',
        'strict-transport-security' : 'max-age=15768000',
        'vary' : 'Accept-Encoding',
        'x-cache' : 'Miss from cloudfront',
        'x-datadome' : 'protected',

        'If-None-Match' : if_none_match,
        'etag' : etag,
        'x-amz-cf-pop' : x_amz_cf_pop,
        'set-cookie' : set_cookie,
        'via' : via,
        'x-amz-cf-id' : x_amz_cf_id
    }

    #CONNEXION SQL
    def __init__(self):
        self.conn = mysql.connector.connect(
            host = 'localhost',
            user = 'root',
            password = 'root',
            database = 'test'
        )

        ## Create cursor, used to execute commands
        self.cur = self.conn.cursor()

    def changeHeaders(self):
        taille = randint(0,len(self.if_none_match_list)-1)

        self.if_none_match = self.if_none_match_list[taille]
        self.etag = self.etag_list[taille]
        self.x_amz_cf_pop = self.x_amz_cf_pop_list[taille]
        self.set_cookie = self.set_cookie_list[taille]
        self.via = self.via_list[taille]
        self.x_amz_cf_id = self.x_amz_cf_id_list[taille]

        self.headers =  {
            'User-Agent' : 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/111.0',
            'Accept' : 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
            'Accept-Encoding' : 'gzip, deflate, br',
            'Accept-Language' : 'fr,fr-FR;q=0.8,en-US;q=0.5,en;q=0.3',
            'Connection' : 'keep-alive',
            'Host' : 'www.leboncoin.fr',
            'Sec-Fetch-Dest' : 'document',
            'Sec-Fetch-Mode' : 'navigate',
            'Sec-Fetch-Site' : 'none',
            'Sec-Fetch-User' : '?1',
            'Upgrade-Insecure-Requests' : 1,

            'accept-ch' : 'Sec-CH-UA,Sec-CH-UA-Mobile,Sec-CH-UA-Platform,Sec-CH-UA-Arch,Sec-CH-UA-Full-Version-List,Sec-CH-UA-Model,Sec-CH-Device-Memory',
            'cache-control' : 'public',
            'content-encoding' : 'gzip',
            'content-security-policy' : 'frame-ancestors *.leboncoin.fr *.leboncoin.io *.leboncoin.ci; report-uri https://api.leboncoin.fr/api/csp-report/v1/report/;',
            'content-security-policy-report-only' : 'object-src *.leboncoin.fr *.leboncoin.io *.leboncoin.ci; frame-ancestors *.leboncoin.fr *.leboncoin.io *.leboncoin.ci; report-uri https://api.leboncoin.fr/api/csp-report/v1/report/;',
            'content-type' : 'text/html; charset=utf-8',
            'referrer-policy' : 'no-referrer-when-downgrade',
            'strict-transport-security' : 'max-age=15768000',
            'vary' : 'Accept-Encoding',
            'x-cache' : 'Miss from cloudfront',
            'x-datadome' : 'protected',

            'If-None-Match' : self.if_none_match,
            'etag' : self.etag,
            'x-amz-cf-pop' : self.x_amz_cf_pop,
            'set-cookie' : self.set_cookie,
            'via' : self.via,
            'x-amz-cf-id' : self.x_amz_cf_id
        }


    def on_error(self, response):
        print("ERROR...")
        rotate_VPN()
        time.sleep(5)
        if self.next_page == "":
            self.next_page = response.request.url
        yield scrapy.Request(self.next_page, callback=self.parse, dont_filter = True)

    
    def start_requests(self):
        urls = [
            'https://www.leboncoin.fr/recherche?category=9&real_estate_type=1%2C2',
        ]
        
        for url in urls:
            yield scrapy.Request(url=url, method='GET', callback=self.parse, headers = self.headers)

    def parse(self, response):

        #CHARGE LE FICHIER JSON
        scriptJson = response.css('script#__NEXT_DATA__').get()
        annonce = HtmlResponse(url=response.request.url, encoding='utf-8', body=scriptJson)
        content = annonce.xpath('//script/text()').get()
        data = json.loads(content)
        
        #DATA
        for info in data['props']['pageProps']['searchData']['ads']:
            #idAdd
            idAdd = None
            try:
                idAdd = info['list_id']
            except:
                idAdd = None
                
            #title
            title = None
            try:
                title = info['subject']
            except:
                title = None
                
            #first_publication_date
            first_publication_date = None
            try:                    
                first_publication_date = info['first_publication_date']
            except:
                first_publication_date = None

            #first_publication_date
            expiration_date = None
            try:
                expiration_date = info['expiration_date']
            except:
                expiration_date = None

            #index_date
            index_date = None
            try:
                index_date = info['index_date']
            except:
                index_date = None

            #status
            status = None
            try:
                status = info['status']
            except:
                status = None

            #category_id
            category_id = None
            try:
                category_id = info['category_id']
            except:
                category_id = None

            #category_name
            category_name = None
            try:
                category_name = info['category_name']
            except:
                category_name = None

            #ad_type
            ad_type = None
            try:
                ad_type = info['ad_type']
            except:
                ad_type = None
                
            #price
            price = None
            try:
                price = info['price'][0]
            except:
                price = None

            #price_cents
            price_cents = None
            try:
                price_cents = info['price_cents']
            except:
                price_cents = None

            #nb_images
            nb_images = None
            try:
                nb_images = info['images']['nb_images']
            except:
                nb_images = None

            #urlsImages
            urlsImages = []
            try:
                for urls in info['images']['urls']:
                    urlsImages.append(urls)
            except:
                urlsImages = []

            #country_id
            country_id = None
            try:
                country_id = info['location']['country_id']
            except:
                country_id = None

            #department_id
            department_id = None
            try:
                department_id = info['location']['department_id']
            except:
                department_id = None

            #department_name
            department_name = None
            try:
                department_name = info['location']['department_name']
            except:
                department_name = None

            #city_label
            city_label = None
            try:
                city_label = info['location']['city_label']
            except:
                city_label = None

            #city
            city = None
            try:
                city = info['location']['city']
            except:
                city = None

            #zipcode
            zipcode = None
            try:
                zipcode = info['location']['zipcode']
            except:
                zipcode = None

            #region_name
            region_name = None
            try:
                region_name = info['location']['region_name']
            except:
                region_name = None

            #activity_sector
            activity_sector = None
            real_estate_type = None
            square = None
            rooms = None
            energy_rate = None
            ges = None
            elevator = None
            fai_included = None
            floor_number = None
            nb_floors_building = None
            nb_parkings = None
            district_id = None
            district_visibility = None
            district_type_id = None
            district_resolution_type = None
            immo_sell_type = None
            is_import = None
            lease_type = None
            
            #attributes
            for attributes in info['attributes']:
                if "activity_sector" in attributes['key']:
                    try:
                        activity_sector = attributes['value_label']
                    except:
                        activity_sector = None
                if "real_estate_type" in attributes['key']:
                    try:
                        real_estate_type = attributes['value_label']
                    except:
                        real_estate_type = None
                if "square" in attributes['key']:
                    try:
                        square = attributes['value_label']
                    except:
                        square = None
                if "rooms" in attributes['key']:
                    try:
                        rooms = attributes['value_label']
                    except:
                        rooms = None
                if "energy_rate" in attributes['key']:
                    try:
                        energy_rate = attributes['value_label']
                    except:
                        energy_rate = None
                if "ges" in attributes['key']:
                    try:
                        ges = attributes['value_label']
                    except:
                        ges = None
                if "elevator" in attributes['key']:
                    try:
                        elevator = attributes['value_label']
                    except:
                        elevator = None
                if "fai_included" in attributes['key']:
                    try:
                        fai_included = attributes['value_label']
                    except:
                        fai_included = None
                if "floor_number" in attributes['key']:
                    try:
                        floor_number = attributes['value_label']
                    except:
                        floor_number = None
                if "nb_floors_building" in attributes['key']:
                    try:
                        nb_floors_building = attributes['value_label']
                    except:
                        nb_floors_building = None
                if "nb_parkings" in attributes['key']:
                    try:
                        nb_parkings = attributes['value_label']
                    except:
                        nb_parkings = None
                if "district_id" in attributes['key']:
                    try:
                        district_id = attributes['value_label']
                    except:
                        district_id = None
                if "district_visibility" in attributes['key']:
                    try:
                        district_visibility = attributes['value_label']
                    except:
                        district_visibility = None
                if "district_type_id" in attributes['key']:
                    try:
                        district_type_id = attributes['value_label']
                    except:
                        district_type_id = None
                if "district_resolution_type" in attributes['key']:
                    try:
                        district_resolution_type = attributes['value_label']
                    except:
                        district_resolution_type = None
                if "immo_sell_type" in attributes['key']:
                    try:
                        immo_sell_type = attributes['value_label']
                    except:
                        immo_sell_type = None
                if "is_import" in attributes['key']:
                    try:
                        is_import = attributes['value_label']
                    except:
                        is_import = None
                if "lease_type" in attributes['key']:
                    try:
                        lease_type = attributes['value_label']
                    except:
                        lease_type = None
                

            #user_id
            user_id = None
            try:
                user_id = info['owner']['user_id']
            except:
                user_id = None

            #type_owner
            type_owner = None
            try:
                type_owner = info['owner']['type']
            except:
                type_owner = None
                
            #name
            name = None
            try:
                name = info['owner']['name']
            except:
                name = None
                
            #siren
            siren = None
            try:
                siren = info['owner']['siren']
            except:
                siren = None
                
            #url
            url = None
            try:
                url = info['url']
            except:
                url = None

            self.cur.execute(""" SELECT url FROM leboncoin WHERE url=%s""", (url,))
            rows = self.cur.fetchone()
            if rows:
                print("Already in the database")
            else:
                try:
                    yield{
                        'idAdd' : idAdd,
                        'title' : title,
                        'first_publication_date' : first_publication_date,
                        'expiration_date' : expiration_date,
                        'index_date' : index_date,
                        'status' : status,
                        'category_id' : category_id,
                        'category_name' : category_name,
                        'ad_type' : ad_type,
                        'price' : price,
                        'price_cents' : price_cents,
                        'country_id' : country_id,
                        'nb_images' : nb_images,
                        'region_name' : region_name,
                        'department_id' : department_id,
                        'department_name' : department_name,
                        'city_label' : city_label,
                        'city' : city,
                        'zipcode' : zipcode,
                        'activity_sector' : activity_sector,
                        'real_estate_type' : real_estate_type,
                        'square' : square,
                        'rooms' : rooms,
                        'energy_rate' : energy_rate,
                        'ges' : ges,
                        'elevator' : elevator,
                        'fai_included' : fai_included,
                        'floor_number' : floor_number,
                        'nb_floors_building' : nb_floors_building,
                        'nb_parkings' : nb_parkings,
                        'district_id' : district_id,
                        'district_visibility' : district_visibility,
                        'district_type_id' : district_type_id,
                        'district_resolution_type' : district_resolution_type,
                        'immo_sell_type' : immo_sell_type,
                        'is_import' : is_import,
                        'lease_type' : lease_type,                        
                        'user_id' : user_id,
                        'type_owner' : type_owner,
                        'name' : name,
                        'siren' : siren,
                        'url' : url,

                        'urlsImages' : urlsImages,
                    }
                except:
                    yield{
                        'idAdd' : None,
                        'title' : None,
                        'first_publication_date' : None,
                        'expiration_date' : None,
                        'index_date' : None,
                        'status' : None,
                        'category_id' : None,
                        'category_name' : None,
                        'ad_type' : None,
                        'price' : None,
                        'price_cents' : None,
                        'country_id' : None,
                        'nb_images' : None,
                        'region_name' : None,
                        'department_id' : None,
                        'department_name' : None,
                        'city_label' : None,
                        'city' : None,
                        'zipcode' : None,
                        'activity_sector' : None,
                        'real_estate_type' : None,
                        'square' : None,
                        'rooms' : None,
                        'energy_rate' : None,
                        'ges' : None,
                        'elevator' : None,
                        'fai_included' : None,
                        'floor_number' : None,
                        'nb_floors_building' : None,
                        'nb_parkings' : None,
                        'district_id' : None,
                        'district_visibility' : None,
                        'district_type_id' : None,
                        'district_resolution_type' : None,
                        'immo_sell_type' : None,
                        'is_import' : None,
                        'lease_type' : None,                        
                        'user_id' : None,
                        'type_owner' : None,
                        'name' : None,
                        'siren' : None,
                        'url' : None,

                        'urlsImages' : None,
                    }
        
        #NEXT PAGE
        boutonFleche = response.css("div._1gbif div._1LfpU main#mainContent div.eGCJeq div.jvloxK nav.ePnYtC ul.ewYnVV li.hqasTv")[14]
        boutonNext = boutonFleche.css("a::attr(href)").get()
        if boutonNext is not None:
            self.next_page = response.urljoin(boutonNext)
            temps = random.uniform(4,6)
            print(temps)
            self.changeHeaders()
            print (self.headers)
            time.sleep(temps)
            
            yield scrapy.Request(self.next_page, callback=self.parse, headers = self.headers)
