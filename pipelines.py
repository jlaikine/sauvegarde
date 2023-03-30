# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html

import mysql.connector

# useful for handling different item types with a single interface
from itemadapter import ItemAdapter


class LeboncoinPipeline:

    def __init__(self):
        self.conn = mysql.connector.connect(
            host = 'localhost',
            user = 'root',
            password = 'root',
            database = 'test'
        )

        ## Create cursor, used to execute commands
        self.cur = self.conn.cursor()
        
        self.cur.execute("""
            CREATE TABLE IF NOT EXISTS leboncoin(
                id INT PRIMARY KEY NOT NULL AUTO_INCREMENT,
                idAdd BIGINT NOT NULL,
                title VARCHAR(245),
                first_publication_date DATETIME,
                expiration_date DATETIME,
                index_date DATETIME,
                status VARCHAR(45),
                category_id INT,
                category_name VARCHAR(45),
                ad_type VARCHAR(45),
                price INT,
                price_cents INT,
                country_id VARCHAR(45),
                nb_images INT,
                region_name VARCHAR(45),
                department_id INT,
                department_name VARCHAR(45),
                city_label VARCHAR(245),
                city VARCHAR(45),
                zipcode VARCHAR(10),
                activity_sector INT,
                real_estate_type VARCHAR(45),
                square VARCHAR(45),
                rooms VARCHAR(45),
                energy_rate VARCHAR(45),
                ges VARCHAR(45),
                elevator VARCHAR(45),
                fai_included VARCHAR(10),
                floor_number INT,
                nb_floors_building INT,
                nb_parkings VARCHAR(45),
                district_id INT,
                district_visibility VARCHAR(10),
                district_type_id INT,
                district_resolution_type VARCHAR(45),
                immo_sell_type VARCHAR(45),
                is_import  VARCHAR(10),
                lease_type VARCHAR(45),
                user_id VARCHAR(245),
                type_owner VARCHAR(45),
                name VARCHAR(245),
                siren INT,
                url VARCHAR(245)
            ) 
        """)

        self.cur.execute("""
            CREATE TABLE IF NOT EXISTS leboncoinImages(
                id INT PRIMARY KEY NOT NULL AUTO_INCREMENT,
                idAdd BIGINT NOT NULL,
                url VARCHAR(245)
            )
        """)

    def process_item(self, item, spider):
        self.cur.execute(""" INSERT INTO leboncoin (idAdd, title, first_publication_date, expiration_date, index_date, status, category_id, category_name, ad_type, price, price_cents, country_id, nb_images, region_name, department_id, department_name, city_label, city, zipcode, activity_sector, real_estate_type, square, rooms, energy_rate, ges, elevator, fai_included, floor_number, nb_floors_building, nb_parkings, district_id, district_visibility, district_type_id, district_resolution_type, immo_sell_type, is_import, lease_type, user_id, type_owner, name, siren, url) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""", (
            item["idAdd"],
            item["title"],
            item["first_publication_date"],
            item["expiration_date"],
            item["index_date"],
            item["status"],
            item["category_id"],
            item["category_name"],
            item["ad_type"],
            item["price"],
            item["price_cents"],
            item["country_id"],
            item["nb_images"],            
            item["region_name"],
            item["department_id"],
            item["department_name"],
            item["city_label"],
            item["city"],
            item["zipcode"],
            item["activity_sector"],
            item["real_estate_type"],
            item["square"],
            item["rooms"],
            item["energy_rate"],
            item["ges"],
            item["elevator"],
            item["fai_included"],
            item["floor_number"],
            item["nb_floors_building"],
            item["nb_parkings"],
            item["district_id"],
            item["district_visibility"],
            item["district_type_id"],
            item["district_resolution_type"],
            item["immo_sell_type"],
            item["is_import"],
            item["lease_type"],
            item["user_id"],
            item["type_owner"],
            item["name"],
            item["siren"],
            item["url"],
        ))

        for url in item["urlsImages"]:
            self.cur.execute(""" INSERT INTO leboncoinImages (idAdd, url) VALUES (%s,%s)""", (
                item["idAdd"],
                url,
            ))

        ## Execute insert of data into database
        self.conn.commit()

        return item


    def close_spider(self, spider):

        ## Close cursor & connection to database 
        self.cur.close()
        self.conn.close()