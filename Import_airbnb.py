import json
import sqlite3
#Do NOT put functions/statement outside functions

def start():
    #Connect to database airbnb.db
    conn = sqlite3.connect('airbnb.db')
    cursor = conn.cursor()
    
    #Delete existing Table
    cursor.execute('DROP TABLE IF EXISTS review')
    cursor.execute('DROP TABLE IF EXISTS reviewer')
    cursor.execute('DROP TABLE IF EXISTS amenities')
    cursor.execute('DROP TABLE IF EXISTS host_accommodation')
    cursor.execute('DROP TABLE IF EXISTS accommodation')
    cursor.execute('DROP TABLE IF EXISTS host')
    
    #Create review Table
    cursor.execute('''CREATE TABLE review 
                   (id INTEGER PRIMARY KEY AUTOINCREMENT, 
                   rid INTEGER, comment TEXT, 
                   datetime TEXT, 
                   accommodation_id INTEGER,
                   FOREIGN KEY (rid) REFERENCES reviewer (rid),
                   FOREIGN KEY (accommodation_id) REFERENCES accommodation (id))''')
                   
    #Create reviewer Table
    cursor.execute('''CREATE TABLE reviewer 
                   (rid INTEGER PRIMARY KEY,
                   rname TEXT)''')
                   
    #Create amenities Table
    cursor.execute('''CREATE TABLE amenities
                   (accommodation_id INTEGER,
                   type TEXT,
                   PRIMARY KEY (accommodation_id, type),
                   FOREIGN KEY (accommodation_id) REFERENCES accommodation (id))''')
                   
    #Create host accomodation Table
    cursor.execute('''CREATE TABLE host_accommodation 
                   (host_id INTEGER,
                   accommodation_id INTEGER,
                   PRIMARY KEY (host_id, accommodation_id),
                   FOREIGN KEY (host_id) REFERENCES host(host_id),
                   FOREIGN KEY (accommodation_id) REFERENCES accommodation(id))''')
                   
    #Create accommodation Table
    cursor.execute('''CREATE TABLE accommodation 
                   (id INTEGER PRIMARY KEY, name TEXT,
                   summary TEXT,
                   url TEXT,
                   review_score_value INTEGER)''')
                   
    #Create host Table
    cursor.execute('''CREATE TABLE host 
                   (host_id INTEGER PRIMARY KEY,
                   host_url TEXT,
                   host_name TEXT,
                   host_about TEXT,
                   host_location TEXT)''')
    
    cursor.execute('PRAGMA foreign_keys = ON')
    #Inset Data
    #import JSON into DB
    with open('airbnb.json',mode = 'r',encoding = 'utf-8') as file:
        data = file.read()
    file.close()
    
    #Convert data to json
    listings = json.loads(data)
    for listing in listings:
        accommodation_id = listing['_id']
        accommodation_name = listing['name']
        accommodation_summary = listing['summary']
        accommodation_url = listing['listing_url']
        accomodation_r_score_value = listing['review_scores'].get('review_scores_value')
        cursor.execute('INSERT INTO accommodation (id, name, summary, url, review_score_value) VALUES (?,?,?,?,?)', (accommodation_id,accommodation_name,accommodation_summary,accommodation_url,accomodation_r_score_value))
        
        host_id = listing['host']['host_id']
        host_url = listing['host']['host_url']
        host_name = listing['host']['host_name']
        host_about = listing['host']['host_about']
        host_location = listing['host']['host_location']
        cursor.execute('SELECT * FROM host WHERE (host_id = ?)', (host_id,))
        entry = cursor.fetchone()
        if not entry:
            cursor.execute('INSERT INTO host (host_id,host_url,host_name,host_about,host_location) VALUES (?,?,?,?,?)',(host_id,host_url,host_name,host_about,host_location))
        
        cursor.execute('INSERT INTO host_accommodation (host_id, accommodation_id) VALUES (?,?)',(host_id,accommodation_id))
        
        for review_item in listing['reviews']:
            rid = review_item['reviewer_id']
            rname = review_item['reviewer_name']
            comment = review_item['comments']
            datetime = review_item['date']['$date']
            cursor.execute('SELECT * FROM reviewer WHERE (rid = ?)',(rid,))
            entry = cursor.fetchone()
            if not entry:
                cursor.execute('INSERT INTO reviewer (rid, rname) VALUES (?,?)',(rid,rname))
            
            cursor.execute('INSERT INTO review (rid, comment, datetime, accommodation_id) VALUES (?,?,?,?)',(rid, comment, datetime, accommodation_id))
        
        for amenities_type in listing['amenities']:
            cursor.execute('SELECT * FROM amenities WHERE (accommodation_id = ? AND type = ?)',((accommodation_id, amenities_type)))
            entry = cursor.fetchone()
            if not entry:
                cursor.execute('INSERT INTO amenities (accommodation_id, type) VALUES (?,?)', (accommodation_id, amenities_type))
                
    cursor.close()
    conn.commit()
    conn.close()
if __name__ == '__main__':
    start()

