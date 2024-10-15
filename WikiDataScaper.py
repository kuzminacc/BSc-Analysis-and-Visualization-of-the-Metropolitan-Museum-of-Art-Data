import requests
from bs4 import BeautifulSoup
from pymongo import MongoClient
import re

# Povezivanje sa MongoDB bazom podataka
client = MongoClient('localhost', 27017)
db = client['museum_database']
collection = db['artistsUpgraded']

# Preuzimanje dokumenata iz kolekcije
documents = collection.find()

# Iteriranje kroz dokumente
for doc in documents:
    # Preuzimanje URL-a iz Wikidata atributa za umetnika
    wikidata_url = doc.get('Artist Wikidata URL')
    
    # Provera da li postoji validan URL iz Wikidata atributa
    if not wikidata_url or wikidata_url == "" or wikidata_url == "(not assigned)":
        print(f"Preskačem dokument {doc['_id']} zbog praznog ili nevažećeg URL-a iz Wikidata atributa.")
        continue
    
    try:
        # Zahtevanje HTML stranice preko URL-a iz Wikidata atributa
        response = requests.get(wikidata_url)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Inicijalizacija liste muzeja i mesta boravka
        museums = []
        lived_places = []

        # Pretraga div elemenata koji sadrže podatke o muzejima
        div_root1 = soup.find('div', id='P551')
        if div_root1:
            # Iteracija kroz div elemente koji sadrže podatke o muzejima
            div_root2 = div_root1.find_all('div', class_='wikibase-statementview-mainsnak-container')
            for div2 in div_root2:
                div_root3 = div2.find_all('div', class_='wikibase-statementview-mainsnak')
                for div3 in div_root3:
                    div = div3.find_all('div', class_='wikibase-snakview-value wikibase-snakview-variation-valuesnak')
                    for d in div:
                        anchors = d.find_all('a')
                        for a in anchors:
                            h = a['href']
                            # Preuzimanje koordinata lokacije muzeja sa Wikidata stranice
                            responseLocation = requests.get('https://www.wikidata.org/' + h)
                            soupLocation = BeautifulSoup(responseLocation.content, 'html.parser')
                            div_root1Location = soupLocation.find('div', id='P625')

                            if div_root1Location:
                                div_root2Location = div_root1Location.find('div', class_='wikibase-statementview-mainsnak-container')
                                div_root3Location = div_root2Location.find('div', class_='wikibase-statementview-mainsnak')
                                div_root4Location = div_root3Location.find('div', class_='wikibase-kartographer-caption')
                                # Parsiranje koordinata
                                match = re.search(r'(\d+)°(\d+)\'([\d.]+)"(N|S),\s*(\d+)°(\d+)\'([\d.]+)"(E|W)', div_root4Location.text)
                                if match:
                                    lat_degrees, lat_minutes, lat_seconds, lat_direction, lon_degrees, lon_minutes, lon_seconds, lon_direction = match.groups()
                                    latitude = float(lat_degrees) + (float(lat_minutes) / 60) + (float(lat_seconds) / 3600)
                                    if lat_direction == 'S':
                                        latitude *= -1
                                    longitude = float(lon_degrees) + (float(lon_minutes) / 60) + (float(lon_seconds) / 3600)
                                    if lon_direction == 'W':
                                        longitude *= -1
                                    latitude = round(latitude, 7)
                                    longitude = round(longitude, 7)
                                    locationName = a.text
                                    lived_places.append({"type": "Feature", "geometry": {"type": "Point", "coordinates": [longitude, latitude]}, "properties": {"name": locationName}})
                                else:
                                    print("Koordinate nisu pronađene.")
                            
        # Pretraga div elemenata koji sadrže podatke o radovima umetnika
        div_root1Works = soup.find('div', id='P6379')

        if div_root1Works:

            div_root2Works = div_root1Works.find_all('div', class_='wikibase-statementview-mainsnak-container')

            for div2 in div_root2Works:
                div_root3Works = div2.find_all('div', class_='wikibase-statementview-mainsnak')
                for div3 in div_root3Works:
                    div = div3.find_all('div', class_='wikibase-snakview-value wikibase-snakview-variation-valuesnak')
                    for d in div:
                        anchors = d.find_all('a')
                        hrefs = [a['href'] for a in anchors]
                        for a in anchors:
                            h = a['href']
                            # Preuzimanje koordinata lokacije rada umetnika sa Wikidata stranice
                            responseLocation = requests.get('https://www.wikidata.org/' + h)
                            soupLocation = BeautifulSoup(responseLocation.content, 'html.parser')
                            div_root1Location = soupLocation.find('div', id='P625')
                            if div_root1Location:
                                div_root2Location = div_root1Location.find('div', class_='wikibase-statementview-mainsnak-container')
                                div_root3Location = div_root2Location.find('div', class_='wikibase-statementview-mainsnak')
                                div_root4Location = div_root3Location.find('div', class_='wikibase-kartographer-caption')
                                # Parsiranje koordinata
                                match = re.search(r'(\d+)°(\d+)\'([\d.]+)"(N|S),\s*(\d+)°(\d+)\'([\d.]+)"(E|W)', div_root4Location.text)
                                if match:
                                    lat_degrees, lat_minutes, lat_seconds, lat_direction, lon_degrees, lon_minutes, lon_seconds, lon_direction = match.groups()
                                    latitude = float(lat_degrees) + (float(lat_minutes) / 60) + (float(lat_seconds) / 3600)
                                    if lat_direction == 'S':
                                        latitude *= -1
                                    longitude = float(lon_degrees) + (float(lon_minutes) / 60) + (float(lon_seconds) / 3600)
                                    if lon_direction == 'W':
                                        longitude *= -1
                                    latitude = round(latitude, 7)
                                    longitude = round(longitude, 7)
                                    name = a.text
                                    museums.append({"type": "Feature", "geometry": {"type": "Point", "coordinates": [longitude, latitude]}, "properties": {"name": name}})
                                else:
                                    print("Koordinate nisu pronađene.")
        
        # Ažuriranje dokumenta u bazi sa informacijama o muzej
        collection.update_one({"_id": doc["_id"]}, {"$set": {"Museums": museums, "LivedPlaces": lived_places}})
    except requests.RequestException as e:
        print(f"Neuspelo preuzimanje {wikidata_url}: {e}")
    except Exception as e:
        print(f"Došlo je do greške prilikom obrade {wikidata_url}: {e}")


        