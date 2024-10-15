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

# Iteracija kroz dokumente
for doc in documents:
    # Preuzimanje ULAN URL-a umetnika iz dokumenta
    ulan_url = doc.get('Artist ULAN URL')
    
    # Provera da li postoji validan ULAN URL
    if not ulan_url or ulan_url == "" or ulan_url == "(not assigned)":
        print(f"Preskačem dokument {doc['_id']} zbog praznog ili nevažećeg ULAN URL-a.")
        continue
    
    try:
        # Zahtevanje HTML stranice preko URL-a
        response = requests.get(ulan_url)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')

        # Inicijalizacija promenljivih za pol, mesto rođenja i mesto smrti
        gender = None
        birth_place = None
        death_place = None

        # Iteracija kroz span elemente sa klasom 'page'
        for span in soup.find_all('span', class_='page'):
            span_text = span.get_text()
            
            # Provera da li se u tekstu nalazi informacija o rođenju ili smrti umetnika
            if "Born:" in span_text or "Died:" in span_text:
                anchors = span.find_all('a')
                hrefs = [a['href'] for a in anchors]
                bornUrl = ''
                diedUrl = ''
                
                # Određivanje URL-a za mesto rođenja i/ili smrti
                if "Born:" in span_text and "Died:" not in span_text:
                    bornUrl = "https://www.getty.edu/vow/" + hrefs[0]
                elif "Died:" in span_text and "Born" not in span_text:
                    diedUrl = "https://www.getty.edu/vow/" + hrefs[0]
                else:
                    bornUrl = "https://www.getty.edu/vow/" + hrefs[0]
                    diedUrl = "https://www.getty.edu/vow/" + hrefs[1]

                # Ako postoji URL za mesto rođenja, preuzmi informacije o geografskoj širini i dužini
                if len(bornUrl) > 0:
                    responseBorn = requests.get(bornUrl)
                    soupBorn = BeautifulSoup(responseBorn.content, 'html.parser')

                    for span in soupBorn.find_all('span', class_='page'):
                        span_text = span.get_text()
                        if "Lat:" in span_text and "decimal degrees" in span_text: 
                            lat_match = re.search(r"Lat:\s*([\d.-]+)\s*decimal degrees", span_text)
                            if lat_match:
                                lat = float(lat_match.group(1))
                        if "Long:" in span_text and "decimal degrees" in span_text: 
                            lon_match = re.search(r"Long:\s*([\d.-]+)\s*decimal degrees", span_text)
                            if lon_match:
                                lon = float(lon_match.group(1))
                    birth_place = {"type": "Point", "coordinates": [lon, lat]}
                
                # Ako postoji URL za mesto smrti, preuzmi informacije o geografskoj širini i dužini
                if len(diedUrl) > 0:
                    responseDied = requests.get(diedUrl)
                    soupDied = BeautifulSoup(responseDied.content, 'html.parser')

                    for span in soupDied.find_all('span', class_='page'):
                        span_text = span.get_text()
                        if "Lat:" in span_text and "decimal degrees" in span_text: 
                            lat_match = re.search(r"Lat:\s*([\d.-]+)\s*decimal degrees", span_text)
                            if lat_match:
                                lat = float(lat_match.group(1))
                        if "Long:" in span_text and "decimal degrees" in span_text: 
                            lon_match = re.search(r"Long:\s*([\d.-]+)\s*decimal degrees", span_text)
                            if lon_match:
                                lon = float(lon_match.group(1))
                    death_place = {"type": "Point", "coordinates": [lon, lat]}
            
            # Preuzimanje informacija o polu umetnika
            if "Gender:" in span_text:
                gender_match = re.search(r"Gender:\s*(\w+)", span_text)
                if gender_match:
                    gender = gender_match.group(1)

        # Priprema podataka za ažuriranje u bazi
        update_fields = {}
        if gender  and not doc.get("Artist Gender") and gender == 'male':
            update_fields["Artist Gender"] = gender
        if birth_place:
            update_fields["BirthPlace"] = birth_place
        if death_place:
            update_fields["DeathPlace"] = death_place
        
        # Ažuriranje dokumenta u bazi
        if update_fields:
            collection.update_one({"_id": doc['_id']}, {"$set": update_fields})

    except requests.RequestException as e:
        print(f"Nije uspelo preuzimanje {ulan_url}: {e}")
    except Exception as e:
        print(f"Došlo je do greške prilikom obrade {ulan_url}: {e}")