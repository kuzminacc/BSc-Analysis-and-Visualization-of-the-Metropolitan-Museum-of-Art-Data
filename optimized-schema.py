import csv
from pymongo import MongoClient

# Connect to MongoDB
client = MongoClient('mongodb://localhost:27017/')
db = client['museum_database']

# Create collections
artists_collection = db['artists']
objects_collection = db['objects']

# Delete all documents from artists collection
artists_collection.delete_many({})

# Delete all documents from objects collection
objects_collection.delete_many({})

# Read data from CSV file and insert into MongoDB
with open('MetObjects.txt', 'r', encoding='utf-8') as file:
    reader = csv.DictReader(file)
    count = 0  # Counter to track the number of rows processed
    objectsForInsert = []
    for row in reader:
        
        # Add check and split for fields with multiple values
        for field in ['Geography Type', 'City', 'State', 'County', 'Country', 'Region', 'Subregion', 'Locale', 'Locus', 'Excavation', 'River']:
            if '|' in row[field]:
                row[field] = row[field].split('|')
            else:
                if row[field]:  # Check if the field is not empty
                    row[field] = row[field]  # Keep it as a single value
                else:
                    row[field] = ""  # Or any other default value you prefer

        artist_roles = row.get('Artist Role', '').split('|')
        artist_display_names = row.get('Artist Display Name', '').split('|')
        artist_nationalities = row.get('Artist Nationality', '').split('|')
        artist_begin_dates = row.get('Artist Begin Date', '').split('|')
        artist_end_dates = row.get('Artist End Date', '').split('|')
        artist_genders = row.get('Artist Gender', '').split('|')
        artist_wikis = row.get('Artist Wikidata URL', '').split('|')
        artist_ulans = row.get('Artist ULAN URL', '').split('|')

        #niz rola da bude i da bude u listi artista unique po imenu i begin date,ako ima vise onda  dodati samo role ako se razlikuje
        artists = []
        for role, display_name, nationality, begin_date, end_date, gender, wiki, ulan in zip(
            artist_roles, artist_display_names, artist_nationalities,
            artist_begin_dates, artist_end_dates, artist_genders, artist_wikis, artist_ulans
        ):
            
            if display_name:

                artist_found = False
                for artist in artists:
                    if artist['Artist Display Name'] == display_name.strip() and artist['Artist Begin Date'] == begin_date.strip():
                        if role and role.strip() not in artist['Artist Role']:
                            artist['Artist Role'].append(role.strip())
                        artist_found = True
                        break

                if not artist_found:
                    artist = {'Artist Display Name': display_name.strip()}
                    if role:
                        artist['Artist Role'] = [role.strip()]
                    if nationality:
                        artist['Artist Nationality'] = nationality.strip()
                    if begin_date:
                        artist['Artist Begin Date'] = begin_date.strip()
                    if end_date:
                        artist['Artist End Date'] = end_date.strip()
                    if gender:
                        artist['Artist Gender'] = gender.strip()
                    if wiki:
                        artist['Artist Wikidata URL'] = wiki.strip()
                    if ulan:
                        artist['Artist ULAN URL'] = ulan.strip()
                    artists.append(artist)

        row['Artist'] = artists

        # Remove artist data from object data
        del row['Constituent ID']
        del row['Artist Role']
        del row['Artist Prefix']
        del row['Artist Display Name']
        del row['Artist Display Bio']
        del row['Artist Suffix']
        del row['Artist Alpha Sort']
        del row['Artist Nationality']
        del row['Artist Begin Date']
        del row['Artist End Date']
        del row['Artist Gender']
        del row['Artist ULAN URL']
        del row['Artist Wikidata URL']

        # Parse Tags, Tags AAT URL, and Tags Wikidata URL
        tags = row.get('Tags', '').split('|')
        aat_urls = row.get('Tags AAT URL', '').split('|')
        wikidata_urls = row.get('Tags Wikidata URL', '').split('|')

        # Create a list of dictionaries containing tags and their URLs
        tags_with_urls = []
        for tag, aat_url, wikidata_url in zip(tags, aat_urls, wikidata_urls):
            if tag:
                tag_dict = {'tag': tag}
                if aat_url:
                    tag_dict['aat_url'] = aat_url
                if wikidata_url:
                    tag_dict['wikidata_url'] = wikidata_url
                tags_with_urls.append(tag_dict)

        # Remove Tags, Tags AAT URL, and Tags Wikidata URL from row dictionary
        del row['Tags']
        del row['Tags AAT URL']
        del row['Tags Wikidata URL']

        # Add the list of tags with URLs to the row dictionary
        row['Tags'] = tags_with_urls

        # Insert remaining object data into MongoDB
        objectsForInsert.append(row)
        if len(objectsForInsert) >= 10000:
                objects_collection.insert_many(objectsForInsert)
                objectsForInsert.clear()

    objects_collection.insert_many(objectsForInsert)
