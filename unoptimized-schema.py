import csv
from pymongo import MongoClient

# Connect to MongoDB
client = MongoClient('mongodb://localhost:27017/')
db = client['museum_database']

# Create collections
artists_collection = db['artistsV1']
objects_collection = db['objectsV1']

# Delete all documents from artists collection
artists_collection.delete_many({})

# Delete all documents from objects collection
objects_collection.delete_many({})

# Function to parse artist data and insert into MongoDB
def process_artist(row):

    if row['Constituent ID']:  # Check if Constituent ID is not null
        
        constituent_id = row['Constituent ID']
        artist_roles = row['Artist Role'].split('|')
        artist_prefixes = row['Artist Prefix'].split('|')
        artist_display_names = row['Artist Display Name'].split('|')
        artist_display_bios = row['Artist Display Bio'].split('|')
        artist_suffixes = row['Artist Suffix'].split('|')
        artist_alpha_sorts = row['Artist Alpha Sort'].split('|')
        artist_nationalities = row['Artist Nationality'].split('|')
        artist_begin_dates = row['Artist Begin Date'].split('|')
        artist_end_dates = row['Artist End Date'].split('|')
        artist_genders = row['Artist Gender'].split('|')
        artist_ulan_urls = row['Artist ULAN URL'].split('|')
        artist_wikidata_urls = row['Artist Wikidata URL'].split('|')
        
        # Iterate over each set of artist data and append to the list
        for role, prefix, display_name, display_bio, suffix, alpha_sort, nationality, begin_date, end_date, gender, ulan_url, wikidata_url in zip(
                artist_roles, artist_prefixes, artist_display_names, artist_display_bios, artist_suffixes, artist_alpha_sorts,
                artist_nationalities, artist_begin_dates, artist_end_dates, artist_genders, artist_ulan_urls, artist_wikidata_urls
            ):
            artist = {
                'Constituent ID' : constituent_id,
                'Artist Role': role.strip(),
                'Artist Prefix': prefix.strip(),
                'Artist Display Name': display_name.strip(),
                'Artist Display Bio': display_bio.strip(),
                'Artist Suffix': suffix.strip(),
                'Artist Alpha Sort': alpha_sort.strip(),
                'Artist Nationality': nationality.strip(),
                'Artist Begin Date': begin_date.strip(),
                'Artist End Date': end_date.strip(),
                'Artist Gender': gender.strip(),
                'Artist ULAN URL': ulan_url.strip(),
                'Artist Wikidata URL': wikidata_url.strip()
            }
            #artists.append(artist)
            # Check if artist with the same Constituent ID already exists
            # Check if artist with the same name already exists in the batch
            existing_artist = artists_collection.find_one({'Artist Display Name': display_name.strip(),
                                                           'Artist Begin Date': begin_date.strip()})
            
            # Insert artist data only if the artist doesn't already exist
            if not existing_artist:
            # Insert artist data into MongoDB
                artists_collection.insert_one(artist)
            else:
                existing_roles = existing_artist.get('Artist Role', [])
                if not isinstance(existing_roles, list):
                    existing_roles = [existing_roles]  # Convert to list if it's a single role (string)
                if role.strip() and role not in existing_roles:
                    existing_roles.append(role)
                    artists_collection.update_one({'_id': existing_artist['_id']}, {'$set': {'Artist Role': existing_roles}})

                existing_prefix = existing_artist.get('Artist Prefix', [])
                if not isinstance(existing_prefix, list):
                    existing_prefix = [existing_prefix]  # Convert to list if it's a single role (string)
                    existing_prefix = list(filter(None, existing_prefix))
                if prefix.strip() and prefix not in existing_prefix:
                    existing_prefix.append(prefix)
                    artists_collection.update_one({'_id': existing_artist['_id']}, {'$set': {'Artist Prefix': existing_prefix}})

# Read data from CSV file and insert into MongoDB
with open('MetObjects.txt', 'r', encoding='utf-8') as file:
    reader = csv.DictReader(file)
    count = 0  # Counter to track the number of rows processed
    #batch_umetnika = []
    batch_objekata = []
    for row in reader:
        #if count >= 20000:     
        #break  # Exit loop after processing 10 rows
        # Extract artist data
        artist_data = {
            key: row[key] for key in row.keys() & {
                'Constituent ID', 'Artist Role', 'Artist Prefix',
                'Artist Display Name', 'Artist Display Bio', 'Artist Suffix',
                'Artist Alpha Sort', 'Artist Nationality', 'Artist Begin Date',
                'Artist End Date', 'Artist Gender', 'Artist ULAN URL',
                'Artist Wikidata URL'
            }
        }
        # Print artist data
        # print("Artist data:", artist_data)
        # Insert artist data into MongoDB
        process_artist(artist_data)

        # Add check and split for fields with multiple values
        for field in ['Geography Type', 'City', 'State', 'County', 'Country', 'Region', 'Subregion', 'Locale', 'Locus', 'Excavation', 'River']:
            if '|' in row[field]:
                row[field] = row[field].split('|')
            else:
                if row[field]:  # Check if the field is not empty
                    row[field] = row[field]  # Keep it as a single value
                else:
                    row[field] = ""  # Or any other default value you prefer
        
        artist_display_names = row.get('Artist Display Name', '').split('|')
        artist_begin_dates = row.get('Artist Begin Date', '').split('|')

        artists = []
        for display_name, begin_date in zip(artist_display_names, artist_begin_dates):
            if display_name != "":
                # napisati proveru da l u ovom nizu vec postoji kombinacija, ako postoji tada ga ne dodati'
                artist = {'Artist Display Name': display_name.strip()}
                artist['Artist Begin Date'] = begin_date.strip()
                # artist_tuple = (display_name.strip(), begin_date.strip())
                if artist not in artists:
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

        # Replace "en dash" character with hyphen in artist dates
        #row['Artist Begin Date'] = row.get('Artist Begin Date', '').replace('–', '-')
        #row['Artist End Date'] = row.get('Artist End Date', '').replace('–', '-')

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

        # Increment the counter
        #count += 1
        # Print remaining object data
        #print("Object data:", row)
        # Insert remaining object data into MongoDB
        batch_objekata.append(row)

        if len(batch_objekata) >= 10000:
                objects_collection.insert_many(batch_objekata)
                batch_objekata.clear()

    objects_collection.insert_many(batch_objekata)
    