import time
import json
import requests
from datetime import datetime
from kafka import KafkaProducer

# 1. Configuration du Producteur
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

TOPIC_NAME = 'flight_data_raw'

# Mapping complet pour l'innovation par continent
def get_continent(country):
    # Les noms correspondent aux formats standards retournés par l'API OpenSky
    africa = {
        "Algeria", "Angola", "Benin", "Botswana", "Burkina Faso", "Burundi", "Cabo Verde", "Cameroon", "Central African Republic",
        "Chad", "Comoros", "Congo", "Congo, Democratic Republic of the", "Cote d'Ivoire", "Djibouti", "Egypt", "Equatorial Guinea",
        "Eritrea", "Eswatini", "Ethiopia", "Gabon", "Gambia", "Ghana", "Guinea", "Guinea-Bissau", "Kenya", "Lesotho", "Liberia",
        "Libya", "Madagascar", "Malawi", "Mali", "Mauritania", "Mauritius", "Morocco", "Mozambique", "Namibia", "Niger", "Nigeria",
        "Rwanda", "Sao Tome and Principe", "Senegal", "Seychelles", "Sierra Leone", "Somalia", "South Africa", "South Sudan",
        "Sudan", "Tanzania, United Republic of", "Togo", "Tunisia", "Uganda", "Zambia", "Zimbabwe"
    }
    
    asia = {
        "Afghanistan", "Armenia", "Azerbaijan", "Bahrain", "Bangladesh", "Bhutan", "Brunei", "Cambodia", "China", "Cyprus",
        "Georgia", "India", "Indonesia", "Iran, Islamic Republic of", "Iraq", "Israel", "Japan", "Jordan", "Kazakhstan",
        "Kuwait", "Kyrgyzstan", "Lao People's Democratic Republic", "Lebanon", "Malaysia", "Maldives", "Mongolia", "Myanmar",
        "Nepal", "Oman", "Pakistan", "Palestine, State of", "Philippines", "Qatar", "Saudi Arabia", "Singapore", "Korea, Republic of",
        "Korea, Democratic People's Republic of", "Sri Lanka", "Syrian Arab Republic", "Taiwan", "Tajikistan", "Thailand",
        "Timor-Leste", "Turkey", "Turkmenistan", "United Arab Emirates", "Uzbekistan", "Viet Nam", "Yemen"
    }

    europe = {
        "Albania", "Andorra", "Austria", "Belarus", "Belgium", "Bosnia and Herzegovina", "Bulgaria", "Croatia", "Czech Republic",
        "Denmark", "Estonia", "Finland", "France", "Germany", "Greece", "Hungary", "Iceland", "Ireland", "Italy", "Latvia",
        "Liechtenstein", "Lithuania", "Luxembourg", "Malta", "Moldova, Republic of", "Monaco", "Montenegro", "Netherlands",
        "North Macedonia", "Norway", "Poland", "Portugal", "Romania", "Russian Federation", "San Marino", "Serbia", "Slovakia",
        "Slovenia", "Spain", "Sweden", "Switzerland", "Ukraine", "United Kingdom", "Holy See (Vatican City State)"
    }

    americas = {
        "Antigua and Barbuda", "Argentina", "Bahamas", "Barbados", "Belize", "Bolivia", "Brazil", "Canada", "Chile", "Colombia",
        "Costa Rica", "Cuba", "Dominica", "Dominican Republic", "Ecuador", "El Salvador", "Grenada", "Guatemala", "Guyana",
        "Haiti", "Honduras", "Jamaica", "Mexico", "Nicaragua", "Panama", "Paraguay", "Peru", "Saint Kitts and Nevis", "Saint Lucia",
        "Saint Vincent and the Grenadines", "Suriname", "Trinidad and Tobago", "United States", "Uruguay", "Venezuela"
    }

    oceania = {
        "Australia", "Fiji", "Kiribati", "Marshall Islands", "Micronesia, Federated States of", "Nauru", "New Zealand", "Palau",
        "Papua New Guinea", "Samoa", "Solomon Islands", "Tonga", "Tuvalu", "Vanuatu"
    }

    if country in africa: return "AFRICA"
    if country in asia: return "ASIA"
    if country in europe: return "EUROPE"
    if country in americas: return "AMERICAS"
    if country in oceania: return "OCEANIA"
    return "OTHER"

def fetch_flights():
    url = "https://opensky-network.org/api/states/all"
    try:
        response = requests.get(url, timeout=15)
        if response.status_code == 429:
            print("⚠️ Rate Limiting API (Trop de requêtes)... Attente prolongée nécessaire.")
            return None
        if response.status_code == 200:
            return response.json().get('states', [])
        return None
    except Exception as e:
        print(f"❌ Erreur : {e}")
        return None

print(f"📡 SkyNet-Federated : Ingestion Multi-Continentale lancée sur {TOPIC_NAME}...")

try:
    while True:
        states = fetch_flights()
        if states:
            count = 0
            now = datetime.now()
            
            # On traite les 100 premiers vols pour assurer une couverture géographique
            for f in states[:100]:
                # f[5] = longitude, f[6] = latitude
                if f[5] is not None and f[6] is not None:
                    country = f[2].strip()
                    continent = get_continent(country)
                    
                    message = {
                        "icao24": f[0],
                        "callsign": f[1].strip() if f[1] else "N/A",
                        "origin_country": country,
                        "continent": continent,
                        "longitude": f[5],
                        "latitude": f[6],
                        "altitude": f[7] if f[7] else 0,
                        "velocity": f[9] if f[9] else 0,
                        "timestamp": time.time(),
                        "hour": now.hour,
                        "day_of_week": now.strftime('%A')
                    }
                    producer.send(TOPIC_NAME, value=message)
                    count += 1
            
            print(f"✅ {count} vols envoyés à {now.strftime('%H:%M:%S')}")
        
        #Attente de 2 minutes pour que Open Sky ne me blqoue pas
        print("⏳ Attente de 2 minutes pour la prochaine capture...")
        time.sleep(120)

except KeyboardInterrupt:
    print("\n🛑 Arrêt propre du producteur.")
finally:
    producer.close()