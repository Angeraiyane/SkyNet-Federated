from neo4j import GraphDatabase
from kafka import KafkaConsumer
from geopy.geocoders import Nominatim
import json
import time

# --- CONFIGURATION ---
URI = "bolt://localhost:7687"
USER = "neo4j"
PASSWORD = "Ange2005"

geolocator = Nominatim(user_agent="skynet_expert_fst_bm")

consumer = KafkaConsumer(
    'global_model_results',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='latest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

class SkyNetGraph:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def get_country_name(self, lat, lon):
        try:
            time.sleep(0.7) # Un peu plus de pause pour éviter le ban de Nominatim
            location = geolocator.reverse(f"{lat}, {lon}", language='fr', timeout=5)
            if location and 'address' in location.raw:
                return location.raw['address'].get('country', 'Espace International')
            return "Espace Maritime"
        except:
            return f"Zone_{round(lat)}_{round(lon)}"

    def sync_data(self, client_id, traffic_map, weight):
        with self.driver.session() as session:
            # 1. On identifie le client (Beni Mellal ou Togo)
            session.run("MERGE (c:Client {id: $cid}) SET c.last_seen = datetime()", cid=client_id)
            
            for zone_id, count in traffic_map.items():
                lat, lon = map(float, zone_id.split('|'))
                country_name = self.get_country_name(lat, lon)
                
                # REQUÊTE CYPHER EXPERTE : Continent -> Pays -> Zone
                query = """
                MERGE (p:Country {name: $country})
                MERGE (z:Zone {id: $zone_id})
                SET z.passages = $count, z.lat = $lat, z.lon = $lon
                
                // On relie la zone au pays
                MERGE (z)-[:SITUÉ_EN]->(p)
                
                // On relie le client (FST_BM ou TOGO) à la zone qu'il a analysée
                WITH z
                MATCH (c:Client {id: $cid})
                MERGE (c)-[r:ANALYSE]->(z)
                SET r.nb_observations = $count, r.derniere_maj = datetime(), r.poids_ia_local = $weight
                """
                session.run(query, 
                            country=country_name, 
                            zone_id=zone_id, 
                            count=count, 
                            weight=weight, 
                            cid=client_id,
                            lat=lat,
                            lon=lon)

db = SkyNetGraph(URI, USER, PASSWORD)
print("🌍 Connecteur Géo-IA v2 (Expert) activé.")
print("📊 Analyse multi-continentale en cours de synchronisation...")

try:
    for message in consumer:
        data = message.value
        cid = data.get('last_client', 'FST_Beni_Mellal')
        
        # On synchronise le graphe
        db.sync_data(cid, data['traffic_map'], data['avg_weight'])
        print(f"✅ Graphe mis à jour par {cid} | Zones actives : {len(data['traffic_map'])}")
        
except Exception as e:
    print(f"❌ Erreur critique : {e}")
finally:
    db.driver.close()