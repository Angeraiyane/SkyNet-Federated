from kafka import KafkaConsumer, KafkaProducer
import json
import numpy as np
import time
import argparse
from sklearn.linear_model import SGDRegressor
from sklearn.preprocessing import StandardScaler
from datetime import datetime

# --- 0. GESTION DES ARGUMENTS (Multi-Client) ---
parser = argparse.ArgumentParser()
parser.add_argument('--name', type=str, default='FST_BM', help='Nom du client')
parser.add_argument('--continents', nargs='+', default=['AFRICA', 'ASIA'], help='Continents à traiter')
args = parser.parse_args()

# --- 1. INITIALISATION DE L'IA ---
model = SGDRegressor()
scaler = StandardScaler()
is_scaler_ready = False

# --- 2. CONFIGURATION KAFKA ---
consumer = KafkaConsumer(
    'flight_data_raw',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='latest', # On prend le flux en direct
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    group_id=f'group_{args.name}' # Groupe unique par client
)

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# --- 3. VARIABLES DE TRAITEMENT & ANALYTICS ---
buffer_x = []
buffer_y = []
traffic_density = {}
time_stats = {} # Pour stocker la fréquence par heure : {"14h": 120}

print(f"🚀 Client IA [{args.name}] lancé pour les continents : {args.continents}")

try:
    for message in consumer:
        flight = message.value
        
        # INNOVATION : Filtrage par Continent
        if flight.get('continent') not in args.continents:
            continue

        alt = flight.get('altitude')
        vel = flight.get('velocity')
        lat = flight.get('latitude')
        lon = flight.get('longitude')
        hour = str(flight.get('hour', 0))

        # 📊 ANALYTICS : Fréquence Temporelle (Pour tes courbes)
        time_stats[hour] = time_stats.get(hour, 0) + 1

        # 🌍 SPATIAL : Densité (Pour Neo4j)
        if lat is not None and lon is not None:
            zone_key = f"{round(lat, 1)}|{round(lon, 1)}"
            traffic_density[zone_key] = traffic_density.get(zone_key, 0) + 1

        # 🧠 MACHINE LEARNING : Apprentissage
        if alt is not None and vel is not None:
            buffer_x.append([alt])
            buffer_y.append(vel)

            if len(buffer_x) >= 20:
                X_raw = np.array(buffer_x)
                y_train = np.array(buffer_y)

                if not is_scaler_ready:
                    scaler.fit(X_raw)
                    is_scaler_ready = True
                
                X_scaled = scaler.transform(X_raw)
                model.partial_fit(X_scaled, y_train)

                # ENVOI AU SERVEUR (Inclut les nouvelles stats)
                update_payload = {
                    "client_id": args.name,
                    "weights": model.coef_.tolist(),
                    "bias": model.intercept_.tolist(),
                    "traffic_map": traffic_density,
                    "time_series": time_stats, # Nouvelle donnée pour tes courbes
                    "timestamp": time.time()
                }

                producer.send('model_updates', value=update_payload)
                print(f"📈 [{args.name}] Poids : {model.coef_[0]:.4f} | Vols traités cette heure ({hour}h) : {time_stats[hour]}")
                
                # Sauvegarde locale pour le Backtesting (J vs J-1)
                with open(f'stats_{args.name}.json', 'w') as f:
                    json.dump(time_stats, f)
                
                buffer_x = []
                buffer_y = []

except KeyboardInterrupt:
    print(f"\n🛑 Arrêt du client {args.name}.")
finally:
    consumer.close()
    producer.close()