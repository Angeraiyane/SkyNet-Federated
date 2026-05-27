from kafka import KafkaConsumer, KafkaProducer
import json
import time
import os
from datetime import datetime, timedelta

# --- 1. CONFIGURATION ---
consumer = KafkaConsumer(
    'model_updates',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='latest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

TOPIC_DASHBOARD = 'global_model_results'
HISTORY_FILE = "history_global.json"

# --- 2. INITIALISATION DES VARIABLES ---
global_weights = []
global_bias = []
global_traffic_map = {}
# Structure pour stocker par date/heure ET par client : {"Date|Heure": {"Client1": 10, "Client2": 20}}
detailed_history = {} 
update_count = 0

print("\n" + "="*60)
print("🏛️  SKYNET EXPERT : SERVEUR D'AGRÉGATION FÉDÉRÉE")
print("📡 Surveillance en temps réel et Analyse J vs J-1")
print("="*60)

# Chargement de l'historique global (somme finale) pour la précision
if os.path.exists(HISTORY_FILE):
    try:
        with open(HISTORY_FILE, 'r') as f:
            # On charge l'historique des totaux pour J-1
            global_totals_db = json.load(f)
        print("✅ Base historique J-1 chargée.")
    except:
        global_totals_db = {}
else:
    global_totals_db = {}

print("-" * 60)

try:
    for message in consumer:
        data = message.value
        client_id = data.get('client_id', 'Inconnu')
        
        # A. Extraction des données
        w = data['weights'][0]
        b = data['bias'][0]
        client_traffic = data.get('traffic_map', {})
        client_time_series = data.get('time_series', {}) 
        
        today_str = datetime.now().strftime('%Y-%m-%d')
        yesterday_str = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        
        # --- B. AGRÉGATION IA ---
        update_count += 1
        global_weights.append(w)
        global_bias.append(b)
        
        avg_w = sum(global_weights) / len(global_weights)
        avg_b = sum(global_bias) / len(global_bias)

        # --- C. MISE À JOUR DE LA CARTE GLOBALE (Zones Actives) ---
        # On additionne les passages pour chaque zone géographique
        for zone, count in client_traffic.items():
            global_traffic_map[zone] = global_traffic_map.get(zone, 0) + count

        # --- D. CUMUL PAR HEURE (FST + TOGO) ---
        for hour, count in client_time_series.items():
            time_key = f"{today_str}|{hour}"
            
            if time_key not in detailed_history:
                detailed_history[time_key] = {}
            
            detailed_history[time_key][client_id] = count

        # Calcul du TOTAL réel pour l'heure actuelle
        current_hour = list(client_time_series.keys())[-1]
        current_key = f"{today_str}|{current_hour}"
        total_vols_heure = sum(detailed_history[current_key].values())

        # --- E. CALCUL PRÉCISION (Somme J vs Somme J-1) ---
        precision_score = 0.0
        status_msg = "⏳ Apprentissage..."
        
        key_yesterday = f"{yesterday_str}|{current_hour}"
        if key_yesterday in global_totals_db:
            val_yesterday = global_totals_db[key_yesterday]
            error = abs(total_vols_heure - val_yesterday) / max(total_vols_heure, 1)
            precision_score = max(0, (1 - error) * 100)
            status_msg = f"📅 Comparaison avec le {yesterday_str}"

        # --- F. SAUVEGARDE ET ENVOI ---
        # Mise à jour de la base historique avec le nouveau total calculé
        global_totals_db[current_key] = total_vols_heure
        with open(HISTORY_FILE, 'w') as f:
            json.dump(global_totals_db, f)

        # Filtrage des stats temporelles pour aujourd'hui uniquement
        today_stats = {k.split('|')[1]: v for k, v in global_totals_db.items() if k.startswith(today_str)}

        # Préparation du payload pour le Dashboard
        dashboard_payload = {
            "avg_weight": avg_w,
            "avg_bias": avg_b,
            "traffic_map": global_traffic_map, # Correction : On envoie la map mise à jour
            "global_time_stats": today_stats,
            "precision_score": round(precision_score, 2),
            "update_count": update_count,
            "last_client": client_id,
            "timestamp": time.time()
        }
        
        producer.send(TOPIC_DASHBOARD, value=dashboard_payload)

        # AFFICHAGE TERMINAL EXPERT
        print(f"📥 Mise à jour de : {client_id}")
        print(f"📈 Modèle Global : Y = ({avg_w:.2e} * X) + {avg_b:.2f}")
        print(f"🌍 Zones actives détectées : {len(global_traffic_map)}")
        print(f"📊 TOTAL CUMULÉ à {current_hour}h : {total_vols_heure} avions")
        print(f"📡 {status_msg} | 🎯 Score : {precision_score:.2f}%")
        print("-" * 60)

except KeyboardInterrupt:
    print("\n🛑 Arrêt du serveur.")
finally:
    consumer.close()
    producer.close()