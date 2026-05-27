import streamlit as st
import pandas as pd
import time
from kafka import KafkaConsumer
import json
import plotly.express as px

# --- CONFIGURATION DE LA PAGE ---
st.set_page_config(page_title="SkyNet Expert - Analytics Overview", layout="wide")
st.title("✈️ SkyNet Expert : Analytics & Federated Intelligence")

# --- 1. CONNEXIONS KAFKA ---
@st.cache_resource
def get_consumers():
    # Flux brut pour les positions réelles
    c_raw = KafkaConsumer(
        'flight_data_raw',
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='latest',
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        consumer_timeout_ms=500
    )
    # Flux agrégé pour les poids de l'IA et les statistiques
    c_res = KafkaConsumer(
        'global_model_results',
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='latest',
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        consumer_timeout_ms=500
    )
    return c_raw, c_res

try:
    c_raw, c_res = get_consumers()
except Exception as e:
    st.error(f"❌ Erreur de connexion Kafka : {e}")
    st.stop()

# --- 2. GESTION DE LA MÉMOIRE (SESSION STATE) ---
if 'history' not in st.session_state:
    st.session_state.history = []
if 'expert_data' not in st.session_state:
    st.session_state.expert_data = {
        "w": 0, "b": 0, "map": {}, "time_series": {}, "precision": 100.0, "last_client": "N/A"
    }

# --- 3. RÉCUPÉRATION DES DONNÉES EN TEMPS RÉEL ---
# 3.1 Lecture des avions réels (Points rouges)
raw_messages = c_raw.poll(timeout_ms=300)
for tp, msgs in raw_messages.items():
    for m in msgs:
        st.session_state.history.append(m.value)
st.session_state.history = st.session_state.history[-50:] # On garde les 50 derniers pour la fluidité

# 3.2 Lecture des résultats IA et Analytics (Points bleus + Graphiques)
res_messages = c_res.poll(timeout_ms=300)
for tp, msgs in res_messages.items():
    for m in msgs:
        d = m.value
        st.session_state.expert_data.update({
            "w": d['avg_weight'],
            "b": d['avg_bias'],
            "map": d['traffic_map'],
            "time_series": d.get('global_time_stats', {}),
            "precision": d.get('precision_score', 100.0),
            "last_client": d.get('last_client', "Inconnu")
        })

# --- 4. AFFICHAGE DES MÉTRIQUES (Innovation J vs J-1) ---
m1, m2, m3, m4 = st.columns(4)
m1.metric("🌍 Zones Actives", len(st.session_state.expert_data['map']))
m2.metric("🎯 Précision J vs J-1", f"{st.session_state.expert_data['precision']}%")
m3.metric("📡 Dernier Client", st.session_state.expert_data['last_client'])
m4.metric("📈 Poids IA (w)", f"{st.session_state.expert_data['w']:.4f}")

st.divider()

# --- 5. ANALYTICS : FRÉQUENCE ET RÉPARTITION ---
col_left, col_right = st.columns([2, 1])

with col_left:
    st.subheader("📊 Fréquence du Trafic Global (Analyse Temporelle)")
    if st.session_state.expert_data['time_series']:
        # 1. On crée le DataFrame
        df_time = pd.DataFrame(list(st.session_state.expert_data['time_series'].items()), columns=['Heure', 'Vols'])
        
        # 2. LA CORRECTION : On convertit la colonne Heure en nombre
        df_time['Heure'] = pd.to_numeric(df_time['Heure'])
        
        # 3. On trie par heure (maintenant c'est 9, 10, 11...)
        df_time = df_time.sort_values('Heure')
        
        st.line_chart(df_time.set_index('Heure'), color="#29b5e8")
    else:
        st.info("📊 En attente de statistiques temporelles...")

with col_right:
    st.subheader("🗺️ Origine par Pays")
    if st.session_state.history:
        df_countries = pd.DataFrame(st.session_state.history)
        fig = px.pie(df_countries, names='origin_country', hole=0.4, color_discrete_sequence=px.colors.qualitative.Pastel)
        st.plotly_chart(fig, use_container_width=True)

# --- 6. CARTE : RÉEL (Rouge) vs PRÉDICTION 10 MIN (Bleu) ---
st.subheader("🛰️ Radar : Positions Réelles vs Prédictions IA ($y = wx + b$)")

# Préparation des points réels
df_real = pd.DataFrame([
    {"lat": f['latitude'], "lon": f['longitude'], "type": "Réel", "color": "#FF0000"} 
    for f in st.session_state.history if f.get('latitude') is not None
])

# Calcul des prédictions basées sur les poids de l'IA
df_pred_points = []
current_w = st.session_state.expert_data['w']
current_b = st.session_state.expert_data['b']

if current_w != 0 and st.session_state.history:
    for f in st.session_state.history[-20:]: # Prédire pour les 20 derniers avions
        # Calcul de la vitesse prédite via le modèle SGD normalisé
        # On simule ici l'application de l'équation apprise
        pred_vel = (current_w * 0.5) + current_b 
        
        # Projection géographique (environ 10 minutes de vol)
        df_pred_points.append({
            "lat": f['latitude'],
            "lon": f['longitude'] + (pred_vel * 600 / 111000), # 600s = 10min
            "type": "Prédiction",
            "color": "#0000FF"
        })

# Fusion et affichage sur la carte
if not df_real.empty:
    if df_pred_points:
        df_total = pd.concat([df_real, pd.DataFrame(df_pred_points)])
    else:
        df_total = df_real
    st.map(df_total, color="color", size=10)
else:
    st.info("📡 Radar en recherche de signal... Lance tes Producers et Clients !")

# Rafraîchissement automatique
time.sleep(3)
st.rerun()