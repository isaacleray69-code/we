import asyncio
import threading
from flask import Flask, jsonify, request
from flask_cors import CORS
import aiohttp
from aiohttp_socks import ProxyConnector
import time
import random
import os
import resource  # Permet de surveiller l'utilisation de la RAM sur Linux/Render

app = Flask(__name__)
CORS(app)

# --- CONFIGURATION LIMITES DE SÉCURITÉ ---
MAX_ALLOWED_CONNECTIONS = 350  # Limite matérielle pour ne pas saturer 512Mo de RAM
RAM_LIMIT_MB = 460  # Seuil d'alerte RAM (on coupe à 460Mo pour garder une marge de sécurité)

test_en_cours = False
stats = {
    "succes": 0, 
    "bloque": 0, 
    "erreurs": 0, 
    "total_envoye": 0, 
    "vitesse": 0, 
    "latence_moyenne": 0
}

LISTE_PROXYS = []
stopped_by_safety = False

headers_globaux = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "fr,fr-FR;q=0.8,en-US;q=0.5,en;q=0.3",
    "Connection": "keep-alive"
}

# Un seul connecteur global standard pour éviter de recréer des objets lourds en mémoire
GLOBAL_CONNECTOR = aiohttp.TCPConnector(force_close=False, enable_cleanup_closed=True, limit=None)
SESSIONS_POOL = {}

def get_current_ram_usage():
    """Retourne l'utilisation actuelle de la RAM par ce script en Mo"""
    usage = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    # Sur Linux, ru_maxrss est en Ko. On convertit en Mo.
    return usage / 1024

async def get_session_for_proxy(proxy_url):
    global SESSIONS_POOL, headers_globaux, GLOBAL_CONNECTOR
    if proxy_url in SESSIONS_POOL:
        return SESSIONS_POOL[proxy_url]
    
    # On limite la création de sessions SOCKS5 uniques pour ne pas saturer la mémoire
    if proxy_url and proxy_url.startswith("socks5://"):
        if len(SESSIONS_POOL) > 50:  # Si trop de sessions de proxys ouvertes, on nettoie pour libérer la RAM
            for sess in list(SESSIONS_POOL.values()):
                await sess.close()
            SESSIONS_POOL.clear()
        
        connector = ProxyConnector.from_url(proxy_url, rdns=True)
        session = aiohttp.ClientSession(connector=connector, headers=headers_globaux)
        SESSIONS_POOL[proxy_url] = session
        return session
    else:
        # Pour les requêtes sans proxy ou HTTP standards, on utilise une session globale unique ultra-légère
        if "global" not in SESSIONS_POOL:
            SESSIONS_POOL["global"] = aiohttp.ClientSession(connector=GLOBAL_CONNECTOR, headers=headers_globaux)
        return SESSIONS_POOL["global"]

async def fetch(url, security_enabled):
    global stats, test_en_cours, stopped_by_safety, LISTE_PROXYS
    if not test_en_cours:
        return
    
    actuel_proxy = random.choice(LISTE_PROXYS) if LISTE_PROXYS else None
    start_req = time.time()
    try:
        session = await get_session_for_proxy(actuel_proxy)
        proxy_param = actuel_proxy if (actuel_proxy and not actuel_proxy.startswith("socks5://")) else None
        
        async with session.get(url, proxy=proxy_param, timeout=aiohttp.ClientTimeout(total=4)) as response:
            stats["total_envoye"] += 1
            latence = time.time() - start_req
            stats["latence_moyenne"] = (stats["latence_moyenne"] * 0.95) + (latence * 0.05)

            if response.status == 200:
                stats["succes"] += 1
            elif response.status in [429, 403, 503]:
                stats["bloque"] += 1
                
            if security_enabled and stats["latence_moyenne"] > 4.5:
                test_en_cours = False
                stopped_by_safety = True

    except Exception:
        stats["total_envoye"] += 1
        stats["erreurs"] += 1
        if security_enabled and stats["erreurs"] > 800 and stats["succes"] == 0:
            test_en_cours = False
            stopped_by_safety = True

async def worker(url, security_enabled):
    while test_en_cours:
        await fetch(url, security_enabled)
        await asyncio.sleep(0.005)  # Légèrement augmenté (0.005s) pour laisser respirer le CPU et la RAM de Render

async def start_async_test(url, max_connections, duration, security_enabled):
    global test_en_cours, stats, stopped_by_safety, SESSIONS_POOL
    test_en_cours = True
    stopped_by_safety = False
    stats = {"succes": 0, "bloque": 0, "erreurs": 0, "total_envoye": 0, "vitesse": 0, "latence_moyenne": 0}
    
    # Sécurité matérielle : On bride le nombre de tâches pour éviter l'explosion de la RAM
    connexions_bridées = min(int(max_connections), MAX_ALLOWED_CONNECTIONS)
    
    start_time = time.time()
    tasks = [asyncio.create_task(worker(url, security_enabled)) for i in range(connexions_bridées)]
    
    while time.time() - start_time < int(duration) and test_en_cours:
        await asyncio.sleep(0.5)
        elapsed = time.time() - start_time
        if elapsed > 0:
            stats["vitesse"] = int(stats["total_envoye"] / elapsed)
            
        # 🚨 VÉRIFICATION DE LA RAM EN TEMPS RÉEL
        if get_current_ram_usage() > RAM_LIMIT_MB:
            print(f"[ALERTE] Saturation RAM évitée ({get_current_ram_usage():.1f}Mo). Arrêt d'urgence.")
            test_en_cours = False
            stopped_by_safety = True
    
    test_en_cours = False
    for task in tasks:
        task.cancel()
        
    # Nettoyage agressif de la mémoire en fin de test
    for sess in list(SESSIONS_POOL.values()):
        await sess.close()
    SESSIONS_POOL.clear()

def run_async_loop(url, connections, duration, security_enabled):
    asyncio.run(start_async_test(url, connections, duration, security_enabled))

# --- ROUTES API ---

@app.route('/start', methods=['POST'])
def start():
    global LISTE_PROXYS, test_en_cours
    if test_en_cours:
        return jsonify({"status": "already_running"})
        
    data = request.json
    security_enabled = data.get('security', True)
    use_proxies = data.get('use_proxies', False)
    proxies_raw = data.get('proxies_raw', '')

    if use_proxies and proxies_raw.strip():
        LISTE_PROXYS = [line.strip() for line in proxies_raw.split('\n') if line.strip()]
    else:
        LISTE_PROXYS = []

    t = threading.Thread(target=run_async_loop, args=(data['url'], data['connections'], data['duration'], security_enabled))
    t.start()
    return jsonify({"status": "started"})

@app.route('/stop', methods=['POST'])
def stop():
    global test_en_cours
    test_en_cours = False
    return jsonify({"status": "stopped"})

@app.route('/stats')
def get_stats():
    # On ajoute la RAM actuelle dans les logs de la console Render pour surveiller
    current_ram = get_current_ram_usage()
    if current_ram > 400:
        print(f"[WARN] Utilisation RAM élevée : {current_ram:.1f} Mo")
        
    return jsonify({**stats, "test_en_cours": test_en_cours, "stopped_by_safety": stopped_by_safety})

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port, debug=False)
