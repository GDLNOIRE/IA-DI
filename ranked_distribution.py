from elasticsearch import Elasticsearch
import matplotlib.pyplot as plt
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()
elastic_user = os.getenv('ELASTIC_USER')
elastic_password = os.getenv('ELASTIC_PASSWORD')

es = Elasticsearch(
    hosts=[{'host': 'localhost', 'port': 9200, 'scheme': 'https'}],
    http_auth=(elastic_user, elastic_password),
    verify_certs=False  # Disable certificate verification
)
index_name = 'xml_data'

# Requête Elasticsearch pour récupérer les données des flux
query = {
    "size": 10000,
    "query": {
        "match_all": {}  # Vous pouvez personnaliser cette requête selon vos besoins
    }
}

# Exécutez la requête Elasticsearch
results = es.search(index=index_name, body=query)

# Analysez les données pour obtenir le nombre de paquets dans chaque flux
flows = results['hits']['hits']
packet_counts = [int(flow['_source']['totalDestinationPackets']) for flow in flows]

# Triez les flux en fonction du nombre de paquets
flows_sorted = sorted(flows, key=lambda x: int(x['_source']['totalDestinationPackets']), reverse=True)
packet_counts_sorted = sorted(packet_counts, reverse=True)

# Créez un graphique de distribution avec l'axe des x pour le nombre de paquets et l'axe des y pour le nombre de flux
plt.figure(figsize=(10, 6))
plt.subplot(121)
plt.bar(packet_counts_sorted, range(1, len(flows_sorted) + 1))
plt.xlabel("Packets")
plt.ylabel("Flows")
plt.title("Distribution Packets vs. Flows (Standard Axes)")

# Créez un graphique de distribution avec des axes log-log
plt.subplot(122)
plt.loglog(packet_counts_sorted, range(1, len(flows_sorted) + 1))
plt.xlabel("Packets (log scale)")
plt.ylabel("Flows (log scale)")
plt.title("Distribution Packets vs. Flows (Log-Log Axes)")

plt.tight_layout()
plt.show()
