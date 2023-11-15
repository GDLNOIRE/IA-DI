import warnings
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
import lxml.etree as ET
import urllib3
from dotenv import load_dotenv
import os

# Load environment variables
elastic_user = "elastic"
elastic_password = "2Tt_dVh8htQwAtlz5-7I"

urllib3.disable_warnings(urllib3.exceptions.SecurityWarning)
warnings.filterwarnings(action='ignore', message='.Using TLS.', category=Warning)

# Créez une instance d'Elasticsearch avec un pool de connexions
es = Elasticsearch(
    hosts=[{'host': 'localhost', 'port': 9200, 'scheme': 'https'}],
    basic_auth=(elastic_user, elastic_password),
    verify_certs=False,
    connections_per_node=64
)

# Fonction pour lire un fichier XML et convertir son contenu en une liste de dictionnaires
def parse_xml_file(xml_file):
    flow_list = []
    tree = ET.parse(xml_file)
    root = tree.getroot()
    for flow_elem in root:
        flow_dict = {}
        filename = os.path.basename(xml_file)
        filename_without_extension = os.path.splitext(filename)[0]
        flow_dict['origin'] = filename_without_extension
        for elem in flow_elem:
            flow_dict[elem.tag] = elem.text
        flow_list.append(flow_dict)
    return flow_list

# Liste des noms de fichiers XML
xml_files = [
    "TestbedSatJun12Flows.xml",
    "TestbedSunJun13Flows.xml",
    "TestbedMonJun14Flows.xml",
    "TestbedTueJun15-1Flows.xml",
    "TestbedTueJun15-2Flows.xml",
    "TestbedWedJun16-1Flows.xml",
    "TestbedThuJun17-1bisFlows.xml",
    "TestbedWedJun16-2Flows.xml",
    "TestbedThuJun17-2Flows.xml"
]

# Créez une liste des dictionnaires à indexer
bulk_data = []

es.indices.delete(index='xml_data', ignore=[400, 404])
for xml_file in xml_files:
    print(f"Parsing {xml_file}")

    xml_filepath = 'TRAIN_ENSIBS/' + xml_file
    flow_list = parse_xml_file(xml_filepath)
    for flow_dict in flow_list:
        bulk_data.append({
            "_index": "xml_data",
            "_source": flow_dict
        })

# Utilisez la fonction bulk pour indexer les données
print("Indexing data...")
success, failed = bulk(es, bulk_data)

# Vérifiez les résultats
print(f"Documents indexés avec succès : {success}")
print(f"Échecs d'indexation : {failed}")
