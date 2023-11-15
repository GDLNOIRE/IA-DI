from flask import Flask, jsonify, request
from elasticsearch import Elasticsearch
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()
elastic_user = os.getenv('ELASTIC_USER')
elastic_password = os.getenv('ELASTIC_PASSWORD')

app = Flask(__name__)
es = Elasticsearch(
    hosts=[{'host': 'localhost', 'port': 9200, 'scheme': 'https'}],
    http_auth=(elastic_user, elastic_password),
    verify_certs=False  # Disable certificate verification
)
index_name = 'xml_data'

# Function to get the list of distinct protocols
@app.route('/protocols', methods=['GET'])
def get_distinct_protocols():
    response = es.search(index=index_name, aggs={'protocols': {'terms': {'field': 'protocolName.keyword'}}})
    protocols = [bucket['key'] for bucket in response['aggregations']['protocols']['buckets']]
    return jsonify(protocols)
@app.route('/matchAll', methods=['GET'])
def matchAll():
    body = {
        "query": {
            "match_all": {
                
            }
        },
        "size":1000
    }
    response = es.search(index=index_name, body=body)
    flows = [hit['_source'] for hit in response['hits']['hits']]
    return jsonify(flows)

# Function to get flows for a given protocol
@app.route('/flows/<protocol>', methods=['GET'])
def get_flows_for_protocol(protocol):
    body = {
        "query": {
            "match": {
                "protocolName.keyword": protocol
            }
        }
    }
    response = es.search(index=index_name, body=body)
    flows = [hit['_source'] for hit in response['hits']['hits']]
    return jsonify(flows)

# Function to get flow count per protocol
@app.route('/flowcount', methods=['GET'])
def get_flow_count_per_protocol():
    response = es.search(index=index_name, size=0, aggs={'protocols': {'terms': {'field': 'protocolName.keyword'}}})
    flow_count_per_protocol = {bucket['key']: bucket['doc_count'] for bucket in response['aggregations']['protocols']['buckets']}
    return jsonify(flow_count_per_protocol)

# Function to draw the ranked distribution #Flows vs #Packets
@app.route('/ranked_distribution', methods=['GET'])
def draw_ranked_distribution():
    response = es.search(index=index_name, size=0, aggs={'flows_vs_packets': {'histogram': {'field': 'totalSourcePackets', 'interval': 1}}})
    buckets = response['aggregations']['flows_vs_packets']['buckets']
    flow_count = [bucket['doc_count'] for bucket in buckets]
    packet_count = [bucket['key'] for bucket in buckets]
    return jsonify({'packet_count': packet_count, 'flow_count': flow_count})

#function to get all app names
@app.route('/getAllAppNames', methods=['GET'])
def get_AllAppNames():
    response = es.search(index=index_name, aggs={'appName': {'terms': {'field': 'appName.keyword', "size": 10000}}})
    allAppNames = [bucket['key'] for bucket in response['aggregations']['appName']['buckets']]
    return jsonify(allAppNames)

@app.route('/totalBytesApps', methods=['GET'])
def get_total_bytes_per_apps():
    # Set fielddata to true for totalSourceBytes and totalDestinationBytes fields
    mapping = {
        "properties": {
            "totalSourceBytes": {
                "type": "text",
                "fielddata": True
            },
            "totalDestinationBytes": {
                "type": "text",
                "fielddata": True
            }
        }
    }
    es.indices.put_mapping(index=index_name, body=mapping)

    aggs = {
        "appName": {
            "terms": {
                "field": "appName.keyword",
                "size": 10000
            },
            "aggs": {
                "totalSourceBytes": {
                    "sum": {
                        "script": {
                            "source": "Integer.parseInt(doc['totalSourceBytes'].value)"
                        }
                    }
                },
                "totalDestinationBytes": {
                    "sum": {
                        "script": {
                            "source": "Integer.parseInt(doc['totalDestinationBytes'].value)"
                        }
                    }
                }
            }
        }
    }

    response = es.search(index=index_name, size=0, body={"aggs": aggs})

    # Do a loop over the aggregations to get the app names and total sizes
    allAppNames = []
    for app_bucket in response['aggregations']['appName']['buckets']:
        app_name = app_bucket['key']
        totalSourceBytes = app_bucket['totalSourceBytes']['value']
        totalDestinationBytes = app_bucket['totalDestinationBytes']['value']
        allAppNames.append({'appName': app_name, 'totalSourceBytes': totalSourceBytes, 'totalDestinationBytes': totalDestinationBytes})

    return jsonify(allAppNames)

@app.route('/totalPacketsApps', methods=['GET'])
def get_total_packets_per_apps():
    # Set fielddata to true for totalSourcePackets and totalDestinationPackets fields
    mapping = {
        "properties": {
            "totalSourcePackets": {
                "type": "text",
                "fielddata": True
            },
            "totalDestinationPackets": {
                "type": "text",
                "fielddata": True
            }
        }
    }
    es.indices.put_mapping(index=index_name, body=mapping)

    aggs = {
        "appName": {
            "terms": {
                "field": "appName.keyword",
                "size": 10000
            },
            "aggs": {
                "totalSourcePackets": {
                    "sum": {
                        "script": {
                            "source": "Integer.parseInt(doc['totalSourcePackets'].value)"
                        }
                    }
                },
                "totalDestinationPackets": {
                    "sum": {
                        "script": {
                            "source": "Integer.parseInt(doc['totalDestinationPackets'].value)"
                        }
                    }
                }

            }
        }
    }

    response = es.search(index=index_name, size=0, body={"aggs": aggs})

    # Do a loop over the aggregations to get the app names and total sizes
    allAppNames = []
    for app_bucket in response['aggregations']['appName']['buckets']:
        app_name = app_bucket['key']
        totalSourcePackets = app_bucket['totalSourcePackets']['value']
        totalDestinationPackets = app_bucket['totalDestinationPackets']['value']
        allAppNames.append({'appName': app_name, 'totalSourcePackets': totalSourcePackets, 'totalDestinationPackets': totalDestinationPackets})

    return jsonify(allAppNames)

@app.route('/totalPayloadsApps', methods=['GET'])
def get_total_payload_per_apps():
    # Set fielddata to true for sourcePayloadAsBase64 and destinationPayloadAsBase64 fields
    mapping = {
        "properties": {
            "sourcePayloadAsBase64": {
                "type": "text",
                "fielddata": True
            },
            "destinationPayloadAsBase64": {
                "type": "text",
                "fielddata": True
            }
        }
    }
    es.indices.put_mapping(index=index_name, body=mapping)
    
    aggs = {
        "appName": {
            "terms": {
                "field": "appName.keyword",
            },
            "aggs": {
                "sourcePayloadSize": {
                    "sum": {
                        "script": {
                            "source": "doc['sourcePayloadAsBase64'].size()"
                        }
                    }
                },
                "destinationPayloadSize": {
                    "sum": {
                        "script": {
                            "source": "doc['destinationPayloadAsBase64'].size()"
                        }
                    }
                }

            }
        }
    }

    response = es.search(index=index_name, size=0, body={"aggs": aggs})

    # Do a loop over the aggregations to get the app names and total sizes
    allAppNames = []
    for app_bucket in response['aggregations']['appName']['buckets']:
        app_name = app_bucket['key']
        totalSourcePayloads = app_bucket['sourcePayloadSize']['value']
        totalDestinationPayloads = app_bucket['destinationPayloadSize']['value']
        allAppNames.append({'appName': app_name, 'totalSourcePayloads': totalSourcePayloads, 'totalDestinationPayloads': totalDestinationPayloads})

    return jsonify(allAppNames)

#function to get the list of flows for a given application
@app.route('/getAllFlowsFromApp/<appName>', methods=['GET'])
def get_AllFlowsFromApp(appName):
# Function to get flows for a given protocol
    body = {
        "query": {
            "match": {
                "appName.keyword": appName
            }
        }
    }
    response = es.search(index=index_name, body=body)
    flows = [hit['_source'] for hit in response['hits']['hits']]
    return jsonify(flows)

#function to get the number of flows for each application
@app.route('/flowcountApp', methods=['GET'])
def get_flow_count_per_app():
    response = es.search(index=index_name, size=0, aggs={'appName': {'terms': {'field': 'appName.keyword'}}})
    flow_count_per_app = {bucket['key']: bucket['doc_count'] for bucket in response['aggregations']['appName']['buckets']}
    return jsonify(flow_count_per_app)


@app.route('/totalBytesProtocols', methods=['GET'])
def get_total_bytes_per_protocols():
    # Set fielddata to true for totalSourceBytes and totalDestinationBytes fields
    mapping = {
        "properties": {
            "totalSourceBytes": {
                "type": "text",
                "fielddata": True
            },
            "totalDestinationBytes": {
                "type": "text",
                "fielddata": True
            }
        }
    }
    es.indices.put_mapping(index=index_name, body=mapping)

    aggs = {
        "protocolName": {
            "terms": {
                "field": "protocolName.keyword",
                "size": 10000
            },
            "aggs": {
                "totalSourceBytes": {
                    "sum": {
                        "script": {
                            "source": "Integer.parseInt(doc['totalSourceBytes'].value)"
                        }
                    }
                },
                "totalDestinationBytes": {
                    "sum": {
                        "script": {
                            "source": "Integer.parseInt(doc['totalDestinationBytes'].value)"
                        }
                    }
                }
            }
        }
    }

    response = es.search(index=index_name, size=0, body={"aggs": aggs})

    # Do a loop over the aggregations to get the app names and total sizes
    allprotocolNames = []
    for protocol_bucket in response['aggregations']['protocolName']['buckets']:
        protocol_name = protocol_bucket['key']
        totalSourceBytes = protocol_bucket['totalSourceBytes']['value']
        totalDestinationBytes = protocol_bucket['totalDestinationBytes']['value']
        allprotocolNames.append({'protocolName': protocol_name, 'totalSourceBytes': totalSourceBytes, 'totalDestinationBytes': totalDestinationBytes})

    return jsonify(allprotocolNames)

@app.route('/totalPacketsProtocols', methods=['GET'])
def get_total_packets_per_protocols():
    # Set fielddata to true for totalSourcePackets and totalDestinationPackets fields
    mapping = {
        "properties": {
            "totalSourcePackets": {
                "type": "text",
                "fielddata": True
            },
            "totalDestinationPackets": {
                "type": "text",
                "fielddata": True
            }
        }
    }
    es.indices.put_mapping(index=index_name, body=mapping)

    aggs = {
        "protocolName": {
            "terms": {
                "field": "protocolName.keyword",
            },
            "aggs": {
                "totalSourcePackets": {
                    "sum": {
                        "script": {
                            "source": "Integer.parseInt(doc['totalSourcePackets'].value)"
                        }
                    }
                },
                "totalDestinationPackets": {
                    "sum": {
                        "script": {
                            "source": "Integer.parseInt(doc['totalDestinationPackets'].value)"
                        }
                    }
                }

            }
        }
    }

    response = es.search(index=index_name, size=0, body={"aggs": aggs})

    # Do a loop over the aggregations to get the protocol names and total sizes
    allProtocolNames = []
    for protocol_bucket in response['aggregations']['protocolName']['buckets']:
        protocol_name = protocol_bucket['key']
        totalSourcePackets = protocol_bucket['totalSourcePackets']['value']
        totalDestinationPackets = protocol_bucket['totalDestinationPackets']['value']
        allProtocolNames.append({'protocolName': protocol_name, 'totalSourcePackets': totalSourcePackets, 'totalDestinationPackets': totalDestinationPackets})

    return jsonify(allProtocolNames)

@app.route('/totalPayloadsProtocols', methods=['GET'])
def get_total_payload_per_protocols():
    # Set fielddata to true for sourcePayloadAsBase64 and destinationPayloadAsBase64 fields
    mapping = {
        "properties": {
            "sourcePayloadAsBase64": {
                "type": "text",
                "fielddata": True
            },
            "destinationPayloadAsBase64": {
                "type": "text",
                "fielddata": True
            }
        }
    }
    es.indices.put_mapping(index=index_name, body=mapping)

    aggs = {
        "protocolName": {
            "terms": {
                "field": "protocolName.keyword",
            },
            "aggs": {
                "sourcePayloadSize": {
                    "sum": {
                        "script": {
                            "source": "doc['sourcePayloadAsBase64'].size()"
                        }
                    }
                },
                "destinationPayloadSize": {
                    "sum": {
                        "script": {
                            "source": "doc['destinationPayloadAsBase64'].size()"
                        }
                    }
                }

            }
        }
    }

    response = es.search(index=index_name, size=0, body={"aggs": aggs})

    # Do a loop over the aggregations to get the protocol names and total sizes
    allProtocolNames = []
    for protocol_bucket in response['aggregations']['protocolName']['buckets']:
        protocol_name = protocol_bucket['key']
        totalSourcePayloads = protocol_bucket['sourcePayloadSize']['value']
        totalDestinationPayloads = protocol_bucket['destinationPayloadSize']['value']
        allProtocolNames.append({'protocolName': protocol_name, 'totalSourcePayloads': totalSourcePayloads, 'totalDestinationPayloads': totalDestinationPayloads})

    return jsonify(allProtocolNames)

if __name__ == '__main__':
    app.run(debug=True)
