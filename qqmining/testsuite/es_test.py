# -*- coding: utf-8 -*-

from elasticsearch import Elasticsearch

es = Elasticsearch([{'host': '219.245.186.69', 'port': 9201}])

mapping = es.indices.get_mapping()

print(mapping)
