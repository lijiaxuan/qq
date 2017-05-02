# -*- coding: utf-8 -*-

from elasticsearch import Elasticsearch

es = Elasticsearch([{'host': '219.245.186.69', 'port': 9200}])

NODE_INDEX = 'neo4j-index-node'
RELATIONSHIP_INDEX = 'neo4j-index-relationship'

qq_query_dsl = \
    {
        "query": {
            "filtered": {
                "filter": {
                    "range": {
                        "age": {
                            "gte": 20,
                            "lt": 40
                        }
                    }
                }
            }
        }
    }

result = es.search(index=NODE_INDEX, doc_type='QQ')

total_cnt = result['hits']['total']

hits = result['hits']['hits']

final_user_result = {}
user_ids = []
for hit in hits:
    source = hit['_source']
    doc_id = hit['_id']
    if 'name' not in source:
        source['name'] = ''
    user_ids.append(doc_id)
    print(source)
    final_user_result[doc_id] = source

print(total_cnt)
print(user_ids)
print(final_user_result)
print(result)
