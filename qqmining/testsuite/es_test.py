# -*- coding: utf-8 -*-

from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search, Q

es = Elasticsearch([{'host': '219.245.186.69', 'port': 9200}])

NODE_INDEX = 'neo4j-index-node'
RELATIONSHIP_INDEX = 'neo4j-index-relationship'

name = "张智平"
uin = ''
age_filter = True
gender_filter = True
age_low = 20
age_high = 40
# 男 1
# 女 0
gender_tag = 0
s = Search().using(es)

if len(uin) != 0:
    s = s.query("match", uin=uin)

if len(name) != 0:
    s = s.query("match", name=name)

if gender_filter:
    s = s.query("match", gender=gender_tag)

if age_filter:
    s = s.filter('range', age={'gte': age_low, 'lt': age_high})

response = s.execute()

final_user_result = []
for hit in s:
    uuid = hit.meta.id
    final_user_result.append([uuid, hit.uin, hit.name])

print(final_user_result)
