#!/usr/bin/python
# Python 3.x
import sys
import subprocess
import json
from argparse import ArgumentParser
from datetime import datetime

def query_kv_decode(kv_pair):
    if ":" not in kv_pair:
        print ("\n[Error]: [-k] is a \'key:value\' pair; parameters must go with a \':\'.")
        return None,None
    else:
        query_key = kv_pair.split(':')[0].strip()
        query_value = kv_pair.split(':')[1].strip()
        print("kv query is \'" + query_key + ":" + query_value + "\'.")
        return query_key, query_value

def main():
    # Check if module Elasticsearch is installed.
    try:
        # from elasticsearch import Elasticsearch
        import elasticsearch
    except ImportError:
        print("[Error]: Cannot find module [Elasticsearch] installed.")
        print("You can use \"pip install elasticsearch\" to fix this problem.")

    if 'elasticsearch' in sys.modules:
        # Default parameters.
        today = datetime.now().strftime("%Y.%m.%d")
        default_index_name = 'logstash-' + today
        try:
            p = subprocess.Popen(["hostname", "-I"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            hostip, err = p.communicate()
            hostip = hostip.strip()
        except:
            hostip = '127.0.0.1'

        # Create command line arguments.
        parser = ArgumentParser(description='Handle Elasticsearch parameters', add_help=False)
        parser.add_argument('-h', action="store", dest='es_host', default=hostip)
        parser.add_argument('-p', action="store", dest='es_port', default=9200)
        parser.add_argument('-i', action="store", dest='es_index', default=default_index_name)
        #parser.add_argument('-t', action="store", dest='es_type')
        parser.add_argument('-r', action="store", dest='relative_time')
        parser.add_argument('-q', action="store", dest='es_query_body', default={"query": {"match_all": {}}})
        parser.add_argument('-k', action="store", dest='es_key_value')
        res = parser.parse_args()

        # Check the [-r](relative_time query) & [-t](ES type) parameters.
        # if [-r]
        if res.relative_time is not None:
            gte_date_range = 'now-' + res.relative_time

            # if not [-k]
            if res.es_key_value is None:

                # Replace [-q] body
                res.es_query_body = {
                    "query": {
                        "bool": {
                            "filter": [{
                                "range": {
                                    "@timestamp": {
                                        "gte": gte_date_range
                                    }
                                }
                            }
                            ]
                        }
                    }
                }
            # if [-k], then use Keyword, kv.
            # Query like "key:value"
            else:
                query_key, query_value = query_kv_decode(res.es_key_value)
                # Replace [-q] body
                res.es_query_body = {
                    "query": {
                        "bool": {
                            "must": [{
                                "match": {
                                    query_key: query_value
                                }
                            }],
                            "filter": [{
                                "range": {
                                    "@timestamp": {
                                        "gte": gte_date_range
                                    }
                                }
                            }]
                        }
                    }
                }
        # Else: use default {"query": {"match_all": {}}} or [-q] query.
        else:
            # if use [-k] argument
            if res.es_key_value is not None:
                query_key, query_value = query_kv_decode(res.es_key_value)
                res.es_query_body = {
                    "query": {
                        "query_string" : {
                            "default_field" : query_key,
                            "query" : query_value
                        }
                    }
                }
        # Create ES object.
        es = elasticsearch.Elasticsearch(host=res.es_host, port=res.es_port)

        # Go Search.
        try:
            output = es.search(index=res.es_index, body=res.es_query_body)
            print("Got %d hits." % output['hits']['total'])

            if int(output['hits']['total']) > 0:
                sys.exit(0)  # Got hits! OK!
            else:
                sys.exit(2)  # No hits, go critical.

        # 404: raise NotFoundError
        except elasticsearch.NotFoundError:
            print("\n[Error]: Not Found index: " + res.es_index)
            print("Please use the [-i] parameter to specify one of the following indices:\nIndices:")
            tmpList = [index for index in es.indices.get('*')]
            for index in tmpList: print("    |-- " + index)
            print("\n")
            sys.exit(2)

        # Error raised when there was an exception while talking to ES.
        except elasticsearch.ConnectionError:
            print("\n[Error]: Connection Error! Is the host: " + res.es_host + " hosted Elasticsearch?")
            print("Or check your Parameters or Network status.\n")
            sys.exit(2)

        # Exception raised when ES returns a non-OK (>=400) HTTP status code.
        except elasticsearch.TransportError as e:
            if e.status_code == 'N/A':
                print("\n[Error]: An actual connection error happens!")
            else:
                print("\n[Error]: TransportError! [" + str(e.status_code) + "]: " + str(e.error))
            print(json.dumps(e.info, sort_keys=True, indent=2, separators=(',', ': ')))
            sys.exit(2)
            # print e.info


if __name__ == '__main__':
    main()
