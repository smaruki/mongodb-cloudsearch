#!/usr/bin/python
# -*- coding: utf-8 -*-

#Replicate data from MongoDB to Amazon CloudSearch

import json
import time
import bson
import string
import boto.cloudsearch
import sys
import os
#from os import path
#from os import system
from datetime import datetime
from pymongo import MongoClient
from pymongo import ReadPreference
from pymongo.cursor import _QUERY_OPTIONS
from bson.objectid import ObjectId
from pymongo.errors import AutoReconnect
from ConfigParser import SafeConfigParser
from optparse import OptionParser
from fields_format import FieldsFormat


class MongoDBCloudSearch():
    def __init__(self):
        cnf_file_name = 'mdbcs.cnf'
        self.section = 'DEFAULT'
        self.secs_reconect = 10
        self.row_number = 0
        self.default_time = datetime(2017, 5, 1)
        self.debug_dic = {}
        self.parser = SafeConfigParser()
        self.parser.read(self.get_config_file(cnf_file_name))
        connection_primary = MongoClient(
            self.parser.get(self.section, 'mongodb_host_primary')
            )
        connection_secondary = MongoClient(
            self.parser.get(self.section, 'mongodb_host_secondary'),
            read_preference=ReadPreference.SECONDARY
            )
        self.db_local = connection_secondary['local']
        self.db_data = connection_secondary[
            self.parser.get(self.section, 'mongodb_database')
            ]
        self.db_log = connection_primary[
            self.parser.get(self.section, 'mongodb_database_log')
            ]
        self.file_path_default = self.parser.get(
            self.section, 'file_last_ts_default'
            )
        self.bulk_insert_amount = self.parser.get(
            self.section, 'bulk_insert_amount'
            )
        self.bulk_update_amount = self.parser.get(
            self.section, 'bulk_update_amount'
            )

    def build_option_parser(self):
        parser = OptionParser()
        parser.add_option(
            '-c', '--collection',
            help="Reads the specified collection from MongoDB Oplog",
            default='drivers'
            )
        parser.add_option(
            '-o', '--operation',
            help="Reads the specified CRUD operations \
                [insert], [update], [delete], [all] ",
            default='insert'
            )
        return parser

    def get_config_file(self, cnf_file_name):
        return(os.path.dirname(
            os.path.realpath(__file__)) + '/' +
            cnf_file_name)

    def get_collection_section(self, collection=None):
        if(collection):
            return collection.upper()
        else:
            return 'DEFAULT'

    def get_timestamp_file(self, section_collection, op):
        file_value = ('file_last_ts_%s' % op)
        try:
            return self.parser.get(section_collection, file_value)
        except:
            print('Error - [%s] %s not found!\nUsing default file' % (
                section_collection,
                file_value
                ))
            return self.file_path_default

    def get_cnf_cloudsearch_index_fields(self, section='DEFAULT'):
        return self.parser.get(section, 'cloudsearch_index_fields')

    def get_last_timestamp_saved(self, file_path):
        last_ts = None
        try:
            last_ts = open(file_path, 'r').read()
            last_ts = last_ts.strip()[:10]
        except:
            print('Error - open timestamp saved file')
        valid_ts = self.valid_timestamp(last_ts)
        last_ts_save = bson.timestamp.Timestamp(int(valid_ts), 0)
        return last_ts_save

    def valid_timestamp(self, ts):
        try:
            datetime.fromtimestamp(int(ts))
            return ts
        except:
            return time.mktime(self.default_time.timetuple())

    def get_oplog_ns(self, section_collection):
        return self.parser.get(section_collection, 'oplog_ns')

    def get_oplog_op(self, operation):
        ops = {
            'insert': 'i',
            'update': 'u',
            'delete': 'd',
            'all': {'$in': ['i', 'u', 'd']}
        }
        return ops[operation]

    def get_oplog_id(self, doc):
        if 'o' in doc and '_id' in doc['o']:
            return doc['o']['_id']
        elif 'o2' in doc and '_id' in doc['o2']:
            return doc['o2']['_id']
        else:
            raise

    def get_oplog_o(self, doc):
        if 'o' in doc:
            if '$set' in doc['o']:
                return doc['o']['$set']
            else:
                return doc['o']
        else:
            return {}

    def get_oplog_ts(self, doc):
        try:
            return self.valid_timestamp(str(doc['ts'])[10:20])
        except:
            return self.valid_timestamp(0)

    def cloudsearch_connect(self, section='DEFAULT'):
        conn = boto.connect_cloudsearch2(
            region=self.parser.get(section, 'cloudsearch_region'),
            aws_access_key_id=self.parser.get(section, 'cloudsearch_id'),
            aws_secret_access_key=self.parser.get(section, 'cloudsearch_key')
        )
        return conn.lookup(self.parser.get(section, 'cloudsearch_domain'))

    def cloudsearch_gen_id(self, collection, oplog_doc_id):
        #collection[:-1] for plural collection name e.g: driver[s]
        return '%s_%s' % (collection[:-1], str(oplog_doc_id))

    def cloudsearch_delete_op(self, cloudsearch_id, service):
        try:
            service.delete(cloudsearch_id)
            return service.commit()
        except Exception as error:
            return('Error commit %s' % error)

    def cloudsearch_commit_op(self, service, op):
        self.row_number += 1
        if self.row_number % self.get_bulk_amount(op) == 0:
            try:
                self.row_number = 0
                return service.commit()
            except Exception as error:
                return('Error commit %s' % error)
        return self.row_number

    def get_bulk_amount(self, op):
        if op == 'i':
            return int(self.bulk_insert_amount)
        elif op == 'u':
            return int(self.bulk_update_amount)
        else:
            return 1

    def get_mongodb_source_doc(self, oplog_doc_id, collection):
        return self.db_data[collection].find_one({
            '_id': ObjectId(oplog_doc_id)
            })

    def save_last_timestamp(self, file_path, last_ts, last_ts_saved):
        # Save if last_ts is more than 30 minutes from saved
        try:
            if int(last_ts) > int(last_ts_saved) + 1800:
                try:
                    save_file = open(file_path, 'w')
                    save_file.write(last_ts)
                    save_file.close()
                    return last_ts
                except:
                    raise
        except:
            raise
        return last_ts_saved

    def debug(self, state=False):
        if state is True and self.debug_dic:
            print('\n---------------------------------')
            for v in self.debug_dic:
                print('[%s] %s\n' % (v, self.debug_dic[v]))
        elif(
                'cloudsearch_id' in self.debug_dic
                and
                'response' in self.debug_dic):
            print('[%s] %s' % (
                self.debug_dic['cloudsearch_id'],
                self.debug_dic['response']))
        self.debug_dic = {}

    def main(self, debug=False):
        option_parser = self.build_option_parser()
        opts, args = option_parser.parse_args()

        section_collection = self.get_collection_section(opts.collection)
        file_path = self.get_timestamp_file(section_collection, opts.operation)
        last_ts = self.get_last_timestamp_saved(file_path)
        last_ts_saved = 0
        domain = self.cloudsearch_connect()
        fields = FieldsFormat()

        print(file_path)
        print(last_ts)
        print(opts)

        # Tailable cursor options.
        tail_opts = {'tailable': True, 'await_data': True}
        while True:
            query = {
                'op': self.get_oplog_op(opts.operation),
                'ns': self.get_oplog_ns(section_collection),
                'ts': {"$gte": last_ts}
                }
            cursor = self.db_local.oplog.rs.find(
                query,
                **tail_opts
                ).sort("$natural", 1)
            cursor.add_option(_QUERY_OPTIONS['oplog_replay'])
            service = domain.get_document_service()
            print('criou domain')
            try:
                while cursor.alive:
                    try:
                        oplog_doc = cursor.next()
                        last_ts = self.get_oplog_ts(oplog_doc)
                        oplog_doc_id = self.get_oplog_id(oplog_doc)
                        oplog_fields = self.get_oplog_o(oplog_doc)
                        cloudsearch_id = self.cloudsearch_gen_id(
                            opts.collection,
                            oplog_doc_id)

                        self.debug_dic.update({'oplog_doc': oplog_doc})
                        self.debug_dic.update(
                            {'cloudsearch_id': cloudsearch_id})

                        if(
                                oplog_doc['op'] in ['u', 'i']
                                and
                                fields.check_fields_in_oplog(oplog_fields)):

                            source_doc = self.get_mongodb_source_doc(
                                oplog_doc_id,
                                opts.collection)
                            mongodb_doc = fields.map_mongodb_oplog_schema(
                                source_doc)
                            doc_add = fields.map_fields(
                                opts.collection,
                                mongodb_doc)
                            service.add(cloudsearch_id, doc_add)
                            commited = self.cloudsearch_commit_op(
                                service,
                                oplog_doc['op'])
                            # Debug Add
                            self.debug_dic.update({'add': True})
                            self.debug_dic.update({'mongodb_doc': mongodb_doc})
                            self.debug_dic.update({'doc_add': doc_add})
                            self.debug_dic.update({'response': commited})
                        elif oplog_doc['op'] == 'd':
                            deleted = self.cloudsearch_delete_op(
                                cloudsearch_id,
                                service)
                            self.debug_dic.update({'response': deleted})
                        last_ts_saved = self.save_last_timestamp(
                            file_path,
                            last_ts,
                            last_ts_saved)
                        self.debug_dic.update({'last_ts_saved': last_ts_saved})
                    except(AutoReconnect, StopIteration):
                        print('Autoconnect')
                        time.sleep(self.secs_reconect)
                    except Exception as error:
                        print('Error - %s' % error)
                    self.debug(debug)
            except:
                raise

if __name__ == '__main__':
    MongoDBCloudSearch().main(debug=True)
