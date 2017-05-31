#!/usr/bin/python
# -*- coding: utf-8 -*-

import string


class FieldsFormat():
    def __init__(self):
        self.cloudsearch_indexes = {
            'area_code': '',
            'car_license_plate': '',
            'country': '',
            'cpf': '',
            'created_at': '',
            'custom_properties': '',
            'devices_imei': '',
            'driver_status': '',
            'email': '',
            'id': '',
            'is_active': '',
            'is_registration_complete': '',
            'name': '',
            'phone': '',
            'sname': '',
            'type': ''
        }
        self.cloudsearch_doc = self.cloudsearch_indexes
        self.mongodb_schema = {
            '_id': '',
            'area_code': '',
            'car': {'plate': ''},
            'country': '',
            'created_at': '',
            'custom_properties': {'phone': '', 'cpf': ''},
            'devices': [{'imei': ''}],
            'email': '',
            'is_active': '',
            'is_registration_complete': '',
            'name': '',
            'phone': '',
            'sname': '',
            'status': '',
            'corp': {'corp_email': ''},
            'is_multiple_rides': ''
        }
        self.mongodb_doc = self.mongodb_schema

    def list_fields(self, str_fields):
        return [field.strip() for field in str_fields.split(',')]

    def check_fields_in_oplog(self, oplog_doc):
        if isinstance(oplog_doc, dict):
            for key in oplog_doc.keys():
                if key.split('.')[0] in self.mongodb_schema.keys():
                    return True
        return False

    def cloudsearch_empty_fields(self, fields_list):
        fields_dict_empty = {}
        for field in fields_list:
            fields_dict_empty[field] = ""
        return(fields_dict_empty)

    def map_mongodb_oplog_schema(self, oplog_doc):
        for field in self.mongodb_schema.keys():
            if field in oplog_doc.keys():
                self.mongodb_schema[field] = oplog_doc[field]
        return self.mongodb_schema

    def map_fields(self, collection, doc_mongodb):
        doc_add = self.cloudsearch_indexes
        doc_add['id'] = self.map_string(doc_mongodb['_id'])
        doc_add['name'] = self.map_string(doc_mongodb['name'])
        doc_add['email'] = self.map_string(doc_mongodb['email'])
        doc_add['created_at'] = self.map_isodate_to_string(
            doc_mongodb['created_at'])
        doc_add['custom_properties'] = self.map_custom_properties(
            doc_mongodb['custom_properties'])
        doc_add['cpf'] = self.map_cpf(doc_mongodb['custom_properties'])
        doc_add['phone'] = self.map_phone(doc_mongodb)
        doc_add['is_active'] = self.map_bool(doc_mongodb['is_active'])
        doc_add['devices_imei'] = self.map_devices_imei(
            doc_mongodb['devices'])
        doc_add['area_code'] = self.map_string(doc_mongodb['area_code'])
        doc_add['is_registration_complete'] = self.map_bool(
            doc_mongodb['is_registration_complete'])
        doc_add['country'] = self.map_string(doc_mongodb['country'])
        doc_add['type'] = collection[:-1]

        if collection == 'drivers':
            doc_add['sname'] = self.map_string(doc_mongodb['sname'])
            doc_add['car_license_plate'] = self.map_license_plate(
                doc_mongodb['car'])
            doc_add['driver_status'] = self.map_driver_status(
                collection, doc_mongodb['status'])
        else:
            doc_add['email_corporate'] = self.map_corp_email(
                doc_mongodb['corp'])
            doc_add['type'] = self.map_customer_type(doc_mongodb)
        return doc_add

    def map_string(self, field):
        try:
            return(str(field).encode('utf-8').strip())
        except:
            return ''

    def map_isodate_to_string(self, field):
        try:
            return str(field.isoformat().split('.')[0])+"Z"
        except:
            return ''

    def map_bool(self, field):
        try:
            return bool(field)
        except:
            return False

    def map_custom_properties(self, field):
        if isinstance(field, dict):
            custom_properties_str = ''
            for cp in field:
                custom_properties_str += '%s ' % self.remove_special_chars(
                    field[cp])
            return custom_properties_str
        else:
            return ''

    def map_phone(self, doc_mongodb):
        if doc_mongodb['phone']:
            return self.map_string(doc_mongodb['phone'])
        elif(
                'phone' in doc_mongodb['custom_properties']
                and
                doc_mongodb['custom_properties']['phone']):
            return self.map_string(doc_mongodb['custom_properties']['phone'])
        else:
            return ''

    def map_cpf(self, field):
        if 'cpf' in field and field['cpf']:
            return self.remove_special_chars(self.map_string(field['cpf']))
        else:
            return ''

    def map_devices_imei(self, field):
        try:
            devices_imei_str = ''
            for f in field:
                if 'imei' in f and f['imei']:
                    devices_imei_str += '%s ' % f['imei']
            return devices_imei_str
        except Exception as error:
            return 'Error %s' % error

    def map_license_plate(self, field):
        if 'license_plate' in field and field['license_plate']:
            return self.remove_special_chars(self.map_string(
                field['license_plate']))
        else:
            return ''

    def map_driver_status(self, collection, field):
        if collection == 'drivers':
            return self.map_string(field)
        else:
            return ''

    def map_corp_email(self, field):
        if 'corp_email' in field and field['corp_email']:
            return self.map_string(field['corp_email'])
        else:
            return ''

    def map_customer_type(self, field):
        if 'is_corp' in field and field['is_corp']:
            return 'corp'
        elif(
                'is_multiple_rides' in field
                and
                field['is_multiple_rides']):
            return 'pro'
        else:
            return 'default'

    def remove_special_chars(self, field):
        try:
            remove_format = string.punctuation + " "
            return ''.join(ch for ch in field if ch not in remove_format)
        except Exception as error:
            return 'Error %s' % error
