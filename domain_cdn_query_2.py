# -*- coding: UTF-8 -*-
import DNS
import urllib
import pymongo
import threading
from datetime import datetime


def domain_online_query(query, type):
    if type == "A":
        return dict_build(query, type)
    elif type == "NS":
        return dict_build(query, type)
    elif type == "CNAME":
        return dict_build(query, type)
    elif type == "SOA":
        return dict_build(query, type)
    elif type == "PTR":
        return dict_build(query, type)
    elif type == "CNAME":
        return dict_build(query, type)
    elif type == "CNAME":
        return dict_build(query, type)
    elif type == "STATUS":
        return domain_online_judge(query)
    elif type == "ANY":
        return any_dict_combine(query)


def dict_build(query, type):
    dict_domain = {}
    dict_domain['query_domain'] = query
    dict_domain['query_type'] = type
    dict_domain['query_answer'] = record_combine(query, type)
    return dict_domain


def any_dict_combine(query):
    dict_domain = {}
    dict_domain['query_domain'] = query
    dict_domain['query_type'] = "all"
    dict_domain['A_record'] = record_combine(query, "A")
    dict_domain['NS_record'] = record_combine(query, "NS")
    dict_domain['CNAME_record'] = record_combine(query, "CNAME")
    dict_domain['SOA_record'] = record_combine(query, "SOA")
    dict_domain['PTR_record'] = record_combine(query, "PTR")
    dict_domain['MX_record'] = record_combine(query, "MX")
    dict_domain['TXT_record'] = record_combine(query, "TXT")
    dict_domain_total = dict_domain.copy()
    dict_domain_total.update(domain_online_judge(query))
    return dict_domain_total


def record_combine(query, type):
    dict_a_record = {}
    try:
        if type == "A":
            type_query = DNS.Type.A
        elif type == "NS":
            type_query = DNS.Type.NS
        elif type == "CNAME":
            type_query = DNS.Type.CNAME
        elif type == "SOA":
            type_query = DNS.Type.SOA
        elif type == "PTR":
            type_query = DNS.Type.PTR
        elif type == "MX":
            type_query = DNS.Type.MX
        elif type == "TXT":
            type_query = DNS.Type.TXT
        DNS.DiscoverNameServers()
        reqobj = DNS.Request()
        answerobj_a = reqobj.req(
            name=query, qtype=type_query, server="222.194.15.253")
        if not len(answerobj_a.answers):
            dict_a_record = {type: ""}
        else:
            for item in answerobj_a.answers:
                if item['typename'] == "SOA":
                    dict_a_record[item['typename']] = soa_tuple_operate(item['data'])
                else:
                    try:
                        if dict_a_record[item['typename']]:
                            dict_a_record[item['typename']] = dict_a_record[item['typename']] + " " + item['data']
                    except:
                        dict_a_record[item['typename']] = item['data']
    except:
        dict_a_record = {type: ""}
    return dict_a_record


def soa_tuple_operate(tuple_soa):
    soa_dict = {}
    soa_dict['name_server'] = tuple_soa[0]
    soa_dict['responsible_person'] = tuple_soa[1]
    soa_dict['serial'] = tuple_soa[2][1]
    soa_dict['refresh'] = {'second':tuple_soa[3][1],'time':tuple_soa[3][2]}
    soa_dict['retry'] = {'second':tuple_soa[4][1],'time':tuple_soa[4][2]}
    soa_dict['expire'] = {'second':tuple_soa[5][1],'time':tuple_soa[5][2]}
    soa_dict['minimum'] = {'second':tuple_soa[6][1],'time':tuple_soa[6][2]}
    return soa_dict


def record_judge(query):
    try:
        DNS.DiscoverNameServers()
        reqobj = DNS.Request()
        answerobj_a = reqobj.req(
            name=query, qtype=DNS.Type.A, server="222.194.15.253")
        if len(answerobj_a.answers):
            return 1
        else:
            pass
    except:
        pass
    try:
        DNS.DiscoverNameServers()
        reqobj = DNS.Request()
        answerobj_a = reqobj.req(
            name=query, qtype=DNS.Type.MX, server="222.194.15.253")
        if len(answerobj_a.answers):
            return 1
        else:
            pass
    except:
        pass
    return 0


def http_code(query):
    '''
    查询http状态码
    '''
    try:
        status = urllib.urlopen(query)
        return str(status.getcode())
    except:
        return "error"


def domain_online_judge(query):
    domain = "http://" + query + "/"
    if(record_judge(query) == 1 and (http_code(domain)[0] == "2" or http_code(domain)[0] == "3")):
        dict_domain = {}
        dict_domain['query_domain'] = query
        dict_domain['status'] = "online"
        dict_domain['http_code'] = http_code(domain)
        return dict_domain
    else:
        dict_domain = {}
        dict_domain['query_domain'] = query
        dict_domain['status'] = "not online"
        dict_domain['http_code'] = http_code(domain)
        return dict_domain


def A_record_handle(dict_a):
    if 'A' in dict_a and dict_a['A'] =='':
        dict_a['A'] = {'A_record':''}
    elif not 'A' in dict_a:
        dict_a['A'] = {'A_record':''}
    elif 'A' in dict_a and dict_a['A'] != '':
        a = dict_a['A'].split(' ')
        dict_a['A'] = {'A_record':a}
    # elif 'CNAME' in dict_a and dict_a['CNAME'] != '':
    #     cname = dict_a['CNAME'].split(' ')
    #     dict_a['CNAME'] = cname
    return dict_a['A']['A_record']


def NS_record_handle(dict_ns):
    if 'NS' in dict_ns and dict_ns['NS'] =='':
        dict_ns['NS'] = {'NS_record':''}
    elif not 'NS' in dict_ns:
        dict_ns['NS'] = {'NS_record':''}
    elif 'NS' in dict_ns and dict_ns['NS'] != '':
        a = dict_ns['NS'].split(' ')
        dict_ns['NS'] = {'NS_record':a}
    # elif 'CNAME' in dict_ns and dict_ns['CNAME'] != '':
    #     cname = dict_ns['CNAME'].split(' ')
    #     dict_ns['CNAME'] = cname
    return dict_ns['NS']['NS_record']


def CNAME_record_handle(dict_cname):
    if 'CNAME' in dict_cname and dict_cname['CNAME'] =='':
        dict_cname['CNAME'] = {'CNAME_record':''}
    elif not 'CNAME' in dict_cname:
        dict_cname['CNAME'] = {'CNAME_record':''}
    elif 'CNAME' in dict_cname and dict_cname['CNAME'] != '':
        a = dict_cname['CNAME'].split(' ')
        dict_cname['CNAME'] = {'CNAME_record':a}
    # elif 'CNAME' in dict_a and dict_a['CNAME'] != '':
    #     cname = dict_a['CNAME'].split(' ')
    #     dict_a['CNAME'] = cname
    return dict_cname['CNAME']['CNAME_record']


def list_compare(list_a,list_b):
    ret_1 =  [i for i in list_a if i not in list_b ]
    ret_2 =  [i for i in list_b if i not in list_a ]
    if not (ret_1 == [] and ret_2 != []) or (ret_2 == [] and ret_1 != []):
        return 1

def cdn_1():
    connection=pymongo.MongoClient('172.29.152.152',27017)
    db=connection.domain_cdn_analysis
    collection=db.domain_dns_query
    for data in collection.find({'flag':1,'id_count':{'$gt':0,'$lt':250000}}):
        count = -1
        for item in data['DNS_record']:
            count = count + 1
        domain = str(data['domain'])
        domain_2 = "www." + domain
        a_record = domain_online_query(domain_2,"A")['query_answer']
        cname_record = domain_online_query(domain_2,"CNAME")['query_answer']
        ns_record = domain_online_query(domain,"NS")['query_answer']
        a = A_record_handle(a_record)
        cname = CNAME_record_handle(cname_record)
        ns = NS_record_handle(ns_record)
        #if data['DNS_record'][count]['A_record'] == a and data['DNS_record'][count]['NS_record'] == ns and data['DNS_record'][count]['CNAME_record'] == cname:
        if list_compare(data['DNS_record'][count]['A_record'],a) == 1 and list_compare(data['DNS_record'][count]['NS_record'],ns) == 1 and list_compare(data['DNS_record'][count]['CNAME_record'],cname) == 1:
            collection.update({'_id':data['_id']},{'$set':{'flag':2}})
        else:
            dns_old = data['DNS_record']
            dns_old.append({'A_record':a,'CNAME_record':cname,'NS_record':ns,'insert_time':datetime.now().strftime('%Y-%m-%d %H:%M:%S')})
            collection.update({'_id':data['_id']},{'$set':{'DNS_record':dns_old,'flag':2}})


def cdn_2():
    connection=pymongo.MongoClient('172.29.152.152',27017)
    db=connection.domain_cdn_analysis
    collection=db.domain_dns_query
    for data in collection.find({'flag':1,'id_count':{'$gt':249999,'$lt':500000}}):
        count = -1
        for item in data['DNS_record']:
            count = count + 1
        domain = str(data['domain'])
        domain_2 = "www." + domain
        a_record = domain_online_query(domain_2,"A")['query_answer']
        cname_record = domain_online_query(domain_2,"CNAME")['query_answer']
        ns_record = domain_online_query(domain,"NS")['query_answer']
        a = A_record_handle(a_record)
        cname = CNAME_record_handle(cname_record)
        ns = NS_record_handle(ns_record)
        if list_compare(data['DNS_record'][count]['A_record'],a) == 1 and list_compare(data['DNS_record'][count]['NS_record'],ns) == 1 and list_compare(data['DNS_record'][count]['CNAME_record'],cname) == 1:
            collection.update({'_id':data['_id']},{'$set':{'flag':2}})
        else:
            dns_old = data['DNS_record']
            dns_old.append({'A_record':a,'CNAME_record':cname,'NS_record':ns,'insert_time':datetime.now().strftime('%Y-%m-%d %H:%M:%S')})
            collection.update({'_id':data['_id']},{'$set':{'DNS_record':dns_old,'flag':2}})


def cdn_3():
    connection=pymongo.MongoClient('172.29.152.152',27017)
    db=connection.domain_cdn_analysis
    collection=db.domain_dns_query
    for data in collection.find({'flag':1,'id_count':{'$gt':499999,'$lt':750000}}):
        count = -1
        for item in data['DNS_record']:
            count = count + 1
        domain = str(data['domain'])
        domain_2 = "www." + domain
        a_record = domain_online_query(domain_2,"A")['query_answer']
        cname_record = domain_online_query(domain_2,"CNAME")['query_answer']
        ns_record = domain_online_query(domain,"NS")['query_answer']
        a = A_record_handle(a_record)
        cname = CNAME_record_handle(cname_record)
        ns = NS_record_handle(ns_record)
        if list_compare(data['DNS_record'][count]['A_record'],a) == 1 and list_compare(data['DNS_record'][count]['NS_record'],ns) == 1 and list_compare(data['DNS_record'][count]['CNAME_record'],cname) == 1:
            collection.update({'_id':data['_id']},{'$set':{'flag':2}})
        else:
            dns_old = data['DNS_record']
            dns_old.append({'A_record':a,'CNAME_record':cname,'NS_record':ns,'insert_time':datetime.now().strftime('%Y-%m-%d %H:%M:%S')})
            collection.update({'_id':data['_id']},{'$set':{'DNS_record':dns_old,'flag':2}})


def cdn_4():
    connection=pymongo.MongoClient('172.29.152.152',27017)
    db=connection.domain_cdn_analysis
    collection=db.domain_dns_query
    for data in collection.find({'flag':1,'id_count':{'$gt':749999,'$lt':1000000}}):
        count = -1
        for item in data['DNS_record']:
            count = count + 1
        domain = str(data['domain'])
        domain_2 = "www." + domain
        a_record = domain_online_query(domain_2,"A")['query_answer']
        cname_record = domain_online_query(domain_2,"CNAME")['query_answer']
        ns_record = domain_online_query(domain,"NS")['query_answer']
        a = A_record_handle(a_record)
        cname = CNAME_record_handle(cname_record)
        ns = NS_record_handle(ns_record)
        if list_compare(data['DNS_record'][count]['A_record'],a) == 1 and list_compare(data['DNS_record'][count]['NS_record'],ns) == 1 and list_compare(data['DNS_record'][count]['CNAME_record'],cname) == 1:
            collection.update({'_id':data['_id']},{'$set':{'flag':2}})
        else:
            dns_old = data['DNS_record']
            dns_old.append({'A_record':a,'CNAME_record':cname,'NS_record':ns,'insert_time':datetime.now().strftime('%Y-%m-%d %H:%M:%S')})
            collection.update({'_id':data['_id']},{'$set':{'DNS_record':dns_old,'flag':2}})


threads = []
t1 = threading.Thread(target=cdn_1)
threads.append(t1)
t2 = threading.Thread(target=cdn_2)
threads.append(t2)
t3 = threading.Thread(target=cdn_3)
threads.append(t3)
t4 = threading.Thread(target=cdn_4)
threads.append(t4)


if __name__ == "__main__":
    for t in threads:
        t.setDaemon(True)
        t.start()
    for t in threads:
        t.join()


