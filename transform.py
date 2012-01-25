#coding: utf-8

import sys
import csv
import sqlaload as sl
from urllib import urlopen
from datetime import datetime

from recon import public_body

LPS_URL = 'leistungsplan_systematik.csv'

def integrate_lps(engine, table):
    fh = urlopen(LPS_URL)
    lp4 = {}
    lp3 = {}
    lp2 = {}
    lp1 = {}
    for row in csv.DictReader(fh):
        row = dict([(k, v.decode('utf-8')) for k,v in row.items()])
        id = row['LP'].strip()
        lp4[id] = row
        lp3[id[:4]] = row
        lp2[id[:2]] = row
        lp1[id[:1]] = row
    for row in sl.distinct(engine, table, 'leistungsplan_systematik'):
        lp_ = row['leistungsplan_systematik']
        lp = lp_.split()[0].strip()
        lp_desc = lp_.split('\t', 1)[-1].strip()
        sl.upsert(engine, table, {
            'leistungsplan_systematik': lp_,
            'lp1_name': lp[:1], 
            'lp1_label': lp1.get(lp[:1], {}).get('Ebene1_Name'), 
            'lp2_name': lp[:2], 
            'lp2_label': lp2.get(lp[:2], {}).get('Ebene2_Name'), 
            'lp3_name': lp[:4], 
            'lp3_label': lp3.get(lp[:4], {}).get('Ebene3_Name'), 
            'lp4_name': lp, 
            'lp4_label': lp4.get(lp, {}).get('Bezeichnung', lp_desc),
            'hh_titel': lp4.get(lp, {}).get('Kapitel/Titel')
            }, 
            ['leistungsplan_systematik'])

def integrate_ressort(engine, table):
    RESSORTS = {
        'BMBF': u'Bundesministerium für Bildung und Forschung',
        'BMELV': u'Bundesministerium für Ernährung, Landwirtschaft und Verbraucherschutz',
        'BMU': u'Bundesministerium für Umwelt, Naturschutz und Reaktorsicherheit',
        'BMWi': u'Bundesministerium für Wirtschaft und Technologie',
        'BMVBS': u'Bundesministerium für Verkehr, Bau und Stadtentwicklung'
        }
    for row in sl.distinct(engine, table, 'ressort'):
        r = row['ressort'].strip()
        r_label = RESSORTS[r]
        r_uri = None
        results = public_body(r_label, jurisdiction="DE")
        if results:
            r_uri = results[0].uri
        sl.upsert(engine, table, {'ressort': row['ressort'],
                                  'ressort_label': r_label,
                                  'ressort_uri': r_uri}, 
                                  ['ressort'])

def fix_formats(engine, table):
    for row in sl.all(engine, table):
        summe = row['foerdersumme'].split(' ', 1)[0]
        summe = summe.replace(".", "").replace(",", ".")
        row['foerdersumme_num'] = float(summe)
        row['laufzeit_von_dt'] = datetime.strptime(row['laufzeit_von'], '%d.%m.%Y')
        row['laufzeit_bis_dt'] = datetime.strptime(row['laufzeit_bis'], '%d.%m.%Y')
        sl.upsert(engine, table, row, ['fkz'])

def dump_table(engine, table):
    file_name = 'fk-%s.csv' % datetime.utcnow().strftime("%Y-%m-%d")
    fh = open(file_name, 'wb')
    sl.dump_csv(sl.all(engine, table), fh)

if __name__ == '__main__':
    assert len(sys.argv)==3, "Usage: %s {lps,res,fix,dump} [engine-url]"
    op = sys.argv[1]
    engine = sl.connect(sys.argv[2])
    table = sl.get_table(engine, 'fk')
    ops = {
        'lps': integrate_lps,
        'res': integrate_ressort,
        'fix': fix_formats,
        'dump': dump_table
        }.get(op)(engine, table)

