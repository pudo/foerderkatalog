#coding: utf-8

import sqlite3
import sys
import csv
from urllib import urlopen
from datetime import datetime

from recon import public_body

LPS_URL = 'leistungsplan_systematik.csv'

def integrate_lps(conn, table):
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
    for col in ['lp1_name', 'lp1_label', 'lp2_name', 'lp2_label', 'lp3_name',
                'lp3_label', 'lp4_label', 'lp4_name', 'hh_titel']:
        try:
            conn.execute("ALTER TABLE %s ADD COLUMN %s TEXT" % (table, col))
        except: pass
    c = conn.cursor()
    c.execute("SELECT DISTINCT leistungsplan_systematik FROM %s" % table)
    for row in c:
        lp = row[0].split()[0].strip()
        lp_desc = row[0].split('\t', 1)[-1].strip()
        conn.execute('UPDATE %s SET lp1_name = ?, lp1_label = ?, '
                     'lp2_name = ?, lp2_label = ?, lp3_name = ?, '
                     'lp3_label = ?, lp4_label = ?, lp4_name = ?, '
                     'hh_titel = ? '
                     'WHERE leistungsplan_systematik = ?' % table,
                     (lp[:1], lp1.get(lp[:1], {}).get('Ebene1_Name'), 
                      lp[:2], lp2.get(lp[:2], {}).get('Ebene2_Name'), 
                      lp[:4], lp3.get(lp[:4], {}).get('Ebene3_Name'), 
                      lp, lp4.get(lp, {}).get('Bezeichnung', lp_desc),
                      lp4.get(lp, {}).get('Kapitel/Titel'),
                      row[0]))
        conn.commit()

def integrate_ressort(conn, table):
    RESSORTS = {
        'BMBF': u'Bundesministerium für Bildung und Forschung',
        'BMELV': u'Bundesministerium für Ernährung, Landwirtschaft und Verbraucherschutz',
        'BMU': u'Bundesministerium für Umwelt, Naturschutz und Reaktorsicherheit',
        'BMWi': u'Bundesministerium für Wirtschaft und Technologie',
        'BMVBS': u'Bundesministerium für Verkehr, Bau und Stadtentwicklung'
        }
    for col in ['ressort_label', 'ressort_uri']:
        try:
            conn.execute("ALTER TABLE %s ADD COLUMN %s TEXT" % (table, col))
        except: pass
    c = conn.cursor()
    c.execute("SELECT DISTINCT ressort FROM %s" % table)
    for row in c:
        r = row[0].strip()
        r_label = RESSORTS[r]
        r_uri = None
        results = public_body(r_label, jurisdiction="DE")
        if results:
            r_uri = results[0].uri
        conn.execute('UPDATE %s SET ressort_label = ?, ressort_uri = ? WHERE ressort = ?' % table,
                (r_label, r_uri, row[0]))
        conn.commit()

def fix_formats(conn, table):
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    c.execute("SELECT fkz, foerdersumme, laufzeit_von, laufzeit_bis FROM %s" % table)
    for row in c:
        summe = row['foerdersumme'].split(' ', 1)[0]
        summe = summe.replace(".", "").replace(",", ".")
        von = datetime.strptime(row['laufzeit_von'], '%d.%m.%Y').strftime('%Y-%m-%d')
        bis = datetime.strptime(row['laufzeit_bis'], '%d.%m.%Y').strftime('%Y-%m-%d')
        conn.execute('UPDATE %s SET foerdersumme = ?, laufzeit_von = ?, '
                     'laufzeit_bis = ? WHERE fkz = ?' % table,
                     (summe, von, bis, row[0]))
        conn.commit()


if __name__ == '__main__':
    assert len(sys.argv)==3, "Usage: %s {lps} [sqlite-db]"
    op = sys.argv[1]
    conn = sqlite3.connect(sys.argv[2])
    ops = {
        'lps': integrate_lps,
        'res': integrate_ressort,
        'fix': fix_formats,
        }.get(op)(conn, 'fk'),

