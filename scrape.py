from datetime import datetime, timedelta
from StringIO import StringIO
import csv
import sqlite3
#from time import sleep
from pprint import pprint
from lxml import html
import logging
import requests

URL = "http://foerderportal.bund.de/foekat/jsp/SucheAction.do"

logging.basicConfig(level=logging.DEBUG)
log = logging.getLogger(__name__)

def get_fkzs_time_period(session, start, end):
    data = {"actionMode":"searchlist",
            "suche.laufzeitVon": start.strftime("%d.%m.%Y"),
            "suche.laufzeitBis": end.strftime("%d.%m.%Y"),
            "suche.lfdVhb":"N",
            "suche.ZeSt":"ZE",
            "suche.ausdruckSuchParam":"0",
            "general.search":"Suche starten"}
    res = session.post(URL, data=data)
    res = session.get(URL, params={"actionMode":"print", "presentationType":"csv"})
    reader = csv.DictReader(StringIO(res.content.encode('utf-8')), delimiter=';')
    for row in reader:
        fkz = row.get('="FKZ"').split('"')[1]
        yield fkz

def get_fkzs():
    session = requests.session()
    begin = datetime(1949, 5, 23)
    #begin = datetime(1979, 5, 23)
    interval = timedelta(days=3*30)
    while True:
        end = begin + interval
        log.info("Period under consideratin: %s", end)
        for id in get_fkzs_time_period(session, begin, end):
            yield session, id
        if end > datetime.now():
            break
        begin = end
    
def get_by_fkz(session, fkz):
    res = session.get(URL, params={'actionMode': 'view', 'fkz': fkz})
    doc = html.document_fromstring(res.content)
    tds = [td.text.strip() for td in doc.findall('.//td')]
    #pprint([(i, td) for i, td in enumerate(tds)])
    #out = {'zuwendungsempfaenger': {}, 'ausfuehrende_stelle': {}}
    out = {}
    out['fkz'] = tds[1]
    out['thema'] = tds[3]
    out['ressort'] = tds[5]
    out['projekttraeger'] = tds[7]
    out['referat'] = tds[9]
    out['arbeitseinheit'] = tds[11]
    out['laufzeit_von'] = tds[13]
    out['foerdersumme'] = tds[15]
    out['laufzeit_bis'] = tds[17]
    out['leistungsplan_systematik'] = tds[19]
    out['foerderart'] = tds[21]
    out['foerderprofil'] = tds[23]
    out['zuwendungsempfaenger.name'] = tds[26]
    out['zuwendungsempfaenger.ort'] = tds[28]
    out['zuwendungsempfaenger.land'] = tds[30]
    out['zuwendungsempfaenger.staat'] = tds[32]
    out['ausfuehrende_stelle.name'] = tds[35]
    out['ausfuehrende_stelle.ort'] = tds[37]
    out['ausfuehrende_stelle.land'] = tds[39]
    out['ausfuehrende_stelle.staat'] = tds[41]
    #pprint(out)
    return out


def scrape(file_name, table="fk"):
    conn = sqlite3.connect(file_name)
    for i, (session, fkz) in enumerate(get_fkzs()):
        if i == 0:
            row = get_by_fkz(session, fkz)
            try:
                keys = sorted(row.keys())
                cols = ", ".join(["\"%s\" TEXT" % k for k in keys])
                conn.execute("CREATE TABLE %s (%s)" % (table, cols))
            except Exception,e:
                log.exception(e)
        c = conn.cursor()
        c.execute("SELECT * FROM %s WHERE fkz = ?" % table, (fkz,))
        if len(list(c)):
            log.debug("FKZ exists: %s", fkz)
            continue
        row = get_by_fkz(session, fkz)
        quotes = ", ".join(["?"] * len(row))
        conn.execute("INSERT INTO %s VALUES (%s)" % (table, quotes),
                [row[k] for k in sorted(row.keys())])
        conn.commit()
        log.debug("Saved: %s", fkz)

if __name__ == '__main__':
    scrape("fk.db")
