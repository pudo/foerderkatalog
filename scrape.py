from datetime import datetime, timedelta
from StringIO import StringIO
import csv
import sqlaload as sl
#from time import sleep
from pprint import pprint
from lxml import html
import logging
import requests

LPSLIST = "http://foerderportal.bund.de/foekat/jsp/LovAction.do?actionMode=searchlist&lov.sqlIdent=lpsys&lov.openerField=suche_lpsysSuche_0_&lov.ZeSt="
URL = "http://foerderportal.bund.de/foekat/jsp/SucheAction.do"

logging.basicConfig(level=logging.DEBUG)
log = logging.getLogger(__name__)

def get_fkzs_page(session, lps):
    res = session.get(URL, params={"actionMode":"searchreset"})
    data = {"actionMode":"searchlist",
            #"suche.laufzeitVon": start.strftime("%d.%m.%Y"),
            #"suche.laufzeitBis": end.strftime("%d.%m.%Y"),
            "suche.lfdVhb":"N",
            "suche.lpsysSuche[0]": lps + "%",
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
    session.config['max_retries'] = 5
    res = session.get(LPSLIST)
    doc = html.document_fromstring(res.content)
    for a in doc.findall('.//tr//a'):
        lps = a.text.strip().split()[0]
        log.info("LPS: %s", lps)
        for id in get_fkzs_page(session, lps):
            yield session, id
    
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


def scrape(engine_url, table="fk"):
    engine = sl.connect(engine_url)
    table = sl.get_table(engine, table)
    for i, (session, fkz) in enumerate(get_fkzs()):
        row = sl.find_one(engine, table, fkz=fkz)
        if row is not None:
            log.debug("FKZ exists: %s", fkz)
            continue
        row = get_by_fkz(session, fkz)
        sl.upsert(engine, table, row, ['fkz'])
        log.debug("Saved: %s", fkz)

if __name__ == '__main__':
    scrape("sqlite:///fk.db")
