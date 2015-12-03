import os
import time
import logging
import requests
from lxml import html
import dataset
from datetime import datetime

log = logging.getLogger(__name__)

URL = "http://foerderportal.bund.de/foekat/jsp/SucheAction.do"
XPATH = './/div[@class="content_background_inner"]//td[1]/a[@title="Detailansicht"]'
PAGE = 1000

FIELDS = {
    'P_Arbeitseinheit': 'arbeitseinheit',
    'P_Bezeichnung des Verbundprojektes': 'bezeichnung',
    'P_Frderart': 'art',
    'P_Frderkennzeichen': 'fkz',
    'P_Frderprofil': 'profil',
    'P_Frdersumme': 'summe',
    'P_Laufzeit bis': 'laufzeit_bis',
    'P_Laufzeit von': 'laufzeit_von',
    'P_Leistungsplan-systematik': 'lps',
    'P_Projekttrger': 'projekttraeger',
    'P_Referat': 'referat',
    'P_Ressort': 'ressort',
    'P_Thema': 'thema',
    'ST_Land': 'stelle_land',
    'ST_Name': 'stelle_name',
    'ST_Ort': 'stelle_ort',
    'ST_Staat': 'stelle_staat',
    'ZE_Land': 'empfaenger_land',
    'ZE_Name': 'empfaenger_name',
    'ZE_Ort': 'empfaenger_ort',
    'ZE_Staat': 'empfaenger_staat',
}

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (iPad; U; CPU OS 3_2_1 like Mac OS X; en-us) AppleWebKit/531.21.10 (KHTML, like Gecko) Mobile/7B405'
}


db = os.environ.get('DATABASE_URI', 'sqlite:///data.sqlite')
engine = dataset.connect(db)
table = engine.get_table('de_foerderkatalog')

logging.basicConfig(level=logging.DEBUG)
requests_log = logging.getLogger("requests")
requests_log.setLevel(logging.WARNING)


def date(row):
    try:
        data = row.get('Veroffentlichungsdatum')
        return datetime.strptime(data, '%d.%m.%Y')
    except:
        return


def run_query():
    session = requests.Session()
    session.headers.update(HEADERS)
    while True:
        try:
            res = session.post(URL, data={
                'actionMode': 'searchlist',
                'suche.nurVerbund': 'N',
                'suche.lfdVhb': 'N',
                'suche.ZeSt': 'ZE',
                'suche.ausdruckSuchParam': '0',
                'general.search': 'Suche starten'
            })
            if 'Suchergebnis' in res.content:
                doc = html.fromstring(res.content)
                opts = doc.findall('.//form[@id="listselect"]//option')
                select = opts[-1].text
                print 'PAGES', select
                count = int(select.split()[-1])
                return count, session
            else:
                print res.content
        except Exception, e:
            log.exception(e)
        time.sleep(10)


def get_offset(session, offset):
    log.info("Getting from offset: %s", offset)
    while True:
        try:
            res = session.post(URL, data={
                'suche.listrowpersite': PAGE,
                'suche.orderby': 1,
                'suche.order': 'asc',
                'suche.listrowfrom': offset
            })
            return html.fromstring(res.content)
        except requests.exceptions.ConnectionError:
            session = run_query()


def get_fkzs():
    count, session = run_query()
    for i, offset in enumerate(xrange(1, count, PAGE)):
        doc = get_offset(session, offset)
        for fkz in doc.findall(XPATH):
            yield fkz.get('href').rsplit('fkz=')[-1]

        if i % 100 == 0:
            count, session = run_query()


def field(td):
    text = td.text.strip()
    header = 'align:right' in td.get('style', '')
    if header:
        fset = td.getparent().getparent().getparent().get('id', 'P')
        text = '%s_%s' % (fset, text.encode('ascii', 'ignore'))
    return header, text


def get_by_fkz(fkz):
    while True:
        try:
            res = requests.get(URL, params={'actionMode': 'view', 'fkz': fkz},
                               headers=HEADERS)
            doc = html.document_fromstring(res.content)
            tdtexts = [field(td) for td in doc.findall('.//td')]
            data = {'url': res.url}
            for i, (header, text) in enumerate(tdtexts):
                if header:
                    data[FIELDS[text]] = tdtexts[i+1][1]
            return data
        except requests.exceptions.ConnectionError:
            pass


def clean_row(row):
    summe = row.get('summe')
    if len(summe):
        summe = summe.strip().split(' ')[0]
        summe = summe.replace('.', '').replace(',', '.')
        row['summe_num'] = summe
    return row


def scrape():
    for fkz in get_fkzs():
        row = table.find_one(fkz=fkz)
        if row is not None:
            continue
        row = get_by_fkz(fkz)
        row = clean_row(row)
        table.upsert(row, ['fkz'])
        log.debug("Saved: %s", fkz)


if __name__ == '__main__':
    scrape()
