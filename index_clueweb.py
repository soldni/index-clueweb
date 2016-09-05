# built-in modules
import re
import os
import gzip
import time
import chardet
import html.parser

# installed modules
from bs4 import BeautifulSoup

# project modules
from utils import elastic
from utils.meta import timer
from utils.core import Bunch
from utils.multiprocessing import pool_map


DOMAIN_RE = r'^(?:https?:\/\/)?(?:[^@\/\n]+@)?(?:www\.)?([^:\/\n]+)'
CLUEWEB_PATH = '/home/ls988/clueweb12-b13/clueweb12-b13/'
DEBUG = False

ES_HOST = 'devram4.cs.georgetown.edu'
ES_PORT = 9200
INDEX_NAME = 'clueweb12_b13'
PROGRESS_FILE = 'progress.txt'


class ClueWebIndexingError(RuntimeError):
    def __init__(self):
        super(ClueWebIndexingError, self).__init__()


def beautify(html_text):
    parser = html.parser.HTMLParser()

    html_text = html_text.replace('\\', ' ').replace('\r', '\n')
    html_text = re.sub(r'<br\\?>', '\n', html_text)
    html_text = html_text.replace('><', '> <')

    # try:
    soup = BeautifulSoup(html_text, 'html.parser')

    title = soup.find('title')
    title = parser.unescape(title.text) if title else ''

    body_soup = soup.find('body')
    if body_soup is None:
        body_soup = soup

    for script in soup.find_all('script'):
        script.decompose()

    for style in soup.find_all('style'):
        style.decompose()

    for tag in soup.find_all(True):
        tag.attrs.clear()

    body_text = body_soup.text.replace('[ \t]{2:}', '\n\n')

    return title, body_text


class WarcHeader(dict):
    def __init__(self):
        dict.__init__(self)
        self.__dict__ = self


class WarcRecord(dict):
    def __init__(self, raw_record):
        dict.__init__(self)
        self.__dict__ = self

        encoding_match = re.search(rb'charset=([a-zA-Z0-9\-]+)', raw_record)

        encoding = (
            encoding_match.group(1).decode('ascii')
            if encoding_match else 'utf-8'
        )

        try:
            raw_record = raw_record.decode(encoding)
        except(UnicodeDecodeError, LookupError):
            encoding = chardet.detect(raw_record)['encoding']
            raw_record = raw_record.decode(encoding, errors='ignore')

        warc_hearder, html_header, content = raw_record.split('\n\n', 2)

        self._parse_header(warc_hearder)
        self._parse_header(html_header)

        self.content = content.strip()

    def _parse_header(self, raw_header):
        header_name = '_meta'

        for i, ln in enumerate(raw_header.strip().split('\n')):
            try:
                key, value = ln.split(':', 1)
            except ValueError:
                header_name, metadata = ln.split('/')
                header_name = re.sub('\W+', '_', header_name)

                if re.match(r'\d\.\d \d+ \w+', header_name):
                    # HTTP header, first line has response code
                    _, resp_code, _ = metadata.split()
                    print(header_name)
                    self.setdefault(header_name, WarcHeader())['resp_code'] =\
                        resp_code

                continue

            key = re.sub('\W+', '_', key)
            self.setdefault(header_name, WarcHeader())[key] = value.strip()


class WarcFile(list):
    def __init__(self, raw_content, version='1.0'):

        warc_split = 'WARC/{}'.format(version).encode('ascii')

        content = [
            (
                warc_split +
                raw_record.replace(b'\r\n', b'\n')
            )
            for raw_record in
            raw_content.split(warc_split)[1:]
        ]
        self.info = WarcRecord(content.pop(0))

        super(WarcFile, self).__init__(
            [WarcRecord(raw_record) for raw_record in content]
        )


def files_iterator(basepath):
    if not DEBUG:
        progress = Progress(PROGRESS_FILE)

    for dirname in os.listdir(basepath):
        for fn_gz in os.listdir(os.path.join(basepath, dirname)):
            fp_gz = os.path.join(basepath, dirname, fn_gz)

            if not DEBUG and fp_gz in progress:
                continue

            with gzip.open(fp_gz) as f:
                content = f.read()
                warc = WarcFile(content)
                yield warc

            if not DEBUG:
                progress.add(fp_gz)


def extract_from_subset(basepath, status):
    files = files_iterator(basepath)

    for warc in files:
        for doc in warc:

            status.cnt += 1
            if status.cnt % 100 == 0:
                delta = time.time() - status.start
                msg = (
                    '{}: {:,} documents processed in {:.0f} s ({:.1e} s / doc)'
                    ''.format(status.id, status.cnt, delta, delta / status.cnt)
                )
                print(msg)

            if doc.content.strip():
                title, body = beautify(doc.content)
            else:
                title = body = ''

            doc = {
                '_id': doc.WARC.WARC_TREC_ID,
                'url': doc.WARC.WARC_Target_URI,
                'domain':
                    re.match(DOMAIN_RE, doc.WARC.WARC_Target_URI).group(1),
                '_type': 'document',
                'title': title,
                'body': body
            }

            yield doc


class Progress(object):
    def __init__(self, path):
        if not os.path.exists(path):
            raise RuntimeError('"{}" does not exists'.format(path))

        with open(path) as f:
            self._progress = set(f.read().strip().split())

        self.path = path

    def __contains__(self, _id):
        return self._progress.__contains__(_id)

    def add(self, _id):
        with open(path, 'a') as f:
            f.write('{}\n'.format(_id))
        self._progress.add(_id)

    @staticmethod
    def make(path, overwrite=False):
        if not os.path.exists(path) or overwrite:
            with open(path, 'w') as f:
                pass


def index_subset(basepath):

    status = Bunch(
        cnt=0, start=time.time(),
        id=re.search(r'_(\d+)', basepath.rsplit('/', 1)[1]).group(1)
    )

    es_client = elastic.get_client(
        host=ES_HOST, port=ES_PORT, timeout=120, index_name=INDEX_NAME
    )
    extracted = extract_from_subset(basepath, status)
    elastic.index_in_bulk(extracted, es_client=es_client)


def main(clueweb_fp=CLUEWEB_PATH):

    if not DEBUG:
        Progress.make(PROGRESS_FILE)

    es_client = elastic.get_client(
            host=ES_HOST, port=ES_PORT, timeout=120, index_name=INDEX_NAME
        )
    elastic.create_index(
        index_name=INDEX_NAME,
        index_settings='clueweb.json',
        es_client=es_client,
        allow_if_not_deleted=(not DEBUG)
    )

    paths = [
        os.path.join(clueweb_fp, p) for p in os.listdir(clueweb_fp)
        if os.path.isdir(os.path.join(clueweb_fp, p)) and
        'ClueWeb12_' in p
    ]

    pool_map(index_subset, [paths], single_thread=DEBUG)


if __name__ == '__main__':
    main()
