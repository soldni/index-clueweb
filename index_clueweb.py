# built-in modules
import re
import gzip

# installed modules
# None

# project modules
from utils import elastic


class WarcHeader(dict):
    def __init__(self):
        dict.__init__(self)
        self.__dict__ = self


class WarcRecord(dict):
    def __init__(self, raw_record):
        dict.__init__(self)
        self.__dict__ = self

        warc_hearder, html_header, content = raw_record.split('\n\n', 2)

        self._parse_header(warc_hearder)
        self._parse_header(html_header)
        self.content = content

    def _parse_header(self, raw_header):
        header_name = '_meta'

        for i, ln in enumerate(raw_header.strip().split('\n')):
            try:
                key, value = ln.split(': ', 1)
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
            self.setdefault(header_name, WarcHeader())[key] = value


class WarcFile(list):
    def __init__(self, raw_content, version='1.0'):

        content = [
            'WARC/{}{}'.format(
                version,
                raw_record.replace('\r\n', '\n')
                )
            for raw_record in
            raw_content.split('WARC/{}'.format(version))[1:]
        ]
        self.info = WarcRecord(content.pop(0))
        super(WarcFile, self).__init__(content)


def files_iterator(basepath, opts):
    for dirname in os.listdir(basepath):
        for fn_gz in os.listdir(os.path.join(basepath, dirname)):
            with gzip.open(fn_gz, mode='r', encoding='utf-8') as f:
                warc = WarcFile(f)


