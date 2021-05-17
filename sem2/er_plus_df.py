import datetime
import json
import os

from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import RawValueProtocol, JSONValueProtocol

def _log(text):
    logfile = '/Users/agasiev/bigdata_msu/sem2/debug.log'
    with open(logfile, 'a') as file:
        print(datetime.datetime.now().isoformat(), '\t', str(text), file=file)

composers = {}

def format_composer(record):
    names = record['full_name'].split('(')
    return dict(
        record_type='composer',
        name_rus=names[0].strip(),
        name_en=names[1].strip(')') if len(names) > 1 else "",
        birth_date=record['birth_date'],
        death_date=record['death_date'],
        country=record['country'],
    )

def format_sheet(record):
    return dict(
        record_type='sheet',
        title=record['title'],
        description=record['description'],
        library_info=record['library_info'],
        publication_year=record['publication_year'],
        author=record['author'],
    )

class MRERnDF(MRJob):

    OUTPUT_PROTOCOL = RawValueProtocol
    INPUT_PROTOCOL = JSONValueProtocol

    def steps(self):
        return [
            MRStep(
                mapper=self.mapper_entity_resolution,
                reducer=self.reducer_data_fusion,
            ),
        ]

    # record => composer_id, formatted_record
    def mapper_entity_resolution(self, _, record):
        filename = os.environ['map_input_file']
        if 'composer' in filename:
            composer_id = len(composers)
            name = record['full_name']
            keyword = name.split('(')[0].strip().split()[-1]
            composers[composer_id] = dict(keyword=keyword, name=name)
            yield composer_id, format_composer(record)
        else: # sheets
            for cid, info in composers.items():
                if info['keyword'] in record['description'] or info['keyword'] in record['author']:
                    yield cid, format_sheet(record)

    # composer_id, [formatted_records] => None, entity
    def reducer_data_fusion(self, composer_id, records):
        result = dict(
            composer_id=composer_id,
            sheets=[],
        )
        for r in records:
            if r['record_type'] == 'composer':
                result['name_rus']=r['name_rus']
                result['name_en']=r['name_en']
                result['birth_date']=r['birth_date']
                result['death_date']=r['death_date']
                result['country']=r['country']
            else: # sheet
                result['sheets'].append(dict(
                    description=r['description'],
                    library_info=r['library_info'],
                    publication_year=r['publication_year'],
                    author=r['author'],
                    title=r['title'],
                ))
        yield None, json.dumps(result, ensure_ascii=False)
        
if __name__ == '__main__':
    MRERnDF.run()
