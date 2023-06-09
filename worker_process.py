from collections import defaultdict
from multiprocessing import Queue

# properties which encode some alias/name
import ujson

ALIAS_PROPERTIES = {'P138', 'P734', 'P735', 'P742', 'P1448', 'P1449', 'P1477', 'P1533', 'P1549', 'P1559', 'P1560',
                    'P1635', 'P1705', 'P1782', 'P1785', 'P1786', 'P1787', 'P1810', 'P1813', 'P1814', 'P1888', 'P1950',
                    'P2358', 'P2359', 'PP2365', 'P2366', 'P2521', 'P2562', 'P2976', 'PP3321', 'P4239', 'P4284',
                    'P4970', 'P5056', 'P5278', 'PP6978', 'P7383'}

# data types in wikidata dump which we ignore
IGNORE = {'wikibase-lexeme', 'musical-notation', 'globe-coordinate', 'commonsMedia', 'geo-shape', 'wikibase-sense',
          'wikibase-property', 'math', 'tabular-data'}


def process_mainsnak(data, language_id):
    datatype = data['datatype']
    if datatype == 'wikibase-item':
        return data['datavalue']['value']['id']
    elif datatype == 'monolingualtext':
        if data['datavalue']['value']['language'] == language_id:
            return data['datavalue']['value']['text']
    elif datatype == 'quantity':
        return data['datavalue']['value']['amount']
    elif datatype == 'time':
        return data['datavalue']['value']['time']


    # Ignore all other triples
    elif datatype in IGNORE:
        return None
    else:
        return None
    return None


def process_json(obj, language_id="en"):
    out_data = defaultdict(list)
    # skip properties
    if obj['type'] == 'property':
        return {}
    id = obj['id']  # The canonical ID of the entity.
    # extract labels
    if language_id in obj['labels']:
        label = obj['labels'][language_id]['value']
        out_data['labels'].append({
            'qid': id,
            'label': label
        })
        out_data['aliases'].append({
            'qid': id,
            'alias': label
        })

    # extract description
    if language_id in obj['descriptions']:
        description = obj['descriptions'][language_id]['value']
        out_data['descriptions'].append({
            'qid': id,
            'description': description,
        })

    # extract aliases
    if language_id in obj['aliases']:
        for alias in obj['aliases'][language_id]:
            out_data['aliases'].append({
                'qid': id,
                'alias': alias['value'],
            })

    return dict(out_data)


def process_data(language_id: str, work_queue: Queue, out_queue: Queue):
    while True:
        json_obj = work_queue.get()
        if json_obj is None:
            break
        if len(json_obj) == 0:
            continue
        out_queue.put(process_json(ujson.loads(json_obj), language_id))
    return
