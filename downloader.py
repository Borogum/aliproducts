#!/usr/bin/python3

import concurrent.futures
import json
import logging
import os
import time
from typing import Any
from zipfile import ZIP_DEFLATED, ZipFile

import requests
from fake_headers import Headers

ALI_URL = 'https://www.alimerkaonline.es/ali-ws/'
ALI_TOKEN = ALI_URL + 'acceso/cp/{cp}'
SHOP_URL = ALI_URL + 'tienda/direcciones'
LOCKER_URL = ALI_URL + 'tienda/direcciones/lockers'
LISTING_URL = ALI_URL + 'catalogo/secciones/familias/subfamilias/{pos}/{cp}'
PRODUCTS_URL = ALI_URL + 'catalogo/productos/subfamilia/{subfamily}/{cp}/1/1000/4'

ch = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s %(levelname)8s %(name)s | %(message)s')
ch.setFormatter(formatter)
logger = logging.getLogger(__name__)
logger.addHandler(ch)
logger.setLevel(logging.DEBUG)

WAIT = .5


def jsonp2json(text: str):
    return text[3:-1]


def worker(sf: dict[str, Any], postal_code: str, point_of_service: str, **kwargs) -> tuple[str, dict]:
    logger.info(f'Download subfamily "{sf["nombre"]}" from "{point_of_service}"')

    res = requests.get(PRODUCTS_URL.format(subfamily=sf['codsubfamilia'], cp=postal_code),
                       params=kwargs, headers=Headers().generate())  # GET

    res.raise_for_status()
    res = json.loads(jsonp2json(res.text))
    time.sleep(WAIT)
    return str(sf['codsubfamilia']) + '.json', res


if __name__ == '__main__':
    # Get timestamp
    timestamp = time.time_ns()  # nanoseconds
    cps = [33404]
    # https://superfastpython.com/multithreaded-zip-files/#Zip_Files_Concurrently_With_Threads_Without_a_Lock
    base_path = 'data'
    loc_dict = {'lockers': LOCKER_URL, 'stores': SHOP_URL}
    with ZipFile(os.path.join(base_path, f'{timestamp}.zip'), 'w', compression=ZIP_DEFLATED) as z:

        for cp in cps:
            # Get auth token
            r = requests.post(ALI_TOKEN.format(cp=cp), headers=Headers().generate())
            r.raise_for_status()
            r = r.json()
            iauthtoken = r['iauthtoken']
            params = {'iauthtoken': iauthtoken}

            # Point of service ids
            poss = set()

            for place, url in loc_dict.items():

                # Get lockers
                r = requests.get(url, params=params, headers=Headers().generate())
                r.raise_for_status()
                r = json.loads(jsonp2json(r.text))

                for section in r:
                    poss.add(str(section['id']))
                    z.writestr(os.path.join(f'{cp}/{place}/', str(section['id'])) + '.json',
                               json.dumps(section, ensure_ascii=False))

            # Get the rest
            for pos in poss:
                r = requests.get(LISTING_URL.format(pos=pos, cp=cp), params=params, headers=Headers().generate())  # GET
                r.raise_for_status()
                r = json.loads(jsonp2json(r.text))

                for section in r:  # Section
                    z.writestr(f'{cp}/sections/{pos}/{str(section["codseccion"]) + ".json"}',
                               json.dumps(section, ensure_ascii=False))

                    # We can use a with statement to ensure threads are cleaned up promptly
                    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
                        future_to_subfamily = {executor.submit(worker, subfamily, cp, pos, **params): subfamily
                                               for family in section['familias'] for subfamily in
                                               family['subfamilias']}

                        for future in concurrent.futures.as_completed(future_to_subfamily):
                            url = future_to_subfamily[future]
                            try:
                                p, j = future.result()
                                z.writestr(os.path.join(f'{cp}/subfamilies/{pos}', p),
                                           json.dumps(j, ensure_ascii=False))
                            except Exception as exc:
                                raise exc
