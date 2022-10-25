import concurrent.futures
import json
import logging
import os
import time

import requests

ALI_URL = f'https://www.alimerkaonline.es/ali-ws/'
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

WAIT = 0.5


def jsonp2json(text: str):
    return text[3:-1]


def worker(sf: dict[str], postal_code: str, point_of_service: str, **kwargs) -> None:
    logger.info(f'Download subfamily "{sf["nombre"]}" from "{point_of_service}"')
    res = requests.get(PRODUCTS_URL.format(subfamily=sf['codsubfamilia'], cp=postal_code),
                       params=kwargs)  # GET

    res.raise_for_status()
    res = json.loads(jsonp2json(res.text))
    with open(os.path.join(d2, str(sf['codsubfamilia'])) + '.json', 'w',
              encoding='utf8') as g:
        json.dump(res, g, indent=4, ensure_ascii=False)
    time.sleep(WAIT)


if __name__ == '__main__':
    # Get timestamp
    timestamp = time.time_ns()  # nanoseconds
    cps = [33404]
    for cp in cps:
        # Get auth token
        r = requests.post(ALI_TOKEN.format(cp=cp))
        r.raise_for_status()
        r = r.json()
        iauthtoken = r['iauthtoken']
        params = {'iauthtoken': iauthtoken}

        # Point of service ids
        poss = set()

        # Get lockers
        r = requests.get(LOCKER_URL, params=params)
        r.raise_for_status()
        r = json.loads(jsonp2json(r.text))
        d = f'data/{timestamp}/{cp}/lockers/'
        os.makedirs(d)
        for section in r:
            poss.add(str(section['id']))
            with open(os.path.join(d, str(section['id'])) + '.json', 'w', encoding='utf8') as f:
                json.dump(section, f, indent=4, ensure_ascii=False)

        # Get stores
        r = requests.get(SHOP_URL, params=params)
        r.raise_for_status()
        r = json.loads(jsonp2json(r.text))
        d = f'data/{timestamp}/{cp}/stores/'
        os.makedirs(d)
        for section in r:
            poss.add(str(section['id']))
            with open(os.path.join(d, str(section['id'])) + '.json', 'w', encoding='utf8') as f:
                json.dump(section, f, indent=4, ensure_ascii=False)

        # Get the rest
        for pos in poss:
            r = requests.get(LISTING_URL.format(pos=pos, cp=cp), params=params)  # GET
            r.raise_for_status()
            r = json.loads(jsonp2json(r.text))

            d1 = f'data/{timestamp}/{cp}/sections/{pos}'
            os.makedirs(d1)
            d2 = f'data/{timestamp}/{cp}/subfamilies/{pos}'
            os.makedirs(d2)

            for section in r:  # Section
                with open(os.path.join(d1, str(section['codseccion'])) + '.json', 'w', encoding='utf8') as f:
                    json.dump(section, f, indent=4, ensure_ascii=False)

                # for family in section['familias']:

                # We can use a with statement to ensure threads are cleaned up promptly
                with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
                    future_to_subfamily = {executor.submit(worker, subfamily, cp, pos, **params): subfamily
                                           for family in section['familias'] for subfamily in
                                           family['subfamilias']}

                    for future in concurrent.futures.as_completed(future_to_subfamily):
                        url = future_to_subfamily[future]
                        try:
                            future.result()
                        except Exception as exc:
                            raise exc
