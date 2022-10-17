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


def jsonp2json(text: str):
    return text[3:-1]


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
            r = requests.get(LISTING_URL.format(pos=pos, cp=cp), params=params)
            r.raise_for_status()
            r = json.loads(jsonp2json(r.text))

            d1 = f'data/{timestamp}/{cp}/sections/{pos}'
            os.makedirs(d1)
            d2 = f'data/{timestamp}/{cp}/subfamilies/{pos}'
            os.makedirs(d2)

            for section in r:  # Section
                with open(os.path.join(d1, str(section['codseccion'])) + '.json', 'w', encoding='utf8') as f:
                    json.dump(section, f, indent=4, ensure_ascii=False)

                for family in section['familias']:
                    for subfamily in family['subfamilias']:
                        logger.info(f'Download subfamily "{subfamily["nombre"]}" from "{pos}"')
                        r = requests.get(PRODUCTS_URL.format(subfamily=subfamily['codsubfamilia'], cp=cp),
                                         params=params)
                        r.raise_for_status()
                        r = json.loads(jsonp2json(r.text))
                        with open(os.path.join(d2, str(subfamily['codsubfamilia'])) + '.json', 'w',
                                  encoding='utf8') as f:
                            json.dump(r, f, indent=4, ensure_ascii=False)
                        time.sleep(0.1)
