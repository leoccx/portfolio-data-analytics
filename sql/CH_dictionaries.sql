-- создание словарей

CREATE DICTIONARY std14_135.ch_price_dict
(
    material String,
    region String,
    distr_chan String,
    price Int32
)
PRIMARY KEY material, region, distr_chan
SOURCE(POSTGRESQL(HOST '192.168.214.203' PORT 5432 USER 'std14_135' PASSWORD '1pRDl54AqDKpy5' DB 'adb' TABLE 'std14_135.price'))
LIFETIME(MIN 0 MAX 300)
LAYOUT(COMPLEX_KEY_HASHED());

CREATE DICTIONARY std14_135.ch_chanel_dict
(
    distr_chan Int32,
    txtsh String
)
PRIMARY KEY distr_chan
SOURCE(POSTGRESQL(HOST '192.168.214.203' PORT 5432 USER 'std14_135' PASSWORD '1pRDl54AqDKpy5' DB 'adb' TABLE 'std14_135.chanel'))
LIFETIME(MIN 0 MAX 300)
LAYOUT(HASHED());

CREATE DICTIONARY std14_135.ch_product_dict
(
    material Int32,
    asgrp Int32,
    brand Int32,
    matcateg String,
    matdirec Int32,
    txt String
)
PRIMARY KEY material
SOURCE(POSTGRESQL(HOST '192.168.214.203' PORT 5432 USER 'std14_135' PASSWORD '1pRDl54AqDKpy5' DB 'adb' TABLE 'std14_135.product'))
LIFETIME(MIN 0 MAX 300)
LAYOUT(HASHED());

DROP DICTIONARY std14_135.ch_chanel_dict;

SHOW DICTIONARIES FROM std14_135;

CREATE DICTIONARY std14_135.ch_region_dict
(
    region String,
    txt String
)
PRIMARY KEY region
SOURCE(POSTGRESQL(HOST '192.168.214.203' PORT 5432 USER 'std14_135' PASSWORD '1pRDl54AqDKpy5' DB 'adb' TABLE 'std14_135.region'))
LIFETIME(MIN 0 MAX 300)
LAYOUT(COMPLEX_KEY_HASHED());

SELECT name, status, source
FROM system.dictionaries
WHERE name = 'std14_135.ch_region_dict';
