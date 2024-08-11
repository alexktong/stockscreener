#!/bin/bash

# Download stock constituents for exchanges
/usr/bin/python3 /home/alexktong_92/python/stockscreener/input/scrapers/get_asx_constituents.py
/usr/bin/python3 /home/alexktong_92/python/stockscreener/input/scrapers/get_hkex_constituents.py
/usr/bin/python3 /home/alexktong_92/python/stockscreener/input/scrapers/get_sgx_constituents.py
/usr/bin/python3 /home/alexktong_92/python/stockscreener/input/scrapers/get_us_constituents.py