#!/usr/bin/env python3

import argparse
from Windhager import Windhager

parser = argparse.ArgumentParser()
parser.add_argument('--windhager', type=str, required=True, help='Windhager IP/Host')
parser.add_argument('--wuser', type=str, default='Service', help='Windhager Username')
parser.add_argument('--wpass', type=str, default='123', help='Windhager Password')
parser.add_argument('--debug', action='store_true', help='Activate Debug')
parser.add_argument('--ta', action='store_true', help='TA Controller')
args = parser.parse_args()

def main():
    level = 'INFO'
    if args.debug:
        level = 'DEBUG'
    w = Windhager(args.windhager, user=args.wuser, password=args.wpass, level=level, ta=args.ta)

    #print(w.id_to_string("0", "118"))
    #print(w.id_to_string("41", "106"))
    
    dps = w.get_lookup_all()
    for dp in dps:
        if 'name' not in dp:
            continue
        if 'value' not in dp:
            continue
        key = f"{dp['groupNr']}-{dp['memberNr']}"
        try:
            value = float(dp['value'])
        except:
            value = None
        oid = dp['OID']

        name = w.id_to_string(dp['groupNr'], dp['memberNr'])
        if name:
            name = name.replace(' ','-')
        #name = ""
        print(f"{oid.lstrip('/')},{key},{name},{value}")

if __name__ == '__main__':
    main()
