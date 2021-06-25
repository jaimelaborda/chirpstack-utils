#!/usr/bin/python3
# File: chirpstack-bulk-import.py
# Purpose: Bulk import of CSV devices file into a ChirpStack application
# Usage: ./chirpstack-bulk-import.py -i devices.csv -s host:port -t API_TOKEN -a 2
# Author: J. Laborda - ITACA UPV uRIS
# ------------------------------------------

import os
import sys
import getopt

import csv
import requests
import json

# Configuration.

def main(argv):
    
    # This must point to the API interface.
    server = "localhost:8080"

    # The API token (retrieved using the web-interface).
    api_token = ""

    inputfile = ""
    app_id = 0

    # Check input parameters
    try:
        opts, args = getopt.getopt(argv,"hi:s:t:a:",["help", "ifile=", "server=", "token=", "application="])
    except getopt.GetoptError:
        print("""
            usage: chirpstack-bulk-import.py [-h] -i <inputfile> -s <server:port> -t <API_TOKEN> -a <APPLICATION_ID>

            optional arguments:
            -h, --help  show this help message and exit
        """)
        sys.exit(2)

    for opt, arg in opts:
        #print(opt)
        if opt in ('-h, --help'):
            print("""
            usage: chirpstack-bulk-import.py [-h] -i <inputfile> -s <server:port> -t <API_TOKEN> -a <APPLICATION_ID>

            optional arguments:
            -h, --help  show this help message and exit
            """)
            sys.exit()
        elif opt in ("-i", "--ifile"):
            inputfile = arg
            #print("Input file: "+inputfile)
        elif opt in ("-s", "--server"):
            server = arg
            #print("Server: "+server)
        elif opt in ("-t", "--token"):
            api_token = arg
            #print("API Token: "+api_token)
        elif opt in ("-a", "--application"):
            app_id = arg
            #print("App ID: "+app_id)

    # Abro el fichero csv
    with open(inputfile) as csvfile:
        csvreader = csv.DictReader(csvfile, delimiter=',')
        line_count = 0
        sensors_count = 0
        sensors_error = 0
        for row in csvreader:
            # Lo recorro linea a linea
            if line_count == 0:
                print(f'Column names are {", ".join(row)}')
                line_count += 1
            
            # Hago una petición a la api para dar de alta cada dispositivo mediante CURL
            payload = {
                "device": {
                    "applicationID": app_id,
                    "description": row["description"],
                    "devEUI": row["deveui"],
                    "deviceProfileID": row["devprofile"],
                    "isDisabled": False,
                    "name": row["\ufeffname"],
                    "referenceAltitude": 0,
                    "skipFCntCheck": False,
                    "tags": {
                        "tipo": row["tag.tipo"],
                        "complejo": row["tag.complejo"],
                        "cont": row["tag.cont"],
                        "edificio": row["tag.edificio"]
                    },
                    "variables": {}
                }
            }

            # If tags available
            # TO-DO: If column name prefix is "tag." then concatenate to a new tag
            # Ex: tag.tipo,tag.complejo,tag.cont,tag.edificio
            #print(type(payload))
            #print(payload)
            #print(payload["device"]["tags"])

            headers = {
                "Content-Type": "application/json",
                "Accept": "application/json",
                "Grpc-Metadata-Authorization": "Bearer "+api_token,
            }

            # Make the API request
            r = requests.post(f'http://{server}/api/devices', json=payload, headers=headers)

            if(r.status_code == 200):
                # Dado de alta correctamente
                # Dar de alta la key

                payload_key = {
                    "deviceKeys": {
                        "nwkKey": row["appkey"],
                        "devEUI": row["deveui"]
                    }
                }

                r = requests.post(f'http://{server}/api/devices/{row["deveui"]}/keys', json=payload_key, headers=headers)

                if(r.status_code == 200):
                    print(f'Device {row["deveui"]} has been succesfully configured.')
                    sensors_count += 1
                else:
                    print(f'Device {row["deveui"]} key is not properly configured. Error: {r.status_code}')
                    sensors_error += 1

            else:
                print(f'Device {row["deveui"]} not properly configured. Error: {r.status_code}')
                sensors_error += 1

            line_count += 1
        print(f'Succesfully processed {sensors_count} sensors.')

if __name__ == "__main__":
    main(sys.argv[1:])
