# XES MQTT Publisher

## Installation

Install packages: ```pip install -r requirements.txt```. If you get a graphviz error when running the app, re-install pm4py with ```pip install -U pm4py```.

## Running

Run ```main.py``` with three arguments, in this order: 
1. The relative or absolute path of the XES file
2. The name of the MQTT broker (e.g. ```broker.hivemq.com```)
3. The port of the MQTT broker (e.g. ```1883```)
4. The base topic to publish to (e.g. ```02269_pm_group9```)
5. The delay between sending single events (e.g. ```0.5``` for a delay of 0.5 seconds)