from flask import Flask
import flask
import os
import json
import logging

import sys
import time

sys.path.insert(0, '/opt/program/SimpleRecSys')
from SimpleRecSys import SimpleRecSys

#Load in model (using spaCy)
#import spacy
#nlp = spacy.load('en_core_web_sm') 

model = SimpleRecSys.SimpleRecSys()

#If you plan to use a your own model artifacts, 
#your model artifacts should be stored in /opt/ml/model/ 

# The flask app for serving predictions
app = Flask(__name__)
@app.route('/ping', methods=['GET'])
def ping():
    # Check if the classifier was loaded correctly
    health = model is not None
    status = 200 if health else 404
    return flask.Response(response= '\n', status=status, mimetype='application/json')


@app.route('/invocations', methods=['POST'])
def transformation():
    
    #Process input
    input_json = flask.request.get_json()
    resp = input_json['input']
    
    #recommendation
    # assumes resp will be a dict
    result = model.predict(resp)

    resultjson = json.dumps(result)
    return flask.Response(response=resultjson, status=200, mimetype='application/json')
