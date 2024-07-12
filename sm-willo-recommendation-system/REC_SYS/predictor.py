from flask import Flask
import flask
import os
import json
import logging

import sys
import time

sys.path.insert(0, '/opt/program/SimpleRecSys')
from SimpleRecSys import SimpleRecSys

class NumpyEncoder(json.JSONEncoder):
    """ Custom encoder for numpy data types """
    def default(self, obj):
        if isinstance(obj, (np.int_, np.intc, np.intp, np.int8,
                            np.int16, np.int32, np.int64, np.uint8,
                            np.uint16, np.uint32, np.uint64)):

            return int(obj)

        elif isinstance(obj, (np.float_, np.float16, np.float32, np.float64)):
            return float(obj)

        elif isinstance(obj, (np.ndarray,)):
            return obj.tolist()

        elif isinstance(obj, (np.bool_)):
            return bool(obj)

        elif isinstance(obj, (np.void)): 
            return None

        return json.JSONEncoder.default(self, obj)

#Load in model (using spaCy)
#import spacy
#nlp = spacy.load('en_core_web_sm') 

model = SimpleRecSys()
# Only works in container? local test needs SimpleRecSys.SimpleRecSys()

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

    # resultjson = json.dumps(result)
    resultjson = json.dumps(out, cls=NumpyEncoder)
    return flask.Response(response=resultjson, status=200, mimetype='application/json')
