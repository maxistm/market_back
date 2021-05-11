from flask import Flask
from flask import render_template, request, jsonify
from flask_cors import CORS, cross_origin
#from data_test import data
import main

app = Flask(__name__)
cors = CORS(app)


@app.route('/api/get/<ticket>',methods=['GET', 'POST'])
def get_ticket(ticket=None):
    #lenght = main.reload()
    content = request.json
    result = {}
    result['ohlcv'] = main.get_ticket(content['ticket'], content['startDate'], content['stopDate'])
    return jsonify(result)

@app.route('/api/get_tickets')
def get_tickets():
    response = jsonify(main.tickets)
    return response


@app.route('/api/reload',methods=['GET', 'POST'])
def reload():
    lenght = main.reload()
    res = {}
    res['down_count'] = 'download count :' + str(lenght)
    res['tickets'] = main.tickets
    response = jsonify(res)
    return response

if __name__ == '__main__':    
    app.run(debug=True)

