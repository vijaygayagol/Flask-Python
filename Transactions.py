import os
import re
import csv, json
#import io
import pandas as pd
#import time
from flask import Flask
from flask import jsonify
from datetime import datetime, timedelta
from threading import Timer
#from threading import Event, Thread


class Repository:
    def __init__(self):

        self.transactions = []
        self.products = []
        self.dFrame = pd.DataFrame(self.transactions)
        self.dFrameProducts = pd.DataFrame(self.products)

    def getProductNameById(self, productId):
        productDetails = self.dFrameProducts[(self.dFrameProducts['productId'] == productId)]
        productDetails = json.loads(productDetails.iloc[0].to_json())
        return productDetails['productName']

    def getTransactionById(self, transactionId):
        try:
            tdata = self.dFrame[(self.dFrame['transactionId'] == transactionId)]
            response = json.loads(tdata.iloc[0].to_json())
            response['transactionDatetime'] = pd.to_datetime(response['transactionDatetime']/ 1000.0, unit='s').strftime(
                "%Y-%m-%d %H:%M:%S")
            response['productName'] = self.getProductNameById(tdata.iloc[0].productId)
            return jsonify(response)
        except IndexError as error:
            return jsonify({"error": "parameter transaction Id : please enter valid value"})
        except ValueError as error:
            return jsonify({"error": "parameter transaction Id : please enter a valid Integer value"})
        except  Exception as ex:
            return jsonify({"error": "parameter transaction Id : please enter integer value"})


    def getManCityNameById(self, productId):
        productDetails = self.dFrameProducts[(self.dFrameProducts['productId'] == productId)]
        productDetails = json.loads(productDetails.iloc[0].to_json())
        return productDetails['productManufacturingCity']


    def transactionSummaryByProducts(self, lastndays):
        startDate = pd.to_datetime(datetime.today() - timedelta(days=lastndays))
        endDate = pd.to_datetime(datetime.now())
        resultset = self.dFrame[
            (self.dFrame['transactionDatetime'] >= startDate) & (self.dFrame['transactionDatetime'] <= endDate)].groupby(
            'productId')[['transactionAmount']].sum()

        responseArray = []

        for indx, row in resultset.iterrows():
            respObj = {}
            respObj['productName'] = self.getProductNameById(indx)
            respObj['totalAmount'] = float(row['transactionAmount'])
            responseArray.append(respObj)

        return jsonify({'summary': responseArray})

    def transactionSummaryByManufacturingCity(self, lastndays):
        startDate = pd.to_datetime(datetime.today() - timedelta(days=lastndays))
        endDate = pd.to_datetime(datetime.now())
        transet = self.dFrame[(self.dFrame['transactionDatetime'] > startDate) & (self.dFrame['transactionDatetime'] < endDate)]

        responseArray = []

        for indx, row in transet.iterrows():
            respObj = {}
            respObj['cityName'] = self.getManCityNameById(int(row['productId']))
            respObj['transactionAmount'] = float(row['transactionAmount'])
            responseArray.append(respObj)

        #print(responseArray)

        resultset = pd.DataFrame(responseArray)

        responseArray = []
        if resultset.empty:
            return jsonify({'summary': responseArray})
        else:
            resultset = resultset.groupby('cityName')[['transactionAmount']].sum()

            print(resultset)

            for indx, row in resultset.iterrows():
                respObj = {}
                respObj['cityName'] = indx
                respObj['totalAmount'] = float(row['transactionAmount'])
                responseArray.append(respObj)

            return jsonify({'summary': responseArray})

    def loadproducts(self):
        path = "E:/"
        tempProd = []

        for filename in os.listdir(path):
            if filename == "ProductReference.csv":
                with open(os.path.join(path, filename), 'r') as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        tempobj = dict(row)
                        tempobj['productId'] = int(tempobj['productId'])
                        tempobj['productName'] = tempobj['productName']
                        tempobj['productManufacturingCity'] = tempobj['productManufacturingCity']
                        tempProd.append(tempobj)
                print(filename)
        self.products = tempProd

        self.dFrameProducts = pd.DataFrame(self.products)
        # init variables
        return self.products, self.dFrameProducts

    def loadtrans(self):
        path = "E:/"
        temptransactions = []

        for filename in os.listdir(path):
            if re.match(r"^Transaction_*", filename):
                with open(os.path.join(path, filename), 'r') as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        tempobj = dict(row)
                        tempobj['transactionId'] = int(tempobj['transactionId'])
                        tempobj['productId'] = int(tempobj['productId'])
                        tempobj['transactionAmount'] = float(tempobj['transactionAmount'])
                        tdatetime = datetime.strptime(tempobj['transactionDatetime'], "%d/%m/%Y %H:%M").strftime(
                            "%Y-%m-%d %H:%M:%S")
                        tempobj['transactionDatetime'] = pd.to_datetime(tdatetime)
                        # print(tempobj['transactionDatetime'] .strftime("%d"))
                        # print(tempobj['transactionDatetime'].strftime("%m"))
                        # print(tempobj['transactionDatetime'].strftime("%Y"))
                        temptransactions.append(tempobj)
                print(filename)
        self.transactions = temptransactions
        self.dFrame = pd.DataFrame(self.transactions)
        # init variables
        return self.dFrame, self.transactions

    def loaddata(self, starttime, endtime):
        path ="E:/"
        temptransactions = []
        tempProd = []

        for filename in os.listdir(path):
            if re.match(r"^Transaction_*", filename):
                #dt=os.path.getmtime(os.path.join(path, filename))
                #dt=pd.to_datetime(dt/1000.0, unit='s')
                #filemodified = datetime.utcfromtimestamp(dt)
                #if ((filemodified > starttime) & (filemodified <= endtime)):
                    with open(os.path.join(path, filename), 'r') as f:
                        reader = csv.DictReader(f)
                        for row in reader:
                            tempobj = dict(row)
                            tempobj['transactionId'] = int(tempobj['transactionId'])
                            tempobj['productId'] = int(tempobj['productId'])
                            tempobj['transactionAmount'] = float(tempobj['transactionAmount'])
                            tdatetime = datetime.strptime(tempobj['transactionDatetime'], "%d/%m/%Y %H:%M").strftime("%Y-%m-%d %H:%M:%S")
                            tempobj['transactionDatetime'] = pd.to_datetime(tdatetime)
                            #print(tempobj['transactionDatetime'] .strftime("%d"))
                            #print(tempobj['transactionDatetime'].strftime("%m"))
                            #print(tempobj['transactionDatetime'].strftime("%Y"))
                            temptransactions.append(tempobj)
                    print(filename)
        self.transactions = self.transactions.append(temptransactions)

        self.dFrame = pd.DataFrame(self.transactions)

        # init variables
        return self.dFrame, self.transactions, self.products, self.dFrameProducts

repo = Repository()


class TReload:

    def __init__(self, t, Function):
        self.t = t
        self.hFunction = Function
        self.thread = Timer(self.t, self.hFunction)



    def start(self):
        self.thread.start()


def refresh():
    endtime = datetime.now()
    starttime = (datetime.now() - timedelta(minutes=5))
    print('before refreshing')
    repo.loaddata(starttime,endtime)
    print('after refreshing the database')


if __name__ == '__main__':
    app = Flask(__name__)


    @app.errorhandler(Exception)
    def handle_bad_request(e):
        return 'bad request!', 404

    @app.route("/assignment/transaction/<transactionId>", methods=["GET"])
    def spec(transactionId):
        try:
            return print(repo.getTransactionById(transactionId))
        except ValueError as error:
            return jsonify({"error": "parameter transaction Id : please enter a valid Integer value"})


    @app.route("/assignment/transactionSummaryByProducts/<lastDays>", methods=["GET"])
    def summary_p(lastDays):
        return repo.transactionSummaryByProducts(int(lastDays))


    @app.route("/assignment/transactionSummaryByManufacturingCity/<lastDays>", methods=["GET"])
    def summary_c(lastDays):
        return repo.transactionSummaryByManufacturingCity(int(lastDays))


    repo.loadproducts()
    repo.loadtrans()
    refresh()
    t = TReload(300, refresh)
    t.start()
    app.run(port=8080,use_reloader=False,debug=True)

