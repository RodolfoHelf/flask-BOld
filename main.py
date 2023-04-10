import pandas as pd
import json
import requests

from flask import Flask,request,Response

# constants
TOKEN = '6205998001:AAHMvft9cC-lmqTM_TPaqaRe4fz9veJ2-dM'

# https://api.telegram.org/bot6205998001:AAHMvft9cC-lmqTM_TPaqaRe4fz9veJ2-dM/getMe

# https://api.telegram.org/bot6205998001:AAHMvft9cC-lmqTM_TPaqaRe4fz9veJ2-dM/getUpdates

# https://api.telegram.org/bot6205998001:AAHMvft9cC-lmqTM_TPaqaRe4fz9veJ2-dM/setWebHook?url=https://3747fcfb0bcfb7.lhr.life


def send_message(chat_id,text):

    url = f"https://api.telegram.org/bot{TOKEN}/"
    url = url + f"sendMessage?chat_id={chat_id}"

    r = requests.post(url,json = {'text':text})

    print(f"Status Code{r.status_code}")

    return None



def load_dataset(store_id):
    # loading test dataset
    df10 = pd.read_csv( '../data/test.csv' )
    df_store_raw = pd.read_csv( '../data/store.csv', low_memory=False )

    # merge test dataset + store
    df_test = pd.merge( df10, df_store_raw, how='left', on='Store' )

    # choose store for prediction
    df_test = df_test[df_test['Store'] == store_id ]
    if not df_test.empty:
        # remove closed days
        df_test = df_test[df_test['Open'] != 0]
        df_test = df_test[~df_test['Open'].isnull()]
        df_test = df_test.drop( 'Id', axis=1 )

        # convert Dataframe to json
        data = json.dumps( df_test.to_dict( orient='records' ) )
    else:
        data = "ERROR"

    return data
    

def predict(data):

    # API Call
    # url = 'http://127.0.0.1:6005/rossmann/predict'
    url = 'https://flask-production-abe8.up.railway.app/rossmann/predict'
    header = {'Content-type': 'application/json' } 
    data = data

    r = requests.post( url, data=data, headers=header )
    print( 'Status Code {}'.format( r.status_code ) )


    d1 = pd.DataFrame( r.json(), columns=r.json()[0].keys() )

    return d1

def parse_message(message):

    chat_id = message["message"]["chat"]["id"]
    store_id = message ["message"]["text"]

    store_id = store_id.replace("/","")

    try:
        store_id = int(store_id)

    except ValueError:
        store_id = "ERROR"

    return chat_id,store_id

# API Initialize

app = Flask(__name__)

@app.route('/',methods = ['GET',"POST"])
def index():
    if request.method == 'POST':
        message = request.get_json()

        chat_id,store_id = parse_message(message)
        if store_id != "ERROR":
            #load data
            data = load_dataset(store_id)
            if data!= "ERROR":

                #prediction
                d1 = predict(data)
                #calculation
                d2 = d1[['store', 'prediction']].groupby( 'store' ).sum().reset_index()

                msg =  'Store Number {} will sell R${:,.2f} in the next 6 weeks'.format( d2['store'].values[0], d2['prediction'].values[0] ) 

                send_message(chat_id,msg)
                return Response("Ok",status=200)
 
            else:
                send_message(chat_id,"Store Not Available")
                return Response("Ok",status=200)
    
        else:
            send_message(chat_id,"Store ID is Wrong")
            return Response("Ok",status=200)


    else:
        return "<h1> Rossmann Telegram Bot </h1>"

if(__name__ == '__main__'):
    app.run(host='0.0.0.0',port = 5000)
