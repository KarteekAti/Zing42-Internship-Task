from datetime import date,datetime
from jugaad_data.nse import bhavcopy_save
import pandas as pd
from jugaad_data.holidays import holidays
import time, os
import psycopg2
import csv
from flask import Flask, redirect ,render_template, request, jsonify, url_for

# Flask app initialization
app = Flask(__name__,instance_relative_config=False)
app.secret_key = os.urandom(24)

#Database Connection
hostname = 'localhost'
database = 'Zing42'
username = 'postgres'
pwd = '23212'
port_id = 5432
conn = psycopg2.connect(host = hostname,dbname = database,user = username,password = pwd,port = port_id)
cur = conn.cursor()

#Replacement of datatype for Database
replacements = {
            'timedelta64[ns]': 'varchar',
            'object': 'varchar',
            'float64': 'float',
            'int64': 'int',
            'datetime64': 'timestamp'
        }

def get_Equity():
    df1 = pd.read_csv("https://archives.nseindia.com/content/equities/EQUITY_L.csv")
    df1.columns = [x.lower().lstrip().replace(" ", "_") for x in df1.columns]
    col_str = ", ".join("{} {}".format(n, d) for (n, d) in zip(df1.columns, df1.dtypes.replace(replacements)))
    df1.to_csv('Equity_segment.csv',index = False)
    return col_str

def Equity_to_DB():
    col_str = get_Equity()
    try:
        cur.execute("create table %s (%s);" %("Equity_Segment",col_str))
    except Exception as e:
        print(e)
    with open('Equity_segment.csv','r') as f:
        reader = csv.reader(f)
        next(reader)
        try:
            for row in reader:
                cur.execute("INSERT INTO equity_segment VALUES (%s, %s, %s, %s,%s,%s,%s,%s)",
                row)
        except Exception as e:
            print(e)
        conn.commit()

def bhav_to_DB(symbol,dates_list,sdate,edate):
    temp_df = pd.read_csv("bhavcopy\cm02Mar2022bhav.csv")
    col_str = ", ".join("{} {}".format(n, d) for (n, d) in zip(temp_df.columns, temp_df.dtypes.replace(replacements)))
    cur.execute("create table if not exists %s (%s);" %("Bhavcopy",col_str))
    try:
        for dates in dates_list: 
            d = dates.strftime("%d%b%Y")
            with open("bhavcopy\cm"+d+"bhav.csv",'r') as f:
                reader = csv.reader(f)
                next(reader)
                for row in reader:
                    cur.execute("INSERT INTO Bhavcopy VALUES (%s, %s, %s, %s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                        row)
                conn.commit()
    except psycopg2.DatabaseError as e:
            print(e)
    return redirect(url_for("showData",symbol = symbol, sdate = sdate, edate = edate))


@app.route('/')
def index():
    return render_template('index.html')

@app.route('/getbhav',methods=['POST','GET'])
def get_Bhav():
    if request.method == "POST":
        symbol = request.form['symbol']
        sdate = request.form['sdate']
        edate = request.form['edate']
        date_range = pd.bdate_range(start=sdate, end = edate, 
                                freq='C', holidays= holidays(2022))
        savepath = os.path.join('E:', os.sep, 'Tech','Python','Zing42-Internship','bhavcopy')
        dates_list = [x.date() for x in date_range]
        for dates in dates_list:
            try:
                bhavcopy_save(dates, savepath)
                d = dates.strftime("%d%b%Y")
                df = pd.read_csv("bhavcopy\cm"+d+"bhav.csv")
                df.drop('Unnamed: 13',axis = 1,inplace = True)
                df.columns = [x.lower().lstrip().replace(" ", "_") for x in df.columns]
                df.to_csv("bhavcopy\cm"+d+"bhav.csv", index=False)
            except Exception as e:
                try:
                    bhavcopy_save(dates, savepath)
                    d = dates.strftime("%d%b%Y")
                    df = pd.read_csv("bhavcopy\cm"+d+"bhav.csv")
                    df.drop('Unnamed: 13',axis = 1,inplace = True)
                    df.columns = [x.lower().lstrip().replace(" ", "_") for x in df.columns]
                    df.to_csv("bhavcopy\cm"+d+"bhav.csv",index=False)
                except Exception as e:
                    print(f'{dates}: File not Found')
        return bhav_to_DB(symbol,dates_list,sdate,edate)
    else:                
        return render_template('index.html')

@app.route('/showData/<symbol>?<sdate>?<edate>',methods=['POST','GET'])
def showData(symbol,sdate,edate):
    sdate = datetime.strptime(sdate,'%Y-%m-%d').strftime("%d-%b-%Y").upper()
    edate = datetime.strptime(edate,'%Y-%m-%d').strftime("%d-%b-%Y").upper()
    cur.execute("""SELECT * FROM bhavcopy WHERE timestamp >= %s AND timestamp <= %s AND symbol = %s """,[sdate,edate,symbol])
    query = cur.fetchall()
    cur.execute(""" DROP TABLE bhavcopy """)
    conn.commit()
    print(query)
    return render_template('showQuery.html',query = query)

if __name__ == '__main__':
    app.run(debug=True)