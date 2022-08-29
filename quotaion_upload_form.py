import math
import datetime
from re import L
import numpy as np
import pandas as pd
import streamlit as st
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import IntegrityError
from authentication import auth
import constants
import json
import sys
import random
import requests
import time
from datetime import timedelta
import os
#os.system('/usr/local/bin/aws s3 cp <local-file-path> s3://avniro-datalake/<folder-path-on-s3>')




def logout():
    st.session_state["password"] = ""
    st.session_state["user_name"] = ""


def upsert_records(engine,session, df):

    
    print("Going to upsert records in Big Mac Index")
    print(df)
    df.fillna("", inplace=True)
    tuples = [tuple(x) for x in df.to_numpy()]
    tuples = ', '.join(map(str, tuples))

    cols = ','.join(list(df.columns))
    
    #this is for reference
    source_column_list = {"big_mac_index": ['timestamp_', 'updated_by', 'Country', 'Module', 'Tier',
        'INR Rate', 'United States', 'Switzerland', 'Norway', 'Nigeria', 'Sweden', 'Uruguay', 'Israel',
        'Canada', 'Venezuela', 'Euro area', 'Britain', 'Denmark', 'New Zealand', 'United Arab Emirates',
        'Australia', 'Singapore', 'Brazil', 'Argentina', 'Sri Lanka', 'Kuwait', 'Costa Rica', 'Czech Republic',
        'Kenya', 'Saudi Arabia', 'Bahrain', 'Chile', 'Thailand', 'China', 'South Korea', 'Nicaragua', 'Honduras',
        'Qatar', 'Croatia', 'Poland', 'Guatemala', 'Japan', 'Peru', 'Pakistan', 'Mexico', 'Colombia', 'Lebanon',
        'Hungary','Vietnam', 'Jordan', 'Oman', 'Moldova', 'Hong Kong', 'Philippines', 'Taiwan', 'Egypt', 'Azerbaijan',
        'South Africa', 'India', 'Ukraine', 'Romania', 'Malaysia', 'Indonesia', 'Turkey', 'Russia']}

    
    with engine.connect().execution_options(autocommit=True) as conn:
        conn.execute('''
    INSERT INTO big_mac_index (timestamp_, updated_by, "Country",
     "Module", "Tier", "INR Rate", "United States", "Switzerland",
      "Norway", "Nigeria", "Sweden", "Uruguay", "Israel", "Canada",
       "Venezuela", "Euro area", "Britain", "Denmark", "New Zealand",
        "United Arab Emirates", "Australia", "Singapore", "Brazil", "Argentina",
         "Sri Lanka", "Kuwait", "Costa Rica", "Czech Republic", "Kenya", "Saudi Arabia",
          "Bahrain", "Chile", "Thailand", "China", "South Korea", "Nicaragua", "Honduras",
           "Qatar", "Croatia", "Poland", "Guatemala", "Japan", "Peru", "Pakistan", "Mexico",
            "Colombia", "Lebanon", "Hungary", "Vietnam", "Jordan", "Oman", "Moldova", "Hong Kong",
             "Philippines", "Taiwan", "Egypt", "Azerbaijan", "South Africa", "India", "Ukraine",
              "Romania", "Malaysia", "Indonesia", "Turkey", "Russia") VALUES {}
    ON CONFLICT("Module","Tier")
    DO UPDATE SET 
    timestamp_ = EXCLUDED.timestamp_,
    "Country" = EXCLUDED."Country",
    "INR Rate" = EXCLUDED."INR Rate",
    "United States" = EXCLUDED."United States",
    "Switzerland" = EXCLUDED."Switzerland",
    "Norway" = EXCLUDED."Norway",
    "Nigeria" = EXCLUDED."Nigeria",
    "Sweden" = EXCLUDED."Sweden",
    "Uruguay" = EXCLUDED."Uruguay",
    "Israel" = EXCLUDED."Israel",
    "Canada" = EXCLUDED."Canada",
    "Venezuela" = EXCLUDED."Venezuela",
    "Euro area" = EXCLUDED."Euro area",
    "Britain" = EXCLUDED."Britain",
    "Denmark" = EXCLUDED."Denmark",
    "New Zealand" = EXCLUDED."New Zealand",
    "United Arab Emirates" = EXCLUDED."United Arab Emirates",
    "Australia" = EXCLUDED."Australia",
    "Singapore" = EXCLUDED."Singapore",
    "Brazil" = EXCLUDED."Brazil",
    "Argentina" = EXCLUDED."Argentina",
    "Sri Lanka" = EXCLUDED."Sri Lanka",
    "Kuwait" = EXCLUDED."Kuwait",
    "Costa Rica" = EXCLUDED."Costa Rica",
    "Czech Republic" = EXCLUDED."Czech Republic",
    "Kenya" = EXCLUDED."Kenya",
    "Saudi Arabia" = EXCLUDED."Saudi Arabia",
    "Bahrain" = EXCLUDED."Bahrain",
    "Chile" = EXCLUDED."Chile",
    "Thailand" = EXCLUDED."Thailand",
    "China" = EXCLUDED."China",
    "South Korea" = EXCLUDED."South Korea",
    "Nicaragua" = EXCLUDED."Nicaragua",
    "Honduras" = EXCLUDED."Honduras",
    "Qatar" = EXCLUDED."Qatar",
    "Croatia" = EXCLUDED."Croatia",
    "Poland" = EXCLUDED."Poland",
    "Guatemala" = EXCLUDED."Guatemala",
    "Japan" = EXCLUDED."Japan",
    "Peru" = EXCLUDED."Peru",
    "Pakistan" = EXCLUDED."Pakistan",
    "Mexico" = EXCLUDED."Mexico",
    "Colombia" = EXCLUDED."Colombia",
    "Lebanon" = EXCLUDED."Lebanon",
    "Hungary" = EXCLUDED."Hungary",
    "Vietnam" = EXCLUDED."Vietnam",
    "Jordan" = EXCLUDED."Jordan",
    "Oman" = EXCLUDED."Oman",
    "Moldova" = EXCLUDED."Moldova",
    "Hong Kong" = EXCLUDED."Hong Kong",
    "Philippines" = EXCLUDED."Philippines",
    "Taiwan" = EXCLUDED."Taiwan",
    "Egypt" = EXCLUDED."Egypt",
    "Azerbaijan" = EXCLUDED."Azerbaijan",
    "South Africa" = EXCLUDED."South Africa",
    "India" = EXCLUDED."India",
    "Ukraine" = EXCLUDED."Ukraine",
    "Romania" = EXCLUDED."Romania",
    "Malaysia" = EXCLUDED."Malaysia",
    "Indonesia" = EXCLUDED."Indonesia",
    "Turkey" = EXCLUDED."Turkey",
    "Russia" = EXCLUDED."Russia"
    '''.format(tuples))



    
    rows = df.shape[0]
    my_bar = st.progress(0)
    #df_split = split_dataframe(df, 5)
    for i in range(rows):
        time.sleep(0.02)
        my_bar.progress((i+1)/rows)
    

    


    # byte_length = str(sys.getsizeof(constants.slack_data_file_upload_successful))
    # headers = {'Content-Type': "application/json", 'Content-Length': byte_length}
    # response = requests.post(constants.url, data=json.dumps(constants.slack_data_file_upload_successful),headers=headers)
    st.success("{} Records upserted successfully".format(rows))
    exit_button = st.button(label="Exit", on_click=logout)



def csv_db(engine,session):
    with st.form(key='my_form', clear_on_submit=True):
        st.title("Price Quotataion Upload")
        st.subheader("Enter Details Below")
        st.date_input("Select date")

        #st.title('Please Upload CSV file with Leads')

        file = st.file_uploader("Upload pricing quote CSV file here", type=['csv'])
        submit_button = st.form_submit_button(label='Submit')
        st.session_state["is_form_submitted"]=submit_button
    if file is not None and st.session_state.get("is_form_submitted",False):
        try:
            df = pd.read_csv(file)
            df = df.replace(np.nan, '')  #check later
            df['timestamp_'] = str(datetime.datetime.now())
            #+ timedelta(hours= 5,minutes= 30)
            upsert_records(engine,session, df)
            timestamp_ = str(datetime.datetime.now())
            
            try:
                #filename = os.rename(df,timestamp_+'.csv')
                #from_ = '/home/centos/ph-pricing-quotations/{}'.format(filename)
                from_ = '/Users/harish/Documents/pycharm_projects/reports_datapull/exports/{}'.format(df)
                #to_ = 's3://avniro-datalake/ph-pricing-quotations/{}'.format(filename)
                to_ = '/Users/harish/Documents/pycharm_projects/reports_datapull/exports/{}'.format(df)
                os.system('cp {} {}'.format(from_/to_))
                #print('/usr/local/bin/aws s3 cp {} {}'.format(from_/to_))
                print(" # File is saved")
            except Exception as e:
                print(" # File is not saved -------->",e)
            
        except Exception as e:
            
            #byte_length = str(sys.getsizeof(constants.slack_data_file_upload_failed))
            #headers = {'Content-Type': "application/json", 'Content-Length': byte_length}
            #response = requests.post(constants.url, data=json.dumps(constants.slack_data_file_upload_failed), headers=headers)
            st.warning("Something went wrong")
            print("this is an  error in outer layer --->",e)



if __name__ == '__main__':

    data = auth()
    val = 2

    if val>1:

        
        # usage: postgres//<db_name>:<password>@<ip>:<port>/<dbname>
        db_string = "postgresql://postgres:postgres@10.50.1.10:5432/avniro_dwh"

        # echo true will generate the sql statements in the logs, set it to none in prod.
        engine = create_engine(db_string, echo=None)

        Session = sessionmaker(bind = engine)
        session = Session()

        print("connected to the database successfully")
        csv_db(engine,session)
    else:
        st.info("Please enter a Valid UserName / Password set.")
        
        
