import streamlit as st
import pandas as pd
# import pandas_profiling
import psycopg2
import json
import sys
import random
import requests
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from auth_test import auth
# from st_aggrid import AgGrid
# from st_aggrid import GridOptionsBuilder
# from streamlit_pandas_profiling import st_profile_report
from streamlit_option_menu import option_menu


def for_approval(session):

    company_query = """select distinct(company_name) as company_name from company_data cd """
    company_result = session.execute(company_query)
    session.commit()
    company_df = pd.DataFrame(company_result)
    print(company_df)
    companies = company_df.company_name.tolist()

    company_selector = st.selectbox("Select Company",companies)


    if company_selector:
        col_a, col_b = st.columns(2)
        company_id_query = """select customer_id from company_data where company_name = '{company_selector}'"""
        company_id_query = company_id_query.replace('{company_selector}',company_selector)
        company_id_result = session.execute(company_id_query)
        session.commit()
        print(company_id_query)
        company_id_df = pd.DataFrame(company_id_result)

        print(company_id_df)
        company_id = company_id_df.customer_id.tolist()
        with col_a:
            company_id_selector = st.selectbox("Company_id", company_id)

        proposal_id_query = """select proposal_id from company_data where company_name = '{company_selector}'"""
        proposal_id_query = proposal_id_query.replace('{company_selector}', company_selector)
        proposal_id_result = session.execute(proposal_id_query)
        session.commit()
        print(proposal_id_query)
        proposal_id_df = pd.DataFrame(proposal_id_result)

        print(proposal_id_df)
        proposal_id = proposal_id_df.proposal_id.tolist()
        with col_b:
            proposal_id_selector = st.selectbox("Proposal_id", proposal_id)

    col_1, col_2 = st.columns(2)

    ae_query = """select ae_name from company_data cd where company_name = '{}'""".format(company_selector)
    ae_result = session.execute(ae_query)
    session.commit()
    print(ae_result)
    ae_df = pd.DataFrame(ae_result)
    ae_name = ae_df.at[ae_df.index[0], 'ae_name']
    print(ae_name)

    sdr_query = """select sdr_name from company_data cd where company_name = '{}'""".format(company_selector)
    sdr_result = session.execute(sdr_query)
    session.commit()
    print(sdr_result)
    sdr_df = pd.DataFrame(sdr_result)
    sdr_name = sdr_df.at[sdr_df.index[0], 'sdr_name']
    print(sdr_name)

    head_count_query = """select head_count from company_data cd where company_name = '{}'""".format(company_selector)
    head_count_result = session.execute( head_count_query)
    session.commit()
    print( head_count_result)
    head_count_df = pd.DataFrame( head_count_result)
    head_count_number =  head_count_df.at[ head_count_df.index[0], 'head_count']
    print( head_count_number)

    country_query = """select country from company_data cd where company_name = '{}'""".format(company_selector)
    country_result = session.execute(country_query)
    session.commit()
    print(country_result)
    country_df = pd.DataFrame(country_result)
    country = country_df.at[country_df.index[0], 'country']
    print(country)

    module_query = """select module from company_data cd where company_name = '{}'""".format(company_selector)
    module_result = session.execute(module_query)
    session.commit()
    print(module_result)
    module_df = pd.DataFrame(module_result)
    module = module_df.at[module_df.index[0], 'module']
    print(module)

    billing_cycle_query = """select billing_cycle from company_data cd where company_name = '{}'""".format(company_selector)
    billing_cycle_result = session.execute(billing_cycle_query)
    session.commit()
    print(billing_cycle_result)
    billing_cycle_df = pd.DataFrame(billing_cycle_result)
    billing_cycle = billing_cycle_df.at[billing_cycle_df.index[0], 'billing_cycle']
    print(billing_cycle)

    currency_query = """select currency from company_data cd where company_name = '{}'""".format(company_selector)
    currency_result = session.execute(currency_query)
    session.commit()
    print(currency_result)
    currency_df = pd.DataFrame(currency_result)
    currency = currency_df.at[currency_df.index[0], 'currency']
    print(currency)

    discount_query = """select discount from company_data cd where company_name = '{}'""".format(company_selector)
    discount_result = session.execute(discount_query)
    session.commit()
    print(discount_result)
    discount_df = pd.DataFrame(discount_result)
    discount = discount_df.at[discount_df.index[0], 'discount']
    discount = str(discount)
    sign = "%"
    discount = (discount + sign)
    print(discount)

    recommended_price_query = """select recommended_price from company_data cd where company_name = '{}'""".format(company_selector)
    recommended_price_result = session.execute(recommended_price_query)
    session.commit()
    print(recommended_price_result)
    recommended_price_df = pd.DataFrame(recommended_price_result)
    recommended_price = recommended_price_df.at[recommended_price_df.index[0], 'recommended_price']
    recommended_price = int(recommended_price)
    print(recommended_price)

    with col_1:
        ae_name_label = st.metric("AE Name", ae_name)
        company_label = st.metric("Company Name", company_selector)
        country_label = st.metric("Country", country)
        billing_cycle = st.metric("Billing Cycle", billing_cycle)
        discount_label = st.metric("Discount Applied", discount)




    with col_2:
        sdr_name_label = st.metric("SDR Name", sdr_name)
        head_count_label = st.metric("Head Count", head_count_number)
        module_label = st.metric("Module", module)
        currency_label = st.metric("currency", currency)
        recommended_price_label = st.metric("Recommended Price", recommended_price)

    status = "Approved", "Rejected"
    approval_box = st.selectbox("Select Status", status)
    print(approval_box)


    def approve_reject():
        if (approval_box == 'Approved'):
            approve_query = """
            update request_approvals
            set status = 'level_1_approved',
            level_1_approval_on = now()
            where proposal_id = {}
            """.format(proposal_id_selector)
            approve_result = session.execute(approve_query)
            session.commit()
            print(approve_query)

        if(approval_box == 'Rejected'):
            reject_query = """
            update request_approvals
            set status = 'level_1_rejected'
            where proposal_id = {}  """.format(proposal_id_selector)
            reject_result = session.execute(reject_query)
            session.commit()
            print(reject_query)



    approve_reject_btn = st.button("Submit", on_click=approve_reject)












if __name__ == '__main__':

    data = auth()

    if data:

        # usage: postgres//<db_name>:<password>@<ip>:<port>/<dbname>
        # todo: change it as per requirement.
        db_string = "postgresql://postgres:postgres@10.50.1.10:5432/avniro_dwh"

        # echo true will generate the sql statements in the logs, set it to none in prod.
        engine = create_engine(db_string, echo=None)

        Session = sessionmaker(bind=engine)
        session = Session()

        print("connected to the database successfully")
        for_approval(session)

    else:
        st.info("Please enter a Valid UserName / Password set.")