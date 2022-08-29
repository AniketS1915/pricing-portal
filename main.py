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

#  Settings -------------------------------
page_title = "Pricing Quote Form"
page_layout = "centered"


#  Settings -------------------------------

def pricing_quote(session):
#     selected = option_menu(
#         menu_title="Main Menu",  # required
#         options=["Get a Quote", "Edit a Quote"],  # required
#         icons=["house", "book", "envelope"],  # optional
#         menu_icon="cast",  # optional
#         default_index=0,  # optional
#         orientation="horizontal",
#     )

    # st.set_page_config(page_title=page_title,layout=page_layout)
    st.title(page_title)

    #     Dropdown Values ________________________________________

    ae_query = """select sf_name from identifier i where "role" = 'AE' and organization = 'ph' and user_status = 'Active' """

    ae_result = session.execute(ae_query)
    session.commit()

    ae_df = pd.DataFrame(ae_result)
    # ae = ae_df
    # print(ae_df)
    ae_selector = st.selectbox("Select AE you want to assign", ae_df)

    # Dropdown for sdr names

    sdr_query = """select sf_name from identifier i where "role" = 'SDR' and organization = 'ph' and user_status = 'Active' """

    sdr_result = session.execute(sdr_query)
    session.commit()

    sdr_df = pd.DataFrame(sdr_result)

    sdr_selector = st.selectbox("Select SDR you want to assign", sdr_df)

    company_name = st.text_input("Company Name")

    # head_count_query = ''' select distinct(tier) from big_mac_index'''
    # head_count_result = session.execute(head_count_query)
    # session.commit()
    # head_count_df = pd.DataFrame(head_count_result)
    # head_count = head_count_df.tier.tolist()
    head_count_selector = st.number_input("Head Count", min_value=0, format="%i", step=10)
    head_count_selector = str(head_count_selector)

    POC = st.text_input("Point of Contact Name")

    POC_Email = st.text_input("Point of Contact Email")

    country_query = '''select * from big_mac_index limit 1'''
    country_result = session.execute(country_query)
    session.commit()
    country_df = pd.DataFrame(country_result)
    country = country_df.columns.tolist()
    print("this is the country list -->",country)

    country_selector = st.selectbox("Select a Country", country)

    module_query = ''' select distinct(module) from big_mac_index_1'''
    module_result = session.execute(module_query)
    session.commit()
    module_df = pd.DataFrame(module_result)
    modules = module_df.module.tolist()

    module_selector = st.multiselect("Select Modules", modules)
    print(module_selector)
    print(type(module_selector))
    print(len(module_selector))
    module_select_convert = str(module_selector)
    print("After Cast", module_select_convert)
    print(type(module_select_convert))

    module_select_convert = module_select_convert.replace("[", "").replace("]", "")

    billing_cycle = "Quaterly", "Yearly"
    billing_cycle_selector = st.selectbox("Biling Cycle", billing_cycle)

    currency = "USD", "INR"
    currency_selector = st.selectbox("Currency", currency)

    discount_selector = st.number_input("Discount", min_value=0, format="%i", step=1)
    discount_selector = str(discount_selector)

    recommended_price = st.number_input("Recomended price", format="%i", step=1)
    recommended_price = str(recommended_price)

    calculation_query = """ select sum(cast(inr_rate as decimal)) as quotation from big_mac_index_1 where tier in ('{head_count_selector}') and module in ({module_selector})"""

    if (len(module_selector) == 0):
        calculation_query = calculation_query.replace(" and module in ({module_selector})", "")

    calculation_query = calculation_query.replace("{head_count_selector}", head_count_selector).replace(
        "{module_selector}", module_select_convert)

    calculation_result = session.execute(calculation_query)
    session.commit()

    print(calculation_query)

    calculation_result_df = pd.DataFrame(calculation_result)
    print("before LOC", calculation_result_df)
    price_number = calculation_result_df.at[calculation_result_df.index[0], 'quotation']
    # price_number = print(calculation_result_df.at[calculation_result_df.index[0],'quotation'])
    print(price_number)

    customer_id_generator_query = """select (case when replace(lower(company_name),' ','') = replace(lower('{}'),' ','') then customer_id
              			else max(customer_id) + 1 end) as customer_id 
              			from company_data cd
              			group by company_name, customer_id""".format(company_name)

    customer_id_result = session.execute(customer_id_generator_query)
    session.commit()
    print(customer_id_generator_query)
    customer_id_df = pd.DataFrame(customer_id_result)
    print("Customer ID Dataframe", customer_id_df)
    customer_id_generator = customer_id_df.at[customer_id_df.index[0], 'customer_id']
    # test_df = list(test_df)
    # print("After list",test_df)
    print("Type of Cust id",type(customer_id_generator))
    customer_id_generator = str(customer_id_generator)
    print("Type of Cust id",type(customer_id_generator))
    print(customer_id_generator)


    def to_db():


        insert_query = """INSERT INTO company_data (customer_id, sdr_name, ae_name, company_name, head_count, poc, poc_email, country, module, billing_cycle, currency, discount, recommended_price ) VALUES ('{customer_id_generator}','{sdr_selector}','{ae_selector}','{company_name}','{head_count_selector}','{POC}','{POC_Email}','{country_selector}',{module_select_convert},'{billing_cycle_selector}', '{currency_selector}','{discount_selector}','{recommended_price}') """
        print(insert_query)
        insert_query = insert_query.replace('{customer_id_generator}', customer_id_generator).replace('{sdr_selector}', sdr_selector).replace('{ae_selector}',ae_selector).replace('{company_name}', company_name).replace('{head_count_selector}', head_count_selector).replace('{POC}',POC).replace('{POC_Email}', POC_Email).replace('{country_selector}', country_selector).replace('{module_select_convert}',module_select_convert).replace('{billing_cycle_selector}', billing_cycle_selector).replace('{currency_selector}',currency_selector).replace('{discount_selector}', discount_selector).replace('{recommended_price}', recommended_price)
        query_result = session.execute(insert_query)
        session.commit()
        print(insert_query)
        update_query = """ insert into request_approvals (proposal_id ,customer_id, request_created_on)
                                select com.proposal_id,com.customer_id, com.request_created_on
                                    from company_data com
                                    where not exists (select 1
                                    from request_approvals r
                                    where com.proposal_id  = r.proposal_id);"""
        update_result = session.execute(update_query)
        session.commit()




    submitted = st.button("Send for Approval", on_click=to_db)
    print("data inserted")

    # module_query = ''' select distinct(module) from big_mac_index'''
    # module_result = session.execute(module_query)
    # session.commit()
    # module_df = pd.DataFrame(module_result)
    # modules = module_df.module.tolist()
    #
    # module_selector = st.selectbox("Select Modules", modules)
    #
    # col1, col2 = st.columns(2)
    # module_selector_dd1 = col1.selectbox("Select Month:", module_selector)
    # # price_edit = col2.number_input("Recomended price", format="%i", step=1)
    #

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
        pricing_quote(session)




    else:
        st.info("Please enter a Valid UserName / Password set.")
