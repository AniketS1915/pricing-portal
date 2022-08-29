import streamlit as st
import sqlite3 as sql
import hashlib
import json
import sys
import random
import requests
import constants


def make_hashes(password):
    return hashlib.sha256(str.encode(password)).hexdigest()


def auth(sidebar=True):
    # if not st.session_state.get('show_login', True):
    #     return st.session_state.get('user', None)
    try:
        conn = sql.connect("file:auth.db?mode=ro", uri=True)
    except sql.OperationalError:
        st.error(
            "Authentication Database is Not Found.\n\nConsider running authlib script in standalone mode to generate."
        )
        return None

    user_placeholder = st.sidebar.empty()
    pwd_placeholder = st.sidebar.empty()
    input_widget1 = user_placeholder.text_input if sidebar else st.text_input
    input_widget2 = pwd_placeholder.text_input if sidebar else st.text_input
    checkbox_widget = st.sidebar.checkbox if sidebar else st.checkbox
    input_widget1("Username:", key="user_name")
    data = conn.execute("select * from users where username = ?", (st.session_state.get("user_name", ""),)).fetchone()
    if st.session_state.get("user_name", ""):
        input_widget2("Password:", type="password", key="password")
        print(st.session_state)

        if data:
            if make_hashes(st.session_state.get("password", "")) == data[2]:
                print("Success!")
                user_placeholder.empty()
                pwd_placeholder.empty()
                return st.session_state.get("user_name", "")
            elif st.session_state.get("password") !="":
                print("Wrong Password")
                byte_length = str(sys.getsizeof(constants.slack_data_login_failed))
                headers = {'Content-Type': "application/json", 'Content-Length': byte_length}
                response = requests.post(constants.url, data=json.dumps(constants.slack_data_login_failed), headers=headers)
                return None
    return None


def _list_users(conn):
    table_data = conn.execute("select username,password,su from users").fetchall()
    if table_data:
        table_data2 = list(zip(*table_data))
        st.table(
            {
                "Username": (table_data2)[0],
                "Password": table_data2[1],
                "Superuser?": table_data2[2],
            }
        )
    else:
        st.write("No entries in authentication database")


def _create_users(conn, init_user="", init_pass="", init_super=False):
    user = st.text_input("Enter Username", value=init_user)
    pass_ = st.text_input("Enter Password (required)", value=init_pass)
    pass_ = make_hashes(pass_)
    super_ = st.checkbox("Is this a superuser?", value=init_super)
    if st.button("Update Database") and user and pass_:
        with conn:
            conn.execute(
                "INSERT INTO USERS(username, password, su) VALUES(?,?,?)",
                (user, pass_, super_),
            )
            st.text("Database Updated")


def _edit_users(conn):
    userlist = [x[0] for x in conn.execute("select username from users").fetchall()]
    userlist.insert(0, "")
    edit_user = st.selectbox("Select user", options=userlist)
    if edit_user:
        user_data = conn.execute(
            "select username,password,su from users where username = ?", (edit_user,)
        ).fetchone()
        _create_users(
            conn=conn,
            init_user=user_data[0],
            init_pass=user_data[1],
            init_super=user_data[2],
        )


def _delete_users(conn):
    userlist = [x[0] for x in conn.execute("select username from users").fetchall()]
    userlist.insert(0, "")
    del_user = st.selectbox("Select user", options=userlist)
    if del_user:
        if st.button(f"Press to remove {del_user}"):
            with conn:
                conn.execute("delete from users where username = ?", (del_user,))
                st.write(f"User {del_user} deleted")


def _superuser_mode():
    with sql.connect("file:auth.db?mode=rwc", uri=True) as conn:
        conn.execute(
            "create table if not exists users (id INTEGER PRIMARY KEY, username UNIQUE ON CONFLICT REPLACE, password, su)"
        )
        mode = st.radio("Select mode", ("View", "Create", "Edit", "Delete"))
        {
            "View": _list_users,
            "Create": _create_users,
            "Edit": _edit_users,
            "Delete": _delete_users,
        }[mode](
            conn
        )  # I'm not sure whether to be proud or horrified about this...


if __name__ == "__main__":
    st.write(
        "Warning, superuser mode\n\nUse this mode to initialise authentication database"
    )
    if st.checkbox("Check to continue"):
        _superuser_mode()