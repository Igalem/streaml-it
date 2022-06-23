import streamlit as st
import os

cron_tmpFile = '/tmp/crontmp.log'
root_password = 'Qwe123!!'

## Creating Cron Jobs listtemp file
os.system("echo '{root_password}' | sudo -S crontab -u igale -l > {cron_tmpFile}".format(root_password=root_password, cron_tmpFile=cron_tmpFile))
crontab = open(cron_tmpFile, 'r').readlines()

st.session_state.crontab = crontab

st.set_page_config(
     page_title="CRONTAB Manager",
     page_icon="🧊",
     layout="wide",
     initial_sidebar_state="collapsed",
     menu_items={
         'Get Help': 'https://www.extremelycoolapp.com/help',
         'Report a bug': "https://www.extremelycoolapp.com/bug",
         'About': "# This is a header. This is an *extremely* cool app!"
     }
 )

add_selectbox = st.sidebar.checkbox(
    "How would you like to be contacted?",
    ("Email", "Home phone", "Mobile phone")
)

menu_cont = st.container
with menu_cont():
    st.subheader('CRONTAB Jobs')
    col1, col2,col3 = st.columns([20,1,10])
    with col1:
        st.empty()

    with col2:
        st.button('Add')
    with col3:
        st.button('Delete')
        
for i, item in enumerate(crontab):
    item_key = item.split(' ')
    item_schedule = ' '.join(item_key[0:5])
    item_mi = ' '.join(item_key[0])
    item_hh = ' '.join(item_key[1])
    item_day = ' '.join(item_key[2])
    item_month = ' '.join(item_key[3])
    item_week = ' '.join(item_key[4])
    item_command = ' '.join(item_key[5:])
    col1, col2,col3, col4, col5, col6 = st.columns([9,2.5,2.5,3.5,2.5,2])
    with col1:
        st.text_input(label = '', value=item_command, key='commad'+str(i))
    
    with col2:
        st_mi = st.selectbox('', ('Every min', '10', '20', '30', '40', '50'), key='min' + str(i))
    
    with col3:
        st_mi = st.selectbox('', ('Every hour', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
        '10', '11', '12', '13', '14', '15', '16', '17', '18', '19', '20', '21', '22', '23'), key='min' + str(i))
    
    with col4:
        st_mi = st.selectbox('', ('Every days in month', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
        '10', '11', '12', '13', '14', '15', '16', '17', '18', '19', '20', '21', '22', '23'), key='dim' + str(i))
    
    with col5:
        st_mi = st.selectbox('', ('Every month', '1', '2', '3', '4', '5', '6', '7', '8', '9',
        '10', '11', '12'), key='mon' + str(i))
    
    with col6:
        st_mi = st.selectbox('', ('Every day', 'Sunday', 'Monday', 'Tuesday', 'Wendsday', 'Thursday', 'Friday', 'Saturday'), key='day' + str(i))

