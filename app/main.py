import csv
import tempfile
import pandas as pd
import snowflake.connector
import streamlit as st
import os

app_version = '3.1v'
DATAFRAME_ROW_LIMIT = 2
ROOT_DIR = os.path.realpath(os.path.pardir)

tempdir = f"{ROOT_DIR}/temp"


def snowflake_connector(user, password, account, region, database, schema):
    snowflakeClient = snowflake.connector.connect(
        user = user,
        password = password,
        account = account,
        region = region,
        database = database,
        schema = schema
        )
    return snowflakeClient

# ===================================================== Streamlit Configuration
st.set_page_config(
    page_title="Snowflake File Loader",
    page_icon="🧊",
    layout="centered",
    initial_sidebar_state="expanded",
    menu_items={
        'Get Help': 'https://github.com/Igalem/streaml-it',
        'Report a bug': "https://github.com/Igalem/streaml-it",
        'About': f"# CSV Loader to Snowflake *\nApplication version {app_version} by igal emona\n\nUse this app to load your CSV file into Snowflake DB."
    }
)

header = st.container()
body = st.container()

st.sidebar.subheader('Connection details configuration')
st.sidebar.caption('Fill here your environment connection details')
st_user = st.sidebar.text_input('', placeholder='Username')
st_password  = st.sidebar.text_input('', placeholder='Password', type='password')
st_account  = st.sidebar.text_input('', placeholder='Account')
st_region  = st.sidebar.text_input('', placeholder='Region')
st_db  = st.sidebar.text_input('', placeholder='Database')
st_schema  = st.sidebar.text_input('', placeholder='Schema')

st_test  = st.sidebar.button('Test Connection')
st.sidebar.title('')
st.sidebar.caption('Created by: Igal Emoona (version {app_version})'.format(app_version=app_version))
df = pd.DataFrame()


with header:
    col1, col2, = st.columns([10,5])

    with col1:
        st.title("Snowflake File Loader ")

    with col2:
        st.image("https://docs.snowflake.com/en/_images/logo-snowflake-sans-text.png",width=60)
    
    ph_uploadfile = st.empty()
    uploaded_files = ph_uploadfile.file_uploader("Select your file", type=['txt', 'csv'], accept_multiple_files=True)

if st_test:
    try:
        snfClient = snowflake_connector(user = st_user,
                            password = st_password,
                            account = st_account,
                            region = st_region,
                            database = st_db,
                            schema = st_schema
                            ).cursor()
        snfClient.close()
    except snowflake.connector.errors.ProgrammingError as e:
                    snf_error = 'Error {0} ({1}): \n{2} ({3})'.format(e.errno, e.sqlstate, e.msg, e.sfqid)
                    st.error(body=snf_error)
                    print(e.errno, e.msg, e.raw_msg, e.sqlstate, e.telemetry_msg)
                    exit()
        
    st.success(body='Successfuly connected!')
    
    

with body:
    if uploaded_files:

        

        col1, col2, col3, col4= st.columns([3,2, 3,3])

        with col1:
            rows = st.number_input(label='Rows limit',value=DATAFRAME_ROW_LIMIT, min_value=1)

        with col2:
            fileHeaders = st.selectbox('Headers:',
                                ('No', 'Yes')) 
        
        with col3:
            ddl = st.selectbox('DDL generator:',
                                ('Create Only', 'Drop & Create', 'None'))
        
        with col4:
            action = st.selectbox('Query Action:',
                                ('Insert', 'Truncate & Insert'))
        
        
        #@st.cache(suppress_st_warning=True)
        def read_uploaded_file(uploaded_file, ignore_header=False):
            global uploaded_temp_file
            
            print('\n\n')
            print('---------------', uploaded_files)
            print(f"log: Uploaded filename: {uploaded_file.name}")

            uploaded_filename = uploaded_file.name
            uploaded_temp_file = tempdir + '/' + uploaded_filename

            with open(os.path.join(tempdir,uploaded_filename),"wb") as f:
                f.write(uploaded_file.getbuffer())

            try:
                data_frame=''
                
                if '.txt' in uploaded_temp_file.lower():
                    try:
                        print(f"log: trying to load TXT with no encoding dataframe: {uploaded_temp_file}")
                        data_frame = pd.read_csv(uploaded_temp_file, quoting=csv.QUOTE_NONE, sep='\t', low_memory=False, header=None)
                    except:
                        pass
                    
                    if len(data_frame) < 1:
                        try:
                            print(f"log: trying to load TXT with UTF-16 encoding dataframe: {uploaded_temp_file}")
                            data_frame = pd.read_csv(uploaded_temp_file, encoding='utf-16', quoting=csv.QUOTE_NONE, sep='\t', low_memory=False, header=None)                            
                        except:
                            pass
                        
                elif '.csv' in uploaded_temp_file.lower():
                    try:
                        print(f"log: trying to load CSV dataframe: {uploaded_temp_file}")
                        data_frame = pd.read_csv(uploaded_temp_file, low_memory=False, header=None)
                    except:
                        try:
                            print(f"log: trying to load CSV with UNICODE encoding dataframe: {uploaded_temp_file}")
                            data_frame = pd.read_csv(uploaded_temp_file, encoding='unicode_escape', low_memory=False, header=None)
                        except:
                            pass
            except:
                pass
            return data_frame
        
        ## Call function for create dataframe from file
        for i,uploaded_file in enumerate(uploaded_files):
            print(f'*** {i} *****', uploaded_file)
            if i==0:
                ignore_header = False
            else:
                ignore_header = True

            dataframe = read_uploaded_file(uploaded_file=uploaded_file, ignore_header=ignore_header)
            print(len(dataframe))
            df = pd.concat([df, dataframe], ignore_index=True)
            


        try:
            df = df.fillna('')
            print(f"log: NaN values cleared from dataframe")
        except:
            st.exception(f'error: Cant read uploaded file: \n\n {uploaded_file}')
            pass
        finally:
            os.remove(uploaded_temp_file)
            print(f"log: temporary file: {uploaded_temp_file} deleted from repository.")
        
        ## Generate general Headers
        df.columns = ['Filed' + str(h) for h in range(len(df.columns))]

        placeholderDataframe = st.empty()
        placeholderDataframe.dataframe(data=df.head(DATAFRAME_ROW_LIMIT))
        
        # st.dataframe(data=df.head(int(rows)))
        st.write('Total rows found:', '**{:,}**'.format(len(df)))
        col1, col2, = st.columns(2)
        with col1:
            placeholder = st.empty()
            execute = placeholder.button('Execute')
        
        with col2:    
            table = st.text_input(label='Tables name to be created:')

        types_map={'int64' : 'int',
                    'O' : 'string', ##'varchar(255)',
                    'bool' : 'boolean',
                    'float64' : 'float'
                    }

        char_to_replace = ['.', ')', '(', '/', ';', ' ', '^', '$', '#', '!', '@', '%', '&', '*', '-', '+', '=','ï»',
        '"', ':', '|', '[', ']', '{', '}', '±', '~', '§', '?', '<', '>']

        if fileHeaders == "No":
            data = df.values.tolist()
        else:
            headers = df.values.tolist()[0]
            df = df.iloc[1::]
            data = df.values.tolist()
            df.columns = headers
            placeholderDataframe.dataframe(data=df.head(DATAFRAME_ROW_LIMIT))

        for i, row in enumerate(data):
            data[i] = [str(r).strip() for r in row]

        # Covert data to string values
        values = ''
        for i in data[0:50]:
            values += "('" + "','".join(i) + "'),"

        headers = df.columns.tolist()
        cname=''
        for i,col_name in enumerate(headers):
            for s in col_name:
                if s in char_to_replace:
                    cname+='_'
                else:
                    cname+=s
            if len(set(cname)) == 1 or fileHeaders.lower() != "yes":
                cname='Field' + str(i)
            
            headers[i] = cname
            cname=''

        df_types = df.dtypes.tolist()
        col_types = []

        for col_type in df_types:
            if col_type == "int64":
                col_types.append(types_map['int64'])
            elif col_type == 'O':
                col_types.append(types_map['O'])
            elif col_type == 'bool':
                col_types.append(types_map['bool'])
            elif col_type == 'float64':
                col_types.append(types_map['float64'])

        columns_sql = [col + ' ' + ty for col,ty in zip(headers,col_types)]
        
        if rows:
            DATAFRAME_ROW_LIMIT = rows
            placeholderDataframe.dataframe(data=df.head(DATAFRAME_ROW_LIMIT))


        if execute and table != '':
            placeholder.empty()
            st_progressbar = st.progress(0)
            try:
                snfClient = snowflake_connector(user=st_user,
                    password = st_password,
                    account=st_account,
                    region = st_region,
                    database = st_db,
                    schema = st_schema).cursor()
            except snowflake.connector.errors.ProgrammingError as e:
                    snf_error = 'Error {0} ({1}): \n{2} ({3})'.format(e.errno, e.sqlstate, e.msg, e.sfqid)
                    st.error(body=snf_error)
                    print(e.errno, e.msg, e.raw_msg, e.sqlstate, e.telemetry_msg)
                    exit()
            st_progressbar.progress(0 + 10)            

            if 'create' in ddl.lower():
                if 'drop' in ddl.lower():
                    try:
                        print('--------- DROP TABLE ----------')
                        sqlDroptable = '''Drop table {table};'''.format(table=table)
                        snfClient.execute(sqlDroptable)
                        print('SQL statement:\n', sqlDroptable)
                        #st.code(body=sqlDroptable)
                    except snowflake.connector.errors.ProgrammingError as e:
                        snf_error = 'Error {0} ({1}): {2} ({3})'.format(e.errno, e.sqlstate, e.msg, e.sfqid)
                        st.error(body=snf_error)
                        print(snf_error)
                        exit()
                
                try:
                    print('--------- CREATE TABLE ----------')
                    sqlCreatetable = '''Create table {table} ({columns_sql});'''.format(table=table, columns_sql=','.join(columns_sql))
                    snfClient.execute(sqlCreatetable)
                    print('SQL statement:\n', sqlCreatetable)
                    #st.code(body=sqlCreatetable)
                except snowflake.connector.errors.ProgrammingError as e:
                    print('SQL statement:\n', sqlCreatetable)
                    snf_error = 'Error {0} ({1}): \n{2} ({3})'.format(e.errno, e.sqlstate, e.msg, e.sfqid)
                    st.error(body=snf_error)
                    print(e.errno, e.msg, e.raw_msg, e.sqlstate, e.telemetry_msg)
                    exit()
            st_progressbar.progress(0 + 10)

            if 'truncate' in action.lower():
                try:
                    print('--------- TRUNCATE TABLE ----------')
                    sqlTrunctable = '''Truncate table {table};'''.format(table=table)
                    snfClient.execute(sqlTrunctable)
                    print('SQL statement:\n', sqlTrunctable)
                    #st.code(body=sqlTrunctable)
                except snowflake.connector.errors.ProgrammingError as e:
                    snf_error = 'Error {0} ({1}): \n{2} ({3})'.format(e.errno, e.sqlstate, e.msg, e.sfqid)
                    st.error(body=snf_error)
                    print(e.errno, e.msg, e.raw_msg, e.sqlstate, e.telemetry_msg)
                    exit()

            st_progressbar.progress(0 + 10)

            fieldsCount = len(headers)
            valuesString=str('%s,' * fieldsCount)[:-1:]

            print('--------- INSERT into TABLE ----------')
            sqlInsert="Insert into {table} Values({valuesString});".format(table=table, valuesString=valuesString)
            print('SQL statement:\n', sqlInsert)

            dataSize = len(data)
            print("dataSize:",dataSize)
            bar_percent = round((dataSize / 16000))
            if bar_percent == 0:
                add_to_bar = 0
                bar_progress = 65
            else:
                bar_progress = 30
                add_to_bar = round((95 - bar_progress) / bar_percent)

            print('=================', dataSize, bar_percent)
            for i in range(0,dataSize,16000):
                if i!=0:
                    end=i+16000
                else:
                    end=16000
                dataBlock = data[i:end]
                
                print('------------ Block range:', i, end, '=', end-i, 'bar_progress=',bar_progress,'add_to_bar=', add_to_bar )

                try:
                    st_progressbar.progress(bar_progress)
                    snfClient.executemany(sqlInsert, dataBlock)
                except snowflake.connector.errors.ProgrammingError as e:
                    snf_error = 'Error {0} ({1}): \n{2} ({3})'.format(e.errno, e.sqlstate, e.msg, e.sfqid)
                    st.error(body=snf_error)
                    print(e.errno, e.msg, e.raw_msg, e.sqlstate, e.telemetry_msg)
                    exit()
                bar_progress+=add_to_bar
            st_progressbar.progress(100)
            st_progressbar.empty()
            st.success('You file loaded into Snowflake!')
            st.snow()
            execute=False
            snfClient.close()