
import streamlit as st
import pandas as pd
import snowflake.connector

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
st.sidebar.caption('Created by: Igal Emoona (version 1.0)')




with header:
    col1, col2, = st.columns(2)

    with col1:
        st.title("Snowflake CSV Loader ")

    with col2:
        st.image(
    "https://docs.snowflake.com/en/_images/logo-snowflake-sans-text.png",
    width=100
    )
    
    uploaded_file = st.file_uploader("Select your file")

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
    except:
        st.error(body='Unable to connect into Snowflake account')
        print('Unable to connect into Snowflake account')
        exit()
        
    st.success(body='Successfuly connected!')
    
    

with body:

    
    if uploaded_file is not None:
        col1, col2, = st.columns(2)

        with col1:
            rows = st.number_input(label='Rows limit',value=3, min_value=1)
            

        df = pd.read_csv(uploaded_file,encoding= 'unicode_escape')
        st.dataframe(data=df.head(int(rows)))
        st.write('Total rows found:', len(df))
        col1, col2, = st.columns(2)
        with col1:
            execute = st.button(label='Execute')
        
        with col2:    
            table = st.text_input(label='Tables name')

        types_map={'int64' : 'int',
                    'O' : 'varchar(255)',
                    'bool' : 'boolean',
                    'float64' : 'float'
                    }

        char_to_replace = ['.', ')', '(', ' ', '^', '$', '#', '!', '@', '%', '&', '*', '-', '+', '=']

        data = df.values.tolist()
        for i, row in enumerate(data):
            data[i] = [str(r) for r in row]

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

        sqlCreatetable = '''Create table {table} ({columns_sql});'''.format(table=table, columns_sql=','.join(columns_sql))
    
        
        if execute and table != '':
            try:
                snfClient = snowflake_connector(user=st_user,
                password = st_password,
                account=st_account,
                region = st_region,
                database = st_db,
                schema = st_schema).cursor()
                snfClient.execute(sqlCreatetable)
            except:
                st.error(body='Unable to connect into Snowflake account')
                print('Unable to connect into Snowflake account')
                #sqlSnowflake.close()
                exit()

            st.code(body=sqlCreatetable)
            fieldsCount = len(headers)
            valuesString=str('%s,' * fieldsCount)[:-1:]
            sqlInsert="Insert into {table} Values({valuesString});".format(table=table, valuesString=valuesString)
            print('mysql stmt: ---------------\n', sqlInsert)
            st.code(body=sqlInsert)

            dataSize = len(data)
            for i in range(1,dataSize,16000-1):
                if i!=1:
                    end=i*2
                else:
                    end=16000
                dataBlock = data[i:end]
                print('------------', i)

                try:
                    snfClient.executemany(sqlInsert, dataBlock)
                except:
                    st.error(body='Unable to connect into Snowflake account')
                    print('Unable to connect into Snowflake account')
                    #sqlSnowflake.close()
                    exit()
            
            st.success('Data loaded into Snowflake!')
            st.balloons()
            execute=False
            snfClient.close()