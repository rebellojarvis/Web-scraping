pip install --upgrade pip
import numpy as np
import pandas as pd
pd.options.mode.chained_assignment = None
# !pip install pycountry
import pycountry as pc


# ### Reading the CSV

# In[2]:


df = pd.read_csv('trades.csv', sep=';')

df = df.rename(columns={'destination_country,': 'destination_country'})

df.head()


# ## Step 1: Import / Export reports
# ## Data quality validation
# ### Checking for NULL/NaN values

# In[3]:


def null_check(df):
    return df.isnull().sum()

def findCountryAlpha2(country_name):
    try:
        return pc.countries.get(name=country_name).alpha_2
    except:
        return ("Country not found!")


# # 2. Tranforming the data

# 
# ### Standardizing column values and data types

# In[4]:


def column_validation(df):
    
    # Converting date column to datetime data type
    
    df['date'] = pd.to_datetime(df['date'])

    # Validating if 'HS_CODE' starts with "870423" and length is 8 characters

    df = df.loc[(df.loc[:,'hs_code'].astype(str).str.startswith('870423')) & (df.loc[:,'hs_code'].astype(str).str.len() == 8)]
    df['hs_code'] = df['hs_code'].astype(np.int64)

    df['shipper_name'] = df['shipper_name'].astype('str') 
    df['std_unit'] = df['std_unit'].astype('str')
    df['std_quantity'] = df['std_quantity'].astype(np.int64)
    df.loc[:,'value_fob_usd'] = df['value_fob_usd'].str.replace(',','.').astype(np.float64)
    
    # As mentioned, we can assume the number is 1 if the value is less than 80,000 USD when there's no information
    
    df.loc[df['value_fob_usd'] < 80000.00,'items_number'] = 1
    df['items_number'] = df['items_number'].astype(np.int64)

    # Check if port code starts with the country's ISO Alpha-2 code

    df[["source_iso","destination_iso"]] = np.nan # for validation purpose
    
    df.loc[:,('source_iso')] = df.apply(lambda row: findCountryAlpha2(row.source_country) , axis = 1)
    df.loc[:,('destination_iso')] = df.apply(lambda row: findCountryAlpha2(row.destination_country) , axis = 1)


    df['source_check'] = [y.startswith(x) for x,y in zip(df['source_iso'], df['source_port'])]
    df['destination_check'] = [y.startswith(x) for x,y in zip(df['destination_iso'], df['destination_port'])]
    
    return df


# In[5]:


print("Count of NULL/Nan values in each column:")
print(null_check(df))
df = column_validation(df)


# In[6]:


df_final = df.iloc[:, :-4]


# ## Goal 1
# ### Popular shipping countries and routes

# In[7]:


df_1 = df_final.groupby(['source_country','destination_country'])['source_country'].count()
print("Most popular shipping countries:\n", df_1.sort_values(ascending = False))


df_2 = df_final.groupby(['source_port','destination_port'])['source_port'].count()
print("\n\n\nMost popular shipping routes:\n", df_2.sort_values(ascending = False))


# ### Average import value (in USD) per country

# In[8]:


df2 = df_final.groupby(['destination_country'])['value_fob_usd'].mean()

print("Average import value per country:\n",df2.sort_values(ascending = False))


# # Step 2 (Web-scraping using BeautifulSoup and requests libraries)

# In[9]:


countries = set(df['source_iso']) | set(df['destination_iso'])


# In[10]:


from bs4 import BeautifulSoup
import requests

main_url = "https://www.cogoport.com"
seaport_page = '/en-IN/knowledge-center/resources/port-info'
results = requests.get(main_url+seaport_page).text
soup = BeautifulSoup(results, "html.parser")


# In[11]:


new_df = pd.DataFrame(columns=['iso', 'seaport', 'lines', 'import_restrictions', 'export_restrictions', 'website'])

for code in countries:
    
    # Finding the country block using iso code
    iso = soup.find_all(text="({})".format(code))
    
    # Navigating to the country parent block
    parent = iso[0].parent.parent.parent.parent
    
    # Retrieving all the seaports and websites mentioned in the block
    ports = parent.find_all("p")
    websites = parent.find_all("a")
    main_table_contents = []
    
    # Iterating through each seaport individually
    for port, website in zip(ports,websites):
        print(port.string)
        
        # Clicking on each seaport mentioned in the country block using websites mentioned
        seaport = requests.get(main_url + website.get("href")).text
        doc = BeautifulSoup(seaport, "html.parser")
        
        # Finding the tables with lines mentioned
        tables = doc.find_all('table')
        main_table_contents = []
        table_contents = []
        
        # Collating all values in the table
        for table in tables:
            rows = table.find_all('td')
            for row in rows:
                table_contents.append([row.text])
        
        main_table_contents.append([table_contents])
        s = []
        
        # Filtering out only the lines values
        for i in range(np.shape(main_table_contents)[0]):
            for j in range(0,np.shape(main_table_contents[i][0])[0],5):
                s.append(main_table_contents[i][0][j][0])
        
        print(set(s))
        
        # Finding the block which has information about Import restrictions
        import_restrictions = doc.find('div', {'class':"styles_info__gszri"})
        
        # Finding the block which has information about Export restrictions        
        export_restrictions = doc.find('div', {'class':"styles_info__SMa4k"})
        print("--------------------------------------------------------------------")
        print("Export restrictions\n",export_restrictions.get_text())
        print("--------------------------------------------------------------------")
        print("Import restrictions\n",import_restrictions.get_text())
        print("--------------------------------------------------------------------")
        
        
        # Adding values to the new dataframe based on the values retrieved from the website
        new_df = new_df.append({'iso' : code, 'seaport' : port.string, 'lines' : ", ".join(str(e) for e in set(s)), 'import_restrictions':import_restrictions.get_text(),'export_restrictions':export_restrictions.get_text(),'website':main_url + website.get("href")}, ignore_index = True)


new_df.to_csv('ports_info.csv',index=False)

# from sqlalchemy import create_engine
# engine = create_engine('postgresql://postgres:password@localhost:5432/postgres')
# new_df.to_sql('ports_info', engine,index=False)
