

def extract_data_from_aws(**kwargs):
    import pandas as pd
    import boto3
    from io import StringIO
    
    
    try:
        s3 = boto3.client(
            's3',
            aws_access_key_id="****************",
            aws_secret_access_key="*******************"",
            config=boto3.session.Config(connect_timeout=120, read_timeout=120)
        )

        bucket = "zaalima-peoject-datasets"
        key = "Global_Superstore.csv"

        response = s3.get_object(Bucket=bucket, Key=key)
        content = response['Body'].read().decode('utf-8')
        df = pd.read_csv(StringIO(content))

    
        local_path = '/opt/airflow/dags/raw_data.csv'
        df.to_csv(local_path, index=False)

        kwargs['ti'].xcom_push(key='data_path', value=local_path)
        print(f"Data saved to: {local_path}")
    
    except Exception as e:
        print(e)
        


def transform_data(**kwargs):
    import pandas as pd
    import boto3
    from io import StringIO
    ti = kwargs['ti']
    data_path = ti.xcom_pull(task_ids='extract_data', key='data_path') 

    final_df = pd.read_csv(data_path)
   
    final_df.drop(columns=['Postal Code', 'Unnamed: 0', 'Row ID', 'Product ID'], inplace=True)
    date_col = ['Order Date', 'Ship Date']
    for col in date_col:
        final_df[col] = pd.to_datetime(final_df[col], format='%Y-%m-%d')
        
    final_df.drop(columns = ['Product Name'],inplace = True)
        
    final_df['Shipping_days'] = final_df['Ship Date'] - final_df['Order Date']
    final_df['Shipping_days'] = final_df['Shipping_days'].astype('str').str.extract(r'(\d+)').astype('int')
    final_df['Price'] = final_df['Sales'] / final_df['Quantity']
    
    final_df['year'] = final_df['Order Date'].dt.year
    final_df['month'] = final_df['Order Date'].dt.month
    final_df['quarter'] = final_df['Order Date'].dt.quarter


    transformed_path = '/opt/airflow/dags/transformed_data.csv'
    final_df.to_csv(transformed_path, index=False)
    
    ti.xcom_push(key='transformed_data_path', value=transformed_path)
    print(f" Transformed Data saved to the path:{transformed_path}")
    
    
def generate_insights(**Kwargs):
    import pandas as pd
    import seaborn as sns 
    import matplotlib.pyplot as plt
    import plotly.express as px
    import boto3
    import numpy as np
    
    
    ti = Kwargs['ti']
    data_path = ti.xcom_pull(task_ids = 'transforming_data', key = 'transformed_data_path' )
    final_df = pd.read_csv(data_path)
    
    s3 = boto3.client(
        's3',
        aws_access_key_id="AKIAXKPUZPDURRFQGERX",
        aws_secret_access_key="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"
    )
    bucket_name = "zaalima-peoject-datasets"
    
    #High Order Customers
    high_order_cust = final_df.groupby('Customer Name').agg({'Order ID':'count'}).reset_index()[:10].sort_values(by='Order ID',ascending = False)
    x = high_order_cust['Customer Name']
    y = high_order_cust['Order ID']
    plt.bar(x,y,color='black')
    plt.xticks(rotation = 90)
    plt.title("High Order Customers")
    plt.xlabel("Customers")
    plt.ylabel("Orders")
    plot_path1 = "/opt/airflow/dags/plots/High_order_customers.png"
    plt.savefig(plot_path1)
    plt.clf()
    s3.upload_file(plot_path1, bucket_name, "visuals/High_order_customers.png")
    print("✅ Uploaded: visuals/High_order_customers.png")
    
    #Market-wise Analysis
    markets = ['APAC', 'Africa', 'Canada', 'EMEA', 'EU', 'LATAM', 'US']
    customers = [11002, 4587, 384, 5029, 10000, 10294, 9994]
    prices = [86.63, 76.33, 81.33, 72.08, 78.37, 56.91, 60.92]


    customers_norm = [x / max(customers) for x in customers]
    prices_norm = [x / max(prices) for x in prices]


    customers_norm += customers_norm[:1]
    prices_norm += prices_norm[:1]
    angles = np.linspace(0, 2 * np.pi, len(markets), endpoint=False).tolist()
    angles += angles[:1]


    plt.figure(figsize=(10, 6))
    ax = plt.subplot(111, polar=True)
    ax.plot(angles, customers_norm, 'b-', label='Customer Count')
    ax.fill(angles, customers_norm, 'b', alpha=0.2)
    ax.plot(angles, prices_norm, 'g-', label='Price')
    ax.fill(angles, prices_norm, 'g', alpha=0.2)

    ax.set_xticks(angles[:-1])
    ax.set_xticklabels(markets)
    plt.title('Market-wise Customer Count vs Price')
    ax.legend(loc = 'upper left')
    plot_path2 = "/opt/airflow/dags/plots/market_customer_analysis.png"
    plt.savefig(plot_path2)
    plt.clf()
    
    s3.upload_file(plot_path2, bucket_name, "visuals/market_customer_analysis.png")
    print("✅ Uploaded: visuals/market_customer_analysis.png")
            
    #sales_orders_country
    high_sales_country = final_df.groupby(['Country','State']).agg({'Order ID':'count','Sales':'sum'}).reset_index().sort_values(by='Sales',ascending=False)[:25]
    high_sales_country['Country_state'] = high_sales_country['Country'] +', '+ high_sales_country['State']

    x = high_sales_country['Country_state']
    y = high_sales_country['Sales']
    z = high_sales_country['Order ID']  

    fig, ax1 = plt.subplots(figsize=(12, 6))

    ax1.bar(x, y, color='skyblue', label='Sales')
    ax1.set_xlabel('Country/State')
    ax1.set_ylabel('Sales', color='blue')
    ax1.tick_params(axis='y', labelcolor='blue')
    plt.xticks(rotation=90)
    ax1.grid(True)


    ax2 = ax1.twinx()
    ax2.plot(x, z, color='red', marker='o', label='Order Count')
    ax2.set_ylabel('Order ID Count', color='red')
    ax2.tick_params(axis='y', labelcolor='red')


    plt.title('Sales vs Order Count by Country/State')
    fig.tight_layout()
    plot_path3 = "/opt/airflow/dags/plots/sales_orders_country.png"
    plt.savefig(plot_path3)
    plt.clf()
    
    s3.upload_file(plot_path3, bucket_name, "visuals/sales_orders_country.png")
    print("✅ Uploaded: visuals/sales_orders_country.png")
            
   #4-

    segment_any = final_df.groupby('Segment').agg({'Sales':'sum','Profit':'sum'}).round().reset_index('Segment')
    segment_any[['Sales','Profit']] = segment_any[['Sales','Profit']].astype('int')
    fig, ax1 = plt.subplots(figsize=(10,5))
    x = segment_any['Segment']
    y = segment_any['Sales']
    z = segment_any['Profit']

    ax1.bar(x,y)
    ax1.set_yticks(ticks = y)
    ax1.set_ylabel('Sales',size = 12)
    ax1.set_xlabel('Segment',size = 12)

    ax2 = ax1.twinx()
    ax2.plot(x,z,color = 'black',marker = 'o',markersize=7)
    ax2.set_ylabel('Profit',size = 12)
    plt.title('Segment Analysis')
    plot_path4 = "/opt/airflow/dags/plots/segment_analysis.png"
    plt.savefig(plot_path4)
    plt.clf()
    s3.upload_file(plot_path4, bucket_name, "visuals/segment_analysis.png")
    print("✅ Uploaded: visuals/segment_analysis.png")

def transform_to_sql(**Kwargs):
    import pandas as pd
    from sqlalchemy import create_engine
    ti = Kwargs['ti']
    data_path = ti.xcom_pull(task_ids='extract_data', key='data_path')
    try:
        df = pd.read_csv(data_path)
        username = 'root'         
        password = 'Rsawant@2810'         
        host = '127.0.0.1'                 
        port = '3306'                     
        database = 'telegana'

        connection_string = f'mysql+mysqlconnector://{username}:{password}@{host}:{port}/{database}'

        engine = create_engine(connection_string)
        df = pd.read_csv(r"C:\Users\raswa\OneDrive\Desktop\Airflow\Zaalima_project\airflow\dags\transformed_data.csv")

        df.to_sql('zaalima', engine, if_exists='replace', index=False)
        
    except Exception as e:
        print("Data loaded to Mysqlworbench!!")
