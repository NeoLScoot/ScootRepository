from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import smtplib
import os
import json
from email.message import EmailMessage
from sshtunnel import SSHTunnelForwarder
import pymysql
import numpy as np

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def run_str_script():
    config_file_path = '/opt/airflow/dags/New Config.json'
    with open(config_file_path, 'r') as f:
        config = json.load(f)

    ssh_host = config['ssh']['host']
    ssh_user = config['ssh']['user']
    ssh_password = config['ssh']['password']
    ssh_pkey = '/opt/airflow/dags/New GO-INFRAS.pem'
    sql_hostname = config['sql']['hostname']
    sql_password = config['sql']['password']
    sql_database = config['sql']['main_database']
    sql_username = config['sql']['username']
    sql_port = config['sql']['port']

    with SSHTunnelForwarder(
        (ssh_host, 22),
        ssh_username=ssh_user,
        ssh_password=ssh_password,
        ssh_pkey=ssh_pkey,
        remote_bind_address=(sql_hostname, int(sql_port))
    ) as tunnel:
        conn = pymysql.connect(
            host='127.0.0.1',
            user=sql_username,
            passwd=sql_password,
            db=sql_database,
            port=tunnel.local_bind_port
        )

        query = """
            SELECT
                rental_id AS ref_id,
                quote_id AS trx_id,
                quote_num AS trx_num,
                rental_date_delivery AS trx_date,
                category_id,
                quoteitem_id AS trx_item_id,
                quoteitem_promotion_id AS promo_id,
                quoteitem_product_name,
                quoteitem_quantity AS item_qty,
                quoteitem_unitprice AS item_price,
                quoteitem_original_unitprice AS item_original_price,
                quoteitem_total AS item_total,
                quote_amount AS trx_total,
                IFNULL(tbl_provinces.province_name, '') AS province_name,
                IFNULL(quote_region.region_number, customer_region.region_number) AS deptcode,
                '' AS countrycode,
                IFNULL(
                    (SELECT rentaltype_name FROM scootaround_rental_types WHERE rental_rentaltype_id = rentaltype_id),
                    ''
                ) AS channel,
                'rental' AS trx_type,
                rental_event_id AS project_id,
                v.vendor_name, 
                tax_amount
            FROM scootaround_rentals
            JOIN tbl_quotes ON rental_quote_id = quote_id
            JOIN tbl_quote_items ON quoteitem_quote_id = quote_id
            LEFT JOIN scootaround_rental_vendors rv ON rv.rentalvendor_rental_id = scootaround_rentals.rental_id
            LEFT JOIN tbl_vendors v ON v.vendor_id = rv.rentalvendor_vendor_id
            LEFT JOIN (
                SELECT quotetax_quote_id, SUM(quotetax_amount) AS tax_amount
                FROM tbl_quote_tax  
                GROUP BY quotetax_quote_id
            ) AS tq_tax ON quotetax_quote_id = quote_id
            LEFT JOIN tbl_products ON quoteitem_product_id = product_id
            LEFT JOIN tbl_categories ON product_category_id = category_id
            LEFT JOIN tbl_departments ON category_department_id = department_id
            JOIN tbl_customers ON quote_customerid = tbl_customers.customer_id
            LEFT JOIN tbl_provinces ON customer_province_id = province_id
            LEFT JOIN tbl_regions AS customer_region ON customer_region_id = customer_region.region_id
            LEFT JOIN tbl_regions AS quote_region ON quote_region_id = quote_region.region_id
            WHERE
                quote_status NOT IN (199, 198, 20, 9)
                AND rental_date_delivery BETWEEN '2025-01-01' AND '2025-12-31'
            GROUP BY trx_item_id;
        """

        data = pd.read_sql_query(query, conn)

        data['item_price_adjusted'] = np.where(
            data['quoteitem_product_name'].str.contains('Cancelled', case=False, na=False),
            -1 * data['item_price'],
            data['item_price']
        )
      
        data['product_type'] = np.select(
        [
        data['quoteitem_product_name'].str.contains('Cancellation', case=False, na=False) &
        ~data['quoteitem_product_name'].str.contains('without CPP|No CPP|w/CPP', case=False, na=False),

        data['quoteitem_product_name'].str.contains('EPP|Equipment Protection', case=False, na=False)
        ],
        [
        'CPP',
        'EPP'
        ],
        default='Others'
        )

        osf_vendors = [
            'Desert Botanical Gardens',
            'Palm Beach Zoo',
            'Unconventional Gift Shop - PACC',
            'UPS Office - New Orleans Ernest N. Morial Convention Center',
            'Walter E Washington Convention Center - La Colombe Coffee Shop and Convenience Store',
            'Westgate Las Vegas Resort & Casino'
        ]

        data['vendor_type'] = np.where(
            data['vendor_name'].isin(osf_vendors),
            'osf_vendors',
            'other_vendors'
        )
        
        # Define conditions for 'channel_group'
        conditions = [
            data['channel'] == 'Cruise',
            data['channel'].isin(['Custom', 'AR - Custom', 'Cruise - Custom']) & (data['vendor_type'] != 'osf_vendors'),
            data['channel'].isin(['Custom', 'AR - Custom', 'Cruise - Custom']) & (data['vendor_type'] == 'osf_vendors'),
            data['channel'].isin(['AR - Facility', 'OSR - Custom', 'OSR - Facility', 'OSR']) & (data['vendor_type'] != 'osf_vendors'),
            data['channel'].isin(['AR - Facility', 'OSR - Custom', 'OSR - Facility', 'OSR']) & (data['vendor_type'] == 'osf_vendors')
        ]

        # Define the corresponding values
        choices = ['Cruise', 'Land', 'Land (OSF Venues)', 'Event (OSR)', 'OSF']

        # Create the new column
        data['channel_regroup'] = np.select(conditions, choices, default='TEST')


        # Create a column where only the first (trx_num, tax_amount) keeps the value; others get 0
        data['tax_amount_deduped'] = data.duplicated(subset=['trx_num', 'tax_amount'], keep='first')
        data['tax_adjusted'] = np.where(data['tax_amount_deduped'], 0, data['tax_amount'])
        data.drop(columns='tax_amount_deduped', inplace=True)
      
        today_str = datetime.today().strftime('%Y-%m-%d')
        csv_file_path = f'/opt/airflow/dags/str_rev_{today_str}.csv'
        data.to_csv(csv_file_path, index=False)

        conn.close()

    sender = 'nlu@scootaround.com'
    receiver = 'nlu@scootaround.com, felipe.ortega@whill.inc' 
    subject = f"STR Daily Report - {today_str}"
    body = "Hi,\n\nAttached is the daily STR Revenue report.\n\nBest regards,\nFP&A"

    msg = EmailMessage()
    msg['Subject'] = subject
    msg['From'] = sender
    msg['To'] = receiver
    msg.set_content(body)

    with open(csv_file_path, 'rb') as f:
        msg.add_attachment(f.read(), maintype='application', subtype='octet-stream', filename=os.path.basename(csv_file_path))

    with smtplib.SMTP('smtp.office365.com', 587) as smtp:
        smtp.starttls()
        smtp.login(sender, 'bbhbrtbzqghvhzll')  # Replace securely
        smtp.send_message(msg)

    print("STR Report generated and email sent.")
  
from pendulum import timezone

local_tz = timezone("America/Winnipeg")

# DAG definition
with DAG(
    dag_id='test_str_revenue_report',
    default_args=default_args,
    description='Run STR Revenue Report Daily',
    schedule='00 06 * * *',
    start_date=datetime(2025, 4, 10, tzinfo=local_tz),
    catchup=False,
    tags=['str', 'revenue']
) as dag:

    run_report = PythonOperator(
        task_id='run_str_report_script',
        python_callable=run_str_script,
    )
