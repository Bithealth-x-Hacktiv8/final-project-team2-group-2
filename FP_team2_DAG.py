'''
=================================================
SCRIPT DAG
Final Project

Nama  Team : Business Team
Anggota    : - Alwan Abdurrahman 
             - Berliana Fitria Dewi 
             - Muhammad Fakhrian Abimanyu 
             - Ryandino 
             - Satriya Fauzan Adhim

Program ini dibuat untuk melakukan automatisasi transform dan load data dari PostgreSQL. 
Adapun dataset yang dipakai adalah dataset mengenai data pasien rumah sakit.
=================================================
'''

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import pandas as pd

def fetch_and_convert_to_dataframe(**kwargs):
    table_names = table_names = [
    "HospitalTrx",
    "Doctor",
    "Surgery",
    "Lab",
    "Room",
    "Drugs",
    "DrugType",
    "Patient",
    "Payment",
    "Review",
    "HospitalCare",
    "Branch"
    ]

    # Menggunakan PostgresHook untuk melakukan koneksi ke PostgreSQL
    hook = PostgresHook(postgres_conn_id='postgres_aws')

    # Inisialisasi dictionary kosong untuk menyimpan DataFrame hasil
    dfs_dict = {}

    for table_name in table_names:
        # Eksekusi query SQL untuk tabel yang diinginkan
        query = f'SELECT * FROM "{table_name}";'

        try:
            conn = hook.get_conn()
            cursor = conn.cursor()
            cursor.execute(query)
            result = cursor.fetchall()

            # Buat DataFrame dari hasil query
            columns = [desc[0] for desc in cursor.description]
            df = pd.DataFrame(result, columns=columns)

            # Tambahkan DataFrame ke kamus dengan nama tabel sebagai key
            dfs_dict[table_name] = df

        except Exception as e:
            print(f"Gagal mengambil data dari tabel {table_name}: {e}")

    return dfs_dict

# Fungsi untuk mengubah tipe kolom ke numerik
def clean_numeric_columns(df, columns):
    for col in columns:
        df[col] = pd.to_numeric(df[col], errors='coerce')

# Fungsi untuk melakukan transformasi pada data money
def clean_price_columns(df, columns):
    for col in columns:
        df[col] = pd.to_numeric(
            df[col].str.replace('$', '').str.replace(',', '').str.replace('.00', ''), 
            errors='coerce'
        ).fillna(0)

# Fungsi untuk melakukan cleaning
def cleaning_data(**kwargs):
    df = kwargs['ti'].xcom_pull(task_ids='fetch_data_and_convert')

    try:
        df_trx = df['HospitalTrx']
        df_trx.replace('-', None, inplace=True)
        numeric_columns_trx = [
            'admin_price', 'drug_qty', 'cogs', 'id_branch', 'id_patient', 
            'id_hospital_care', 'id_room', 'id_doctor', 'id_surgery', 
            'id_lab', 'id_drug', 'id_payment', 'id_review'
        ]
        clean_numeric_columns(df_trx, numeric_columns_trx)
        df['HospitalTrx'] = df_trx

        # Mengubah kata kusus menjadi khusus
        df_surgery = df['Surgery']
        df_surgery['surgery_type'] = df_surgery['surgery_type'].replace('Kusus', 'Khusus')

        clean_price_columns(df_surgery, ['surgery_price'])
        df['Surgery'] = df_surgery

        df_doctor = df['Doctor']
        clean_price_columns(df_doctor, ['doctor_price'])
        df['Doctor'] = df_doctor

        df_lab = df['Lab']
        clean_price_columns(df_lab, ['lab_price'])
        df['Lab'] = df_lab

        df_room = df['Room']
        clean_price_columns(df_room, ['room_price', 'food_price'])
        df['Room'] = df_room

        df_drug = df['DrugType']
        clean_price_columns(df_drug, ['drug_price'])
        df['DrugType'] = df_drug

        df_care = df['HospitalCare']
        clean_price_columns(df_care, ['infusion_cost'])
        df['HospitalCare'] = df_care

        return df

    except Exception as e:
        print(f"Error saat cleaning data: {str(e)}")
        raise e

def add_duration_column(**kwargs):
    df = kwargs['ti'].xcom_pull(task_ids='cleaning_data')

    try:
        # Mengubah tipe data kolom 'Date IN' dan 'Date OUT' menjadi datetime
        df_hos = df['HospitalTrx']
        df_hos['date_in'] = pd.to_datetime(df_hos['date_in'])
        df_hos['date_out'] = pd.to_datetime(df_hos['date_out'])
        
        # Menambahkan kolom Durasi Rawat
        df_hos['Durasi_Rawat'] = df_hos['date_out'] - df_hos['date_in']
        df_hos['Durasi_Rawat'] = df_hos['Durasi_Rawat'] + pd.Timedelta(days=1)

        df['HospitalTrx'] = df_hos
        return df
    
    except Exception as e:
        print(f"Error saat menambahkan kolom durasi rawat: {str(e)}")
        raise e

def hitung_revenue(**kwargs):
    df = kwargs['ti'].xcom_pull(task_ids='add_duration_column')
    
    try:
        # Kolom-kolom untuk keperluan merge
        merge_keys = ['HospitalTrx', 'HospitalCare', 'Room', 'Doctor', 'Surgery', 'Lab', 'Drugs', 'DrugType', 'Branch', 'Patient', 'Payment', 'Review']
        merge_on_keys = ['id_hospital_care', 'id_room', 'id_doctor', 'id_surgery', 'id_lab', 'id_drug', 'drug_type_id', 'id_branch', 'id_patient', 'id_payment', 'id_review']
        right_on_keys = ['hospitalcare_id', 'room_id', 'doctor_id', 'surgery_id', 'lab_id', 'drug_id', 'drug_type_id', 'branch_id', 'patient_id', 'payment_id', 'review_id']

        final_df = df['HospitalTrx'].copy()

        # Merge semua dataframe yang sudah dilist
        for left_key, right_key, df_key in zip(merge_on_keys, right_on_keys, merge_keys[1:]):
            final_df = pd.merge(final_df, df[df_key], left_on=left_key, right_on=right_key, how='left')

        # Menghilangkan kata 'days' pada kolom durasi rawat
        if 'Durasi_Rawat' in final_df:
            final_df['Durasi_Rawat'] = final_df['Durasi_Rawat'].dt.days

        # Mengisi nilai NaN
        price_columns = ['room_price', 'food_price', 'doctor_price', 'surgery_price', 'lab_price', 'drug_price', 'admin_price']
        quantity_columns = ['drug_qty']

        for col in price_columns + quantity_columns:
            if col in final_df.columns:
                final_df[col] = final_df[col].fillna(0)

        # Menghitung revenue
        final_df['revenue'] = (
            (final_df['infusion_cost'] * final_df['Durasi_Rawat']) +
            (final_df['room_price'] * final_df['Durasi_Rawat']) +
            (final_df['food_price'] * final_df['Durasi_Rawat']) +
            (final_df['doctor_price'] * final_df['Durasi_Rawat']) +
            final_df['surgery_price'] +
            final_df['lab_price'] +
            (final_df['drug_price'] * final_df['drug_qty']) +
            final_df['admin_price']
        )

        # Menghitung profit
        final_df['profit'] = final_df['revenue'] - final_df['cogs']

        # Drop kolom yang tidak diperlukan
        columns_to_drop = merge_on_keys + right_on_keys
        final_df.drop(columns=columns_to_drop, inplace=True)

        return final_df

    except Exception as e:
        print(f"Error saat menghitung total cost: {str(e)}")
        raise e

# Fungsi untuk menyimpan DataFrame ke file CSV
def save_to_csv(csv_file_path,**kwargs):
    df = kwargs['ti'].xcom_pull(task_ids='hitung_revenue')
    try:
        # Simpan DataFrame ke file CSV
        df.to_csv(csv_file_path, index=False)
        
        print(f"Data telah disimpan ke: {csv_file_path}")
    except Exception as e:
        print(f"Error saat menyimpan data ke CSV: {str(e)}")
        raise e

# Mendefinisikan default arguments untuk DAG
default_args = {
    'owner': 'tim2Final',
    'depends_on_past': False,
    'start_date': datetime(2024, 4, 1),
}
with DAG('FinalProject_DAG', schedule_interval = '@daily',
         default_args = default_args,
         catchup=False) as dag:

    # Task untuk mengambil data dan mengonversi ke DataFrame
    fetch_data_task = PythonOperator(
    task_id='fetch_data_and_convert',
    python_callable=fetch_and_convert_to_dataframe,
    dag=dag
    )

    # Task untuk melakukan cleaning data
    cleaning_data_task = PythonOperator(
    task_id='cleaning_data',
    python_callable=cleaning_data,
    dag=dag
    )

    # Operator untuk menambahkan kolom durasi rawat
    add_duration_task = PythonOperator(
        task_id='add_duration_column',
        python_callable=add_duration_column,
        provide_context=True, 
        dag=dag
    )

    # Operator untuk menghitung total cost
    hitung_revenue_task = PythonOperator(
        task_id='hitung_revenue',
        python_callable=hitung_revenue,
        provide_context=True, 
        dag=dag
    )

    # Operator untuk menyimpan DataFrame ke file CSV
    save_to_csv_task = PythonOperator(
        task_id='save_to_csv',
        python_callable=save_to_csv,
        provide_context=True,  
        op_kwargs={'csv_file_path': '/opt/airflow/dags/cleaned_data.csv'},  # Path untuk menyimpan file CSV
        dag=dag
    )

# Mendefinisikan urutan task (task dependency)
fetch_data_task >> cleaning_data_task >> add_duration_task >> hitung_revenue_task >> save_to_csv_task 
