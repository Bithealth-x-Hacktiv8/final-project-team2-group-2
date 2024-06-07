import streamlit as st
import pandas as pd
import pickle
import calendar

# Load model
model_filename = "model_pipeline.pkl"
with open(model_filename, 'rb') as file:
    pipeline = pickle.load(file)

# Fungsi untuk melakukan prediksi
def predict_score(jumlah_pasien, avg_review, revenue, cogs):
    # Buat DataFrame dari input user
    profit = revenue - cogs
    input_data = pd.DataFrame({
        'jumlah_pasien': [jumlah_pasien],
        'avg_review': [avg_review],
        'total_revenue': [revenue],
        'total_profit': [profit]
    })
    print(input_data)
    # Lakukan prediksi menggunakan pipeline
    predicted_score = pipeline.predict(input_data)[0]
    
    return predicted_score

def run():
# Interface Streamlit
    st.title('Hospital Performance Index Score Prediction 💯')

    # Form input dari user
    branch_name = st.selectbox("Branch", ["RSMA", "RSMS", "RSMD"])
    month = st.number_input('Month', min_value=1, max_value=12, step=1)
    year = st.number_input('Year', min_value=2000, max_value=2100, step=1)
    jumlah_pasien = st.number_input('Number of Patient', step=1)
    avg_review = st.number_input('Average of Review', min_value=1.0, max_value=5.0, step = 0.1)
    total_revenue = st.number_input('Total Revenue', min_value=0)
    cogs = st.number_input('Total COGS', min_value=0)

    if st.button('Predict Score'):
        # Lakukan prediksi
        predicted_score = predict_score(jumlah_pasien, avg_review, total_revenue, cogs)
        # print(predicted_score)
        # Tampilkan hasil prediksi
        if month == 12: 
            month = 0
            year = year+1
        month_name = calendar.month_name[month+1]

        if predicted_score>1: predicted_score=1
        elif predicted_score<0: predicted_score=0
        st.write(f'Performance score for {branch_name} in {month_name} {year} is: {predicted_score*100:.2f}')

if __name__ == '__main__':
    run()

