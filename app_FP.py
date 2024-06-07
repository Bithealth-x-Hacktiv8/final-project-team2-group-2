import streamlit as st
import predict_FP as predict_FP
import predict as predict
import ml_FP as ml

navigation = st.sidebar.selectbox('Select Page:', ('Form Page','File Upload Page','Chatbot Page'))

if navigation == 'Form Page':
    predict_FP.run()
elif navigation == 'File Upload Page':
    predict.run()
elif navigation == 'Chatbot Page':
    ml.run()
