import os

import streamlit as st
import pandas as pd
from pandasai import SmartDataframe
from pandasai.callbacks import BaseCallback
from pandasai.llm import OpenAI
from pandasai.responses.response_parser import ResponseParser


class StreamlitCallback(BaseCallback):
    def __init__(self, container) -> None:
        """Initialize callback handler."""
        self.container = container

    def on_code(self, response: str):
        self.container.code(response)


class StreamlitResponse(ResponseParser):
    def __init__(self, context) -> None:
        super().__init__(context)

    def format_dataframe(self, result):
        st.dataframe(result["value"])
        return

    def format_plot(self, result):
        st.image(result["value"])
        return

    def format_other(self, result):
        st.write(result["value"])
        return

def load_data(path):
    df = pd.read_csv(path)
    return df

def run():
    st.write("# Chat with Mulia Hospital Dataset 🏥")

    df = load_data('cleaned_data.csv')

    with st.expander("🔎 Dataframe Preview"):
        st.write(df.tail(3))

    query = st.text_area("🧑‍⚕️ Ask me!")
    container = st.container()

    if query:
        llm = OpenAI(api_token=os.environ["OPENAI_API_KEY"])
        # llm = OpenAI(
        #     api_token=os.environ["OPENAI_API_KEY"],
        #     model="gpt-3.5-turbo-16k"  # specify the model here
        # )
        query_engine = SmartDataframe(
            df,
            config={
                "llm": llm,
                "response_parser": StreamlitResponse,
                "callback": StreamlitCallback(container),
            },
        )

        answer = query_engine.chat(query)
        if answer != None:
            st.write("Sorry, I can't answer the question yet")
        # st.write("### Response:")
        # st.write(answer)