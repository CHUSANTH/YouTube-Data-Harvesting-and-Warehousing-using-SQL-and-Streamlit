with st.sidebar:
    import streamlit as st

with st.sidebar:
    selected = st.selectbox("Select a Zone", ["Home", "Data Zone", "Analysis Zone", "Query Zone"], index=0)


st.header(':blue[Channel Data Analysis zone]')