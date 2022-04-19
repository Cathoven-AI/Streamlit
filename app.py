import retentioneering
import pandas as pd
import streamlit as st
import plotly.express as px
from PIL import ImageColor
import plotly.graph_objects as go

@st.cache()
def load():
    return pd.read_excel('sections.xlsx')

df = load()

col1, col2 = st.columns(2)
with col1:
    user = st.radio("Group users by",('IP','Section'))
with col2:
    event = st.radio("Group events by",('Scene', 'Event'))


if user == 'Section':
    user_col = 'user_id'
else:
    user_col = 'remote_addr'

if event == 'Scene':
    event_col = 'scene'
    threshold = st.number_input('Threshold',value=10)
else:
    event_col = 'event'
    threshold = st.number_input('Threshold',value=500)

@st.cache()
def get_matrix():
    retentioneering.config.update({
        'user_col': user_col,
        'event_col':event_col,
        'event_time_col':'requested_at',
    })
    return df.rete.get_adjacency(weight_col='user_id', norm_type=None)
df_m = get_matrix()

label = []
source = []
target = []
value = []
for i in range(len(df_m)):
    label.append(df_m.index[i])
    for j in range(len(df_m)):
        v = df_m.iloc[i,j]
        if v >= threshold:
            source.append(i)
            target.append(j)
            value.append(df_m.iloc[i,j])
            
colors = ['rgba'+str(tuple(list(ImageColor.getcolor(x, "RGB"))+[0.8])) for x in px.colors.qualitative.Plotly]
colors = colors * int(max(len(target),len(label))/len(colors)+1)
color_node = [colors[0]]*len(label)
color_link = []
for i in range(len(target)):
    color_link.append(colors[target[i]])
    color_node[target[i]] = colors[target[i]].replace('0.8','0.5')


fig = go.Figure(data=[go.Sankey(
    node = dict(
      pad = 100,
      thickness = 20,
      line = dict(color = "black", width = 0.5),
      label = label,
      color = color_node
    ),
    link = dict(
      source = source,
      target = target,
      value = value,
      color = color_link
  ))])

fig.update_layout(title_text="Sankey Diagram", font_size=10)

st.plotly_chart(fig, use_container_width=True)