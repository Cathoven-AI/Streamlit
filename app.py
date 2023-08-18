import streamlit as st
import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import mysql.connector

@st.cache_data
def load_data(host,user,password):
    config = {
    'user': user,
    'password': password,
    'host': host,
    'database': 'ado_hub',
    'raise_on_warnings': True
    }
    cnx = mysql.connector.connect(**config)
    mycursor = cnx.cursor()

    sql = "SELECT * FROM user_table"
    mycursor.execute(sql)
    results = mycursor.fetchall()

    df1 = pd.DataFrame(results, columns = mycursor.column_names)
    df1 = df1[(df1['is_staff']==False)&(df1['is_superuser']==False)]
    df1 = df1[['id','username','date_joined','pro','last_login','last_interaction']]
    df1 = df1[(df1['date_joined']<df1['last_login'])&(df1['date_joined']<df1['last_interaction'])]

    sql = "SELECT user_id,requested_at,path,remote_addr,host,username_persistent FROM rest_framework_tracking_apirequestlog"
    mycursor.execute(sql)
    results = mycursor.fetchall()

    df2 = pd.DataFrame(results, columns = mycursor.column_names)
    df2 = df2[df2['host'].apply(lambda x: 'localhost' not in x and 'test' not in x)]

    mycursor.close()

    return df1, df2

st.sidebar.subheader("Credentials")
host = st.sidebar.text_input('host')
user = st.sidebar.text_input('user')
password = st.sidebar.text_input('password',type='password')

if host!='' and user!='' and password!='':
    try:
        df1, df2 = load_data(host,user,password)
    except Exception as e:
        st.warning(e)
        st.stop()
else:
    st.stop()

st.sidebar.divider()
st.sidebar.subheader("Settings")
show_trends = st.sidebar.checkbox("Show trends",value=True,key="show_trends")
if show_trends:
    st.sidebar.write("Window size")
    daily_window_size = st.sidebar.number_input("Daily",value=7,min_value=2,max_value=30,step=1,key="daily_window_size")
    weekly_window_size = st.sidebar.number_input("Weekly",value=6,min_value=2,max_value=12,step=1,key="weekly_window_size")
    biweekly_window_size = st.sidebar.number_input("Bi-weekly",value=4,min_value=2,max_value=12,step=1,key="biweekly_window_size")
    monthly_window_size = st.sidebar.number_input("Monthly",value=3,min_value=2,max_value=12,step=1,key="monthly_window_size")

# 5. Trial Users: users who try features without registered
@st.cache_data
def trial_users(dates):
    counts = []
    for date in dates:
        date = pd.to_datetime(np.array(date))
        trial_user_ids = set(df2[(df2['username_persistent']=='Anonymous')&(df2['requested_at'].dt.normalize()>=date[0])&(df2['requested_at'].dt.normalize()<=date[1])&(df2['path']!='/')]['remote_addr'].values)
        counts.append(len(trial_user_ids))
    return np.array(counts)

@st.cache_data
def new_users(dates):
    counts = []
    for date in dates:
        date = pd.to_datetime(np.array(date))
        counts.append(len(df1[(df1['date_joined'].dt.normalize()>=date[0])&(df1['date_joined'].dt.normalize()<=date[1])]))
    return np.array(counts)

@st.cache_data
def registered_users(dates):
    dates = pd.to_datetime(np.array(dates))
    counts = []
    for date in dates:
        counts.append(len(df1[df1['date_joined'].dt.normalize()<=date]))
    return np.array(counts)

@st.cache_data
def subscription_users(dates):
    dates = pd.to_datetime(dates)
    return len(df1[df1['pro']==True])

@st.cache_data
def active_users(dates, among=None):
    df2_temp = df2[df2['username_persistent']!='Anonymous'].copy()
    df2_temp['requested_at'] = df2_temp['requested_at'].dt.normalize()
    #df2_temp = df2_temp[df2_temp['requested_at']>=pd.to_datetime(dates[0][0])-pd.Timedelta(days=31)]
    df2_temp = df2_temp.drop_duplicates(['user_id','requested_at'])
    #df2_temp['requested_at'] = df2_temp['requested_at'].apply(lambda x:x.strftime('%Y-%m-%d'))
    #if among is not None:
    #    df2_temp = df2_temp[df2_temp['user_id'].apply(lambda x: x in among)]

    counts = []
    for date in dates:
        date = pd.to_datetime(np.array(date))
        active_user_ids = set(df2_temp[(df2_temp['requested_at']>=date[0])&(df2_temp['requested_at']<=date[1])]['user_id'].values)
        if among is not None:
            active_user_ids = active_user_ids.intersection(among)
        # count = 0
        # date_range = set([x.strftime('%Y-%m-%d') for x in pd.date_range(start=date[0], end=date[1], freq='D')])
        # for _,g in df2_temp.groupby('user_id'):
        #     if len(date_range.intersection(set(g['requested_at'].values)))>0:
        #         count += 1
        # counts.append(count)
        counts.append(len(active_user_ids))

    return np.array(counts)


@st.cache_data
def rurr(dates):
    df2_temp = df2[df2['username_persistent']!='Anonymous'].copy()
    df2_temp['requested_at'] = df2_temp['requested_at'].dt.normalize()
    #df2_temp = df2_temp[df2_temp['requested_at']>=pd.to_datetime(dates[0][0])-pd.Timedelta(days=31)]
    df2_temp = df2_temp.drop_duplicates(['user_id','requested_at'])
    
    rurrs = []
    for date in dates:
        date = pd.to_datetime(np.array(date))
        freq = (date[1]-date[0]).days+1

        reactive_user_count = 0
        return_reactive_user_count = 0
        for _,g in df2_temp.groupby('user_id'):
            if len(g[(g['requested_at']>=date[0]-pd.Timedelta(days=freq))&(g['requested_at']<=date[1]-pd.Timedelta(days=freq))])==0:
                continue
            if len(g[(g['requested_at']>=date[0]-pd.Timedelta(days=freq*2))&(g['requested_at']<=date[1]-pd.Timedelta(days=freq*2))])>0:
                continue
            if len(g[g['requested_at']<date[1]-pd.Timedelta(days=freq*2)])==0:
                continue
            reactive_user_count += 1
            if len(g[(g['requested_at']>=date[0])&(g['requested_at']<=date[1])])>0:
                return_reactive_user_count += 1
        if reactive_user_count==0:
            rurrs.append(0)
        else:
            rurrs.append(return_reactive_user_count/reactive_user_count)
    return np.array(rurrs)

@st.cache_data
def get_reactive_users(dates):
    df2_temp = df2[df2['username_persistent']!='Anonymous'].copy()
    df2_temp['requested_at'] = df2_temp['requested_at'].dt.normalize()
    #df2_temp = df2_temp[df2_temp['requested_at']>=pd.to_datetime(dates[0][0])-pd.Timedelta(days=31)]
    df2_temp = df2_temp.drop_duplicates(['user_id','requested_at'])
    
    reactive_users = []
    for date in dates:
        date = pd.to_datetime(np.array(date))
        freq = (date[1]-date[0]).days+1

        active_this_period = set(df2_temp[(df2_temp['requested_at']>=date[0])&(df2_temp['requested_at']<=date[1])]['user_id'].values)
        active_previous_period = set(df2_temp[((df2_temp['requested_at']>=date[0]-pd.Timedelta(days=freq))&(df2_temp['requested_at']<=date[1]-pd.Timedelta(days=freq)))]['user_id'].values)
        active_before_previous_period = set(df2_temp[df2_temp['requested_at']<date[0]-pd.Timedelta(days=freq)]['user_id'].values)

        # user_ids = []
        # for user_id,g in df2_temp.groupby('user_id'):
        #     if len(g[(g['requested_at']>=date[0])&(g['requested_at']<=date[1])])==0:
        #         continue
        #     if len(g[(g['requested_at']>=date[0]-pd.Timedelta(days=freq))&(g['requested_at']<=date[1]-pd.Timedelta(days=freq))])>0:
        #         continue
        #     if len(g[g['requested_at']<date[1]-pd.Timedelta(days=freq)])==0:
        #         continue
        #     user_ids.append(user_id)
        #reactive_users.append(user_ids)
        reactive_users.append(set((active_this_period-active_previous_period).intersection(active_before_previous_period)))
    return reactive_users


@st.cache_data
def engagement_rate(dates):
    return active_users(dates)/registered_users([date[1] for date in dates])

@st.cache_data
def moving_average(arr, window_size=0):
    if window_size<=1:
        return arr
    numbers_series = pd.Series(arr)
    windows = numbers_series.rolling(window_size)
    moving_averages = windows.mean()
    moving_averages_list = moving_averages.tolist()
    final_list = np.array(moving_averages_list[window_size - 1:])
    return final_list

def get_dates(start,end,freq):
    if freq=='Daily':
        date_range_end = pd.date_range(start=start, end=end, freq='D')
        date_range_start = date_range_end
    elif freq=='Weekly':
        date_range_end = pd.date_range(start=start, end=end, freq='W-SUN')
        date_range_start = [x-pd.Timedelta(days=6) for x in date_range_end]
    elif freq=='Bi-weekly':
        date_range_end = pd.date_range(start=start, end=end, freq='2W-SUN')
        date_range_start = [x-pd.Timedelta(days=13) for x in date_range_end]
    else:
        date_range_end = pd.date_range(start=start, end=end, freq='M')
        date_range_start = pd.to_datetime(date_range_end.to_numpy().astype('datetime64[M]'))

    date_range_start_str = np.array([x.strftime('%Y-%m-%d') for x in date_range_start])
    date_range_end_str = np.array([x.strftime('%Y-%m-%d') for x in date_range_end])
    date_range_str = np.array(list(zip(date_range_start_str,date_range_end_str)))
    return date_range_start, date_range_end, date_range_str


default_to = pd.to_datetime((pd.Timestamp.today()-pd.Timedelta(days=1)).date())
default_from = default_to-pd.Timedelta(days=62)


au_expander = st.expander("AU")
au_expander.write("Active Users")
au_col1, au_col2, au_col3 = au_expander.columns(3)
au_from = au_col1.date_input(label="From",value=default_from,key='au_from')
au_to = au_col2.date_input(label="To",value=default_to,key='au_to')
au_freq = au_col3.selectbox('Frequency',('Daily', 'Weekly', 'Bi-weekly', 'Monthly'),index=1,key='au_freq')
au_yrange = au_expander.slider("Y-axis range", value=(0, 500), min_value=0, max_value=2000, step=100, key='au_yrange')

date_range_start, date_range_end, date_range_str = get_dates(au_from,au_to,au_freq)
active_user_counts = active_users(date_range_str)

fig = go.Figure()

if au_freq=='Daily':
    x = [x.strftime('%b-%d %a') for x in date_range_end]
    fig.add_trace(go.Scatter(x=x, y=active_user_counts, name='DAU'))
    fig.update_layout(xaxis_title='Day',yaxis_title='DAU')
elif au_freq=='Weekly':
    x = [x[0].strftime('%b %d')+"-"+x[1].strftime('%b %d') for x in zip(date_range_start,date_range_end)]
    fig.add_trace(go.Scatter(x=x, y=active_user_counts, name='WAU'))
    fig.update_layout(xaxis_title='Week',yaxis_title='WAU')
elif au_freq=='Bi-weekly':
    x = [x[0].strftime('%b %d')+"-"+x[1].strftime('%b %d') for x in zip(date_range_start,date_range_end)]
    fig.add_trace(go.Scatter(x=x, y=active_user_counts, name='2WAU'))
    fig.update_layout(xaxis_title='Bi-week',yaxis_title='2WAU')
else:
    x = [x.strftime('%Y %b') for x in date_range_end]
    fig.add_trace(go.Scatter(x=x, y=active_user_counts, name='MAU'))
    fig.update_layout(xaxis_title='Month',yaxis_title='MAU')

if show_trends:
    if au_freq=='Daily':
        extra_range_start, extra_range_end, extra_range_str = get_dates(au_from-pd.Timedelta(days=daily_window_size-1),au_from,au_freq)
        active_user_trend = moving_average(list(active_users(extra_range_str))+list(active_user_counts),window_size=daily_window_size)[-len(active_user_counts):]
    elif au_freq=='Weekly':
        extra_range_start, extra_range_end, extra_range_str = get_dates(au_from-pd.Timedelta(days=(weekly_window_size-1)*7),au_from,au_freq)
        active_user_trend = moving_average(list(active_users(extra_range_str))+list(active_user_counts),window_size=weekly_window_size)[-len(active_user_counts):]
    elif au_freq=='Bi-weekly':
        extra_range_start, extra_range_end, extra_range_str = get_dates(au_from-pd.Timedelta(days=(biweekly_window_size-1)*14),au_from,au_freq)
        active_user_trend = moving_average(list(active_users(extra_range_str))+list(active_user_counts),window_size=biweekly_window_size)[-len(active_user_counts):]
    else:
        extra_range_start, extra_range_end, extra_range_str = get_dates(au_from-pd.Timedelta(days=(monthly_window_size-1)*31),au_from,au_freq)
        active_user_trend = moving_average(list(active_users(extra_range_str))+list(active_user_counts),window_size=monthly_window_size)[-len(active_user_counts):]
    fig.add_trace(go.Scatter(x=x, y=active_user_trend, name='Trend', line=dict(color='firebrick', dash='dash')))

fig.update_layout(legend=dict(yanchor="top",y=0.99,xanchor="left",x=0.01))
fig.update_yaxes(range=au_yrange)
au_expander.plotly_chart(fig, use_container_width=True)



nu_expander = st.expander("NU")
nu_expander.write("New Users")
nu_col1, nu_col2, nu_col3 = nu_expander.columns(3)
nu_from = nu_col1.date_input(label="From",value=default_from,key='nu_from')
nu_to = nu_col2.date_input(label="To",value=default_to,key='nu_to')
nu_freq = nu_col3.selectbox('Frequency',('Daily', 'Weekly', 'Bi-weekly', 'Monthly'),index=1,key='nu_freq')
nu_yrange = nu_expander.slider("Y-axis range", value=(0, 500), min_value=0, max_value=2000, step=50, key='nu_yrange')

date_range_start, date_range_end, date_range_str = get_dates(nu_from,nu_to,nu_freq)
new_user_counts = new_users(date_range_str)

fig = go.Figure()
if nu_freq=='Daily':
    x = [x.strftime('%b-%d %a') for x in date_range_end]
    fig.add_trace(go.Scatter(x=x, y=new_user_counts, name='DNU'))
    fig.update_layout(xaxis_title='Day',yaxis_title='DNU')
elif nu_freq=='Weekly':
    x = [x[0].strftime('%b %d')+"-"+x[1].strftime('%b %d') for x in zip(date_range_start,date_range_end)]
    fig.add_trace(go.Scatter(x=x, y=new_user_counts, name='WNU'))
    fig.update_layout(xaxis_title='Week',yaxis_title='WNU')
elif nu_freq=='Bi-weekly':
    x = [x[0].strftime('%b %d')+"-"+x[1].strftime('%b %d') for x in zip(date_range_start,date_range_end)]
    fig.add_trace(go.Scatter(x=x, y=new_user_counts, name='WNU'))
    fig.update_layout(xaxis_title='Bi-week',yaxis_title='2WNU')
else:
    x = [x.strftime('%Y %b') for x in date_range_end]
    fig.add_trace(go.Scatter(x=x, y=new_user_counts, name='WNU'))
    fig.update_layout(xaxis_title='Month',yaxis_title='MNU')

if show_trends:
    if nu_freq=='Daily':
        extra_range_start, extra_range_end, extra_range_str = get_dates(nu_from-pd.Timedelta(days=daily_window_size-1),nu_from,nu_freq)
        new_user_trend = moving_average(list(new_users(extra_range_str))+list(new_user_counts),window_size=daily_window_size)[-len(new_user_counts):]
    elif nu_freq=='Weekly':
        extra_range_start, extra_range_end, extra_range_str = get_dates(nu_from-pd.Timedelta(days=(weekly_window_size-1)*7),nu_from,nu_freq)
        new_user_trend = moving_average(list(new_users(extra_range_str))+list(new_user_counts),window_size=weekly_window_size)[-len(new_user_counts):]
    elif nu_freq=='Bi-weekly':
        extra_range_start, extra_range_end, extra_range_str = get_dates(nu_from-pd.Timedelta(days=(biweekly_window_size-1)*14),nu_from,nu_freq)
        new_user_trend = moving_average(list(new_users(extra_range_str))+list(new_user_counts),window_size=biweekly_window_size)[-len(new_user_counts):]
    else:
        extra_range_start, extra_range_end, extra_range_str = get_dates(nu_from-pd.Timedelta(days=(monthly_window_size-1)*31),nu_from,nu_freq)
        new_user_trend = moving_average(list(new_users(extra_range_str))+list(new_user_counts),window_size=monthly_window_size)[-len(new_user_counts):]
    fig.add_trace(go.Scatter(x=x, y=new_user_trend, name='Trend', line=dict(color='firebrick', dash='dash')))

fig.update_layout(legend=dict(yanchor="top",y=0.99,xanchor="left",x=0.01))
fig.update_yaxes(range=nu_yrange)
nu_expander.plotly_chart(fig, use_container_width=True)





er_expander = st.expander("Engagement Rate")
er_expander.write("Active users / All users")
er_col1, er_col2, er_col3 = er_expander.columns(3)
er_from = er_col1.date_input(label="From",value=default_from,key='er_from')
er_to = er_col2.date_input(label="To",value=default_to,key='er_to')
er_freq = er_col3.selectbox('Frequency',('Daily', 'Weekly', 'Bi-weekly', 'Monthly'),index=1,key='er_freq')
er_yrange = er_expander.slider("Y-axis range", value=(0, 50), min_value=0, max_value=100, step=5, key='er_yrange')

date_range_start, date_range_end, date_range_str = get_dates(er_from,er_to,er_freq)
er = np.round(engagement_rate(date_range_str)*100,2)

fig = go.Figure()
if er_freq=='Daily':
    x = [x.strftime('%b-%d %a') for x in date_range_end]
    fig.add_trace(go.Scatter(x=x, y=er, name='Daily Engagement Rate (%)'))
    fig.update_layout(xaxis_title='Day',yaxis_title='Daily Engagement Rate (%)')
elif er_freq=='Weekly':
    x = [x[0].strftime('%b %d')+"-"+x[1].strftime('%b %d') for x in zip(date_range_start,date_range_end)]
    fig.add_trace(go.Scatter(x=x, y=er, name='Weekly Engagement Rate (%)'))
    fig.update_layout(xaxis_title='Week',yaxis_title='Weekly Engagement Rate (%)')
elif er_freq=='Bi-weekly':
    x = [x[0].strftime('%b %d')+"-"+x[1].strftime('%b %d') for x in zip(date_range_start,date_range_end)]
    fig.add_trace(go.Scatter(x=x, y=er, name='Bi-weekly Engagement Rate (%)'))
    fig.update_layout(xaxis_title='Bi-week',yaxis_title='Bi-weekly Engagement Rate (%)')
else:
    x = [x.strftime('%Y %b') for x in date_range_end]
    fig.add_trace(go.Scatter(x=x, y=er, name='Monthly Engagement Rate (%)'))
    fig.update_layout(xaxis_title='Month',yaxis_title='Monthly Engagement Rate (%)')

if show_trends:
    if er_freq=='Daily':
        extra_range_start, extra_range_end, extra_range_str = get_dates(er_from-pd.Timedelta(days=daily_window_size-1),er_from,er_freq)
        extra_er = np.round(engagement_rate(extra_range_str)*100,2)
        er_trend = moving_average(list(extra_er)+list(er),window_size=daily_window_size)[-len(er):]
    elif er_freq=='Weekly':
        extra_range_start, extra_range_end, extra_range_str = get_dates(er_from-pd.Timedelta(days=(weekly_window_size-1)*7),er_from,er_freq)
        extra_er = np.round(engagement_rate(extra_range_str)*100,2)
        er_trend = moving_average(list(extra_er)+list(er),window_size=weekly_window_size)[-len(er):]
    elif er_freq=='Bi-weekly':
        extra_range_start, extra_range_end, extra_range_str = get_dates(er_from-pd.Timedelta(days=(biweekly_window_size-1)*14),er_from,er_freq)
        extra_er = np.round(engagement_rate(extra_range_str)*100,2)
        er_trend = moving_average(list(extra_er)+list(er),window_size=biweekly_window_size)[-len(er):]
    else:
        extra_range_start, extra_range_end, extra_range_str = get_dates(er_from-pd.Timedelta(days=(monthly_window_size-1)*31),er_from,er_freq)
        extra_er = np.round(engagement_rate(extra_range_str)*100,2)
        er_trend = moving_average(list(extra_er)+list(er_expander),window_size=monthly_window_size)[-len(er):]
    fig.add_trace(go.Scatter(x=x, y=er_trend, name='Trend', line=dict(color='firebrick', dash='dash')))

fig.update_layout(legend=dict(yanchor="top",y=0.99,xanchor="left",x=0.01))
fig.update_yaxes(range=er_yrange)
er_expander.plotly_chart(fig, use_container_width=True)





stickiness_expander = st.expander("Stickiness")
stickiness_expander.write("DAU/MAU or WAU/MAU")
stickiness_col1, stickiness_col2, stickiness_col3 = stickiness_expander.columns(3)
stickiness_from = stickiness_col1.date_input(label="From",value=default_from,key='stickiness_from')
stickiness_to = stickiness_col2.date_input(label="To",value=default_to,key='stickiness_to')
stickiness_freq = stickiness_col3.selectbox('Frequency',('Daily', 'Weekly'),index=1,key='stickiness_freq')
stickiness_yrange = stickiness_expander.slider("Y-axis range", value=(0, 100), min_value=0, max_value=100, step=5, key='stickiness_yrange')

date_range_start, date_range_end, date_range_str = get_dates(stickiness_from,stickiness_to,stickiness_freq)
month_range_start = [end-pd.Timedelta(days=29) for end in date_range_end]
month_range_start_str = np.array([x.strftime('%Y-%m-%d') for x in month_range_start])
month_range_end_str = np.array([x.strftime('%Y-%m-%d') for x in date_range_end])
month_range_str = np.array(list(zip(month_range_start_str,month_range_end_str)))
mau = active_users(month_range_str)
au = active_users(date_range_str)
stickiness = np.round(au/mau*100,2)

fig = go.Figure()
if stickiness_freq=='Daily':
    x = [x.strftime('%b-%d %a') for x in date_range_end]
    fig.add_trace(go.Scatter(x=x, y=stickiness, name='DAU/MAU (%)'))
    fig.update_layout(xaxis_title='Day',yaxis_title='DAU/MAU (%)')
else:
    x = [x[0].strftime('%b %d')+"-"+x[1].strftime('%b %d') for x in zip(date_range_start,date_range_end)]
    fig.add_trace(go.Scatter(x=x, y=stickiness, name='WAU/MAU (%)'))
    fig.update_layout(xaxis_title='Week',yaxis_title='WAU/MAU (%)')

if show_trends:
    if stickiness_freq=='Daily':
        extra_range_start, extra_range_end, extra_range_str = get_dates(stickiness_from-pd.Timedelta(days=daily_window_size-1),stickiness_from,stickiness_freq)
        extra_month_range_start = [end-pd.Timedelta(days=29) for end in extra_range_end]
        extra_month_range_start_str = np.array([x.strftime('%Y-%m-%d') for x in extra_month_range_start])
        extra_month_range_end_str = np.array([x.strftime('%Y-%m-%d') for x in extra_range_end])
        extra_month_range_str = np.array(list(zip(extra_month_range_start_str,extra_month_range_end_str)))
        extra_mau = active_users(extra_month_range_str)
        extra_au = active_users(extra_range_str)
        extra_stickiness = np.round(extra_au/extra_mau*100,2)
        stickiness_trend = moving_average(list(extra_stickiness)+list(stickiness),window_size=daily_window_size)[-len(stickiness):]
    else:
        extra_range_start, extra_range_end, extra_range_str = get_dates(stickiness_from-pd.Timedelta(days=(weekly_window_size-1)*7),stickiness_from,stickiness_freq)
        extra_month_range_start = [end-pd.Timedelta(days=29) for end in extra_range_end]
        extra_month_range_start_str = np.array([x.strftime('%Y-%m-%d') for x in extra_month_range_start])
        extra_month_range_end_str = np.array([x.strftime('%Y-%m-%d') for x in extra_range_end])
        extra_month_range_str = np.array(list(zip(extra_month_range_start_str,extra_month_range_end_str)))
        extra_mau = active_users(extra_month_range_str)
        extra_au = active_users(extra_range_str)
        extra_stickiness = np.round(extra_au/extra_mau*100,2)
        stickiness_trend = moving_average(list(extra_stickiness)+list(stickiness),window_size=weekly_window_size)[-len(stickiness):]
    fig.add_trace(go.Scatter(x=x, y=stickiness_trend, name='Trend', line=dict(color='firebrick', dash='dash')))

fig.update_layout(legend=dict(yanchor="top",y=0.99,xanchor="left",x=0.01))
fig.update_yaxes(range=stickiness_yrange)
stickiness_expander.plotly_chart(fig, use_container_width=True)




# curr_expander = st.expander("CURR")
# curr_expander.write("How many active users are new users (New users / Active users).")
# curr_col1, curr_col2, curr_col3 = curr_expander.columns(3)
# curr_from = curr_col1.date_input(label="From",value=default_from,key='curr_from')
# curr_to = curr_col2.date_input(label="To",value=default_to,key='curr_to')
# curr_freq = curr_col3.selectbox('Frequency',('Weekly', 'Bi-weekly', 'Monthly'),key='curr_freq')
# curr_yrange = curr_expander.slider("Y-axis range", value=(0, 100), min_value=0, max_value=100, step=5, key='curr_yrange')

# if curr_from is not None and au_to is not None:
#     date_range_start, date_range_end, date_range_str = get_dates(curr_from,curr_to,curr_freq)
#     curr = (registered_users([date[1] for date in date_range_str])-new_users(date_range_str))/registered_users([date[0] for date in date_range_str])*100
#     temp = pd.DataFrame({'CURR':curr})
#     if curr_freq=='Daily':
#         temp['date'] = [x.strftime('%b-%d %a') for x in date_range_end]
#         fig = px.line(temp, x="date", y='CURR', labels={'date':'Day','CURR':"%"})
#     elif curr_freq=='Weekly':
#         temp['date'] = [x[0].strftime('%b %d')+"-"+x[1].strftime('%b %d') for x in zip(date_range_start,date_range_end)]
#         fig = px.line(temp, x="date", y='CURR', labels={'date':'Week','CURR':"%"},markers=True)
#     elif curr_freq=='Bi-weekly':
#         temp['date'] = [x[0].strftime('%b %d')+"-"+x[1].strftime('%b %d') for x in zip(date_range_start,date_range_end)]
#         fig = px.line(temp, x="date", y='CURR', labels={'date':'Bi-week','CURR':"%"},markers=True)
#     elif curr_freq=='Monthly':
#         temp['date'] = [x.strftime('%Y %b') for x in date_range_end]
#         fig = px.line(temp, x="date", y='CURR', labels={'date':'Month','CURR':"%"},markers=True)

#     fig.update_yaxes(range=curr_yrange)
#     curr_expander.plotly_chart(fig, use_container_width=True)



# miscellaneous_expander = st.expander("RURR/SURR")
# miscellaneous_col1, miscellaneous_col2, miscellaneous_col3 = miscellaneous_expander.columns(3)
# miscellaneous_freq = miscellaneous_col3.selectbox('Frequency',('-','Daily', 'Weekly', 'Bi-weekly', 'Monthly'), key='miscellaneous_freq')
# miscellaneous_from = miscellaneous_col1.date_input(label="From",value=default_from,key='miscellaneous_from')
# miscellaneous_to = miscellaneous_col2.date_input(label="To",value=default_to,key='miscellaneous_to')
# miscellaneous_yrange = miscellaneous_expander.slider("Y-axis range", value=(0, 100), min_value =0, max_value=100, step=5, key='miscellaneous_yrange')
# if miscellaneous_freq!='-':
#     date_range_start, date_range_end, date_range_str = get_dates(miscellaneous_from,miscellaneous_to,miscellaneous_freq)
#     miscellaneous_user_counts = miscellaneous_users(date_range_str)
#     active_user_counts = active_users(date_range_str)
#     temp = pd.DataFrame({'RURR':miscellaneous_user_counts['reactivated_users']/active_user_counts*100,
#                          'SURR':miscellaneous_user_counts['returning_users']/active_user_counts*100})
#     if miscellaneous_freq=='Daily':
#         temp['date'] = [x.strftime('%b-%d %a') for x in date_range_end]
#         fig = px.line(temp, x="date", y=['RURR','SURR'], labels={'date':'Day', 'value':'%', 'variable': 'Metrics'})
#     elif miscellaneous_freq=='Weekly':
#         temp['date'] = [x[0].strftime('%b %d')+"-"+x[1].strftime('%b %d') for x in zip(date_range_start,date_range_end)]
#         fig = px.line(temp, x="date", y=['RURR','SURR'], labels={'date':'Week', 'value':'%', 'variable': 'Metrics'},markers=True)
#     elif miscellaneous_freq=='Bi-weekly':
#         temp['date'] = [x[0].strftime('%b %d')+"-"+x[1].strftime('%b %d') for x in zip(date_range_start,date_range_end)]
#         fig = px.line(temp, x="date", y=['RURR','SURR'], labels={'date':'Bi-week', 'value':'%', 'variable': 'Metrics'},markers=True)
#     elif miscellaneous_freq=='Monthly':
#         temp['date'] = [x.strftime('%Y %b') for x in date_range_end]
#         fig = px.line(temp, x="date", y=['RURR','SURR'], labels={'date':'Month', 'value':'%', 'variable': 'Metrics'},markers=True)

#     fig.update_yaxes(range=miscellaneous_yrange)
#     miscellaneous_expander.plotly_chart(fig, use_container_width=True)


curr_expander = st.expander("CURR")
curr_expander.write("Current user retention rate")
curr_col1, curr_col2, curr_col3 = curr_expander.columns(3)
curr_freq = curr_col3.selectbox('Frequency',('Weekly', 'Bi-weekly', 'Monthly'), key='curr_freq')
curr_from = curr_col1.date_input(label="From",value=default_from,key='curr_from')
curr_to = curr_col2.date_input(label="To",value=default_to,key='curr_to')
date_range_start, date_range_end, date_range_str = get_dates(curr_from,curr_to,curr_freq)

data = []
raw_numbers = []
for i in range(len(date_range_str)):
    date = pd.to_datetime(np.array(date_range_str[i]))
    current_user_ids = set(df2[(df2['username_persistent']!='Anonymous')&(df2['requested_at'].dt.normalize()>=date[0])&(df2['requested_at'].dt.normalize()<=date[1])]['user_id'])
    raw_numbers.append(len(current_user_ids))
    l = np.array(active_users(date_range_str[i:],current_user_ids))
    l = list(np.round(l/len(current_user_ids),4)*100)
    data.append(l+[np.nan]*(len(date_range_str)-len(l)))
#data.append([1]+[np.nan]*(len(date_range_str)-1))
data = np.array(data)

if curr_freq=='Weekly' or curr_freq=='Bi-weekly':
    y = [x[0].strftime('%b %d')+"-"+x[1].strftime('%b %d') + f'        {raw_numbers[i]}'  for i,x in enumerate(zip(date_range_start,date_range_end))]
else:
    y = [x.strftime('%Y %b') + f'        {raw_numbers[i]}'  for i,x in enumerate(date_range_end)]
fig = px.imshow(data,
                labels=dict(x="Period", y="Date", color="Retention Rate (%)"),
                y=y,
                color_continuous_scale='rdylgn'
               )
fig.update_xaxes(side="top")

annotations = []
for i in range(len(date_range_str)):
    for j in range(len(date_range_str)):
        if not np.isnan(data[i, j]):
            if 70>data[i, j]>30:
                annotations.append(dict(x=j, y=i, text=f"{data[i, j]:.1f}%", showarrow=False, font=dict(color="black")))
            else:
                annotations.append(dict(x=j, y=i, text=f"{data[i, j]:.1f}%", showarrow=False, font=dict(color="white")))

fig.update_layout(annotations=annotations)

curr_expander.plotly_chart(fig, use_container_width=True)





nurr_expander = st.expander("NURR")
nurr_expander.write("New user retention rate")
nurr_col1, nurr_col2, nurr_col3 = nurr_expander.columns(3)
nurr_freq = nurr_col3.selectbox('Frequency',('Weekly', 'Bi-weekly', 'Monthly'), key='nurr_freq')
nurr_from = nurr_col1.date_input(label="From",value=default_from,key='nurr_from')
nurr_to = nurr_col2.date_input(label="To",value=default_to,key='nurr_to')
date_range_start, date_range_end, date_range_str = get_dates(nurr_from,nurr_to,nurr_freq)

data = []
raw_numbers = []
for i in range(len(date_range_str)):
    date = pd.to_datetime(np.array(date_range_str[i]))
    new_user_ids = df1[(df1['date_joined'].dt.normalize()>=date[0])&(df1['date_joined'].dt.normalize()<=date[1])]['id']
    raw_numbers.append(len(new_user_ids))
    l = list(np.round(active_users(date_range_str[i:],new_user_ids)/len(new_user_ids),4)*100)
    data.append(l+[np.nan]*(len(date_range_str)-len(l)))
data = np.array(data)

if nurr_freq=='Weekly' or nurr_freq=='Bi-weekly':
    y = [x[0].strftime('%b %d')+"-"+x[1].strftime('%b %d') + f'        {raw_numbers[i]}'  for i,x in enumerate(zip(date_range_start,date_range_end))]
else:
    y = [x.strftime('%Y %b') + f'        {raw_numbers[i]}'  for i,x in enumerate(date_range_end)]
fig = px.imshow(data,
                labels=dict(x="Period", y="Date", color="Retention Rate (%)"),
                y=y,
                color_continuous_scale='rdylgn'
               )
fig.update_xaxes(side="top")

# Customize the annotations to include percentage sign
annotations = []
for i in range(len(date_range_str)):
    for j in range(len(date_range_str)):
        if not np.isnan(data[i, j]):
            if 70>data[i, j]>30:
                annotations.append(dict(x=j, y=i, text=f"{data[i, j]:.1f}%", showarrow=False, font=dict(color="black")))
            else:
                annotations.append(dict(x=j, y=i, text=f"{data[i, j]:.1f}%", showarrow=False, font=dict(color="white")))

fig.update_layout(annotations=annotations)

nurr_expander.plotly_chart(fig, use_container_width=True)




# rurr_expander = st.expander("RURR")
# rurr_expander.write("reactive user retention rate")
# rurr_col1, rurr_col2, rurr_col3 = rurr_expander.columns(3)
# rurr_freq = rurr_col3.selectbox('Frequency',('Weekly', 'Bi-weekly', 'Monthly'), key='rurr_freq')
# rurr_from = rurr_col1.date_input(label="From",value=default_from,key='rurr_from')
# rurr_to = rurr_col2.date_input(label="To",value=default_to,key='rurr_to')
# rurr_yrange = stickiness_expander.slider("Y-axis range", value=(0, 100), min_value=0, max_value=100, step=5, key='rurr_yrange')

# date_range_start, date_range_end, date_range_str = get_dates(rurr_from,rurr_to,rurr_freq)

# temp = pd.DataFrame({'rurr':np.round(rurr(date_range_str)*100,2)})

# if rurr_freq=='Weekly':
#     temp['date'] = [x[0].strftime('%b %d')+"-"+x[1].strftime('%b %d') for x in zip(date_range_start,date_range_end)]
#     fig = px.line(temp, x="date", y='rurr', labels={'date':'Week', 'rurr':'RURR (%)'},markers=True)
# elif rurr_freq=='Bi-weekly':
#     temp['date'] = [x[0].strftime('%b %d')+"-"+x[1].strftime('%b %d') for x in zip(date_range_start,date_range_end)]
#     fig = px.line(temp, x="date", y='rurr', labels={'date':'Bi-week', 'rurr':'RURR (%)'},markers=True)
# elif rurr_freq=='Monthly':
#     temp['date'] = [x.strftime('%Y %b') for x in date_range_end]
#     fig = px.line(temp, x="date", y='rurr', labels={'date':'Month', 'rurr':'RURR (%)'},markers=True)

# fig.update_yaxes(range=stickiness_yrange)

# rurr_expander.plotly_chart(fig, use_container_width=True)


rurr_expander = st.expander("RURR")
rurr_expander.write("Reactive user retention rate")
rurr_col1, rurr_col2, rurr_col3 = rurr_expander.columns(3)
rurr_freq = rurr_col3.selectbox('Frequency',('Weekly', 'Bi-weekly', 'Monthly'), key='rurr_freq')
rurr_from = rurr_col1.date_input(label="From",value=default_from,key='rurr_from')
rurr_to = rurr_col2.date_input(label="To",value=default_to,key='rurr_to')
date_range_start, date_range_end, date_range_str = get_dates(rurr_from,rurr_to,rurr_freq)

reactive_user_ids = get_reactive_users(date_range_str)

data = []
for i in range(len(date_range_str)):
    active_user_counts = active_users(date_range_str[i:],reactive_user_ids[i])
    l = list(np.round(active_user_counts/len(reactive_user_ids[i]),4)*100)
    data.append(l+[np.nan]*(len(date_range_str)-len(l)))
data = np.array(data)

if rurr_freq=='Weekly' or rurr_freq=='Bi-weekly':
    y = [x[0].strftime('%b %d')+"-"+x[1].strftime('%b %d') + f'        {len(reactive_user_ids[i])}' for i, x in enumerate(zip(date_range_start,date_range_end))]
else:
    y = [x.strftime('%Y %b') + f'        {len(reactive_user_ids[i])}' for i, x in enumerate(date_range_end)]
fig = px.imshow(data,
                labels=dict(x="Period", y="Date", color="Retention Rate (%)"),
                y=y,
                color_continuous_scale='rdylgn'
               )
fig.update_xaxes(side="top")

annotations = []
for i in range(len(date_range_str)):
    for j in range(len(date_range_str)):
        if not np.isnan(data[i, j]):
            if 70>data[i, j]>30:
                annotations.append(dict(x=j, y=i, text=f"{data[i, j]:.1f}%", showarrow=False, font=dict(color="black")))
            else:
                annotations.append(dict(x=j, y=i, text=f"{data[i, j]:.1f}%", showarrow=False, font=dict(color="white")))

fig.update_layout(annotations=annotations)

rurr_expander.plotly_chart(fig, use_container_width=True)
