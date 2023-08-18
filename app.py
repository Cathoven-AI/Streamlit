import streamlit as st
import numpy as np
import pandas as pd
import plotly.figure_factory as ff
import plotly.express as px
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



with st.sidebar:
    host = st.text_input('host')
    user = st.text_input('user')
    password = st.text_input('password',type='password')

if host!='' and user!='' and password!='':
    try:
        df1, df2 = load_data(host,user,password)
    except Exception as e:
        st.warning(e)
        st.stop()
else:
    st.stop()

# 5. Trial Users: users who try features without registered
@st.cache_data
def trial_users(dates, period=7):
    dates = pd.to_datetime(dates)
    counts = []
    for date in dates:
        count = 0
        for _,g in df2[(df2['requested_at'].dt.normalize()>=date-pd.Timedelta(days=period-1))&(df2['requested_at'].dt.normalize()<=date)&(df2['path']!='/')].groupby('remote_addr'):
            #if len(set(g['username_persistent'].values)-set(['Anonymous']))==0:
            if 'Anonymous' in set(g['username_persistent'].values):
                count += 1
        counts.append(count)
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
    df2_temp = df2_temp[df2_temp['requested_at']>=pd.to_datetime(dates[0][0])-pd.Timedelta(days=31)]
    df2_temp = df2_temp.drop_duplicates(['user_id','requested_at'])
    df2_temp['requested_at'] = df2_temp['requested_at'].apply(lambda x:x.strftime('%Y-%m-%d'))
    if among is not None:
        df2_temp = df2_temp[df2_temp['user_id'].apply(lambda x: x in among)]

    counts = []
    for date in dates:
        date = pd.to_datetime(np.array(date))
        count = 0
        date_range = set([x.strftime('%Y-%m-%d') for x in pd.date_range(start=date[0], end=date[1], freq='D')])
        for _,g in df2_temp.groupby('user_id'):
            if len(date_range.intersection(set(g['requested_at'].values)))>0:
                count += 1
        counts.append(count)
    return np.array(counts)


# 9. Reactivated Users: users who log in whose previous login was 7-29 days before
# 10. Returning users: users who log in whose previous login was more than 29 days before
# 11. Active users_7d that might lost: users who fail to log in whose previous login was 1-6 days before
# 12. Active users_30d that migh lost: users who fail to log in whose previous login was 1-29 days before
# 13. Lost users: users who haven't logged in since more than 30 days ago
@st.cache_data
def miscellaneous_users(dates):
    df2_temp = df2[df2['username_persistent']!='Anonymous'].copy()
    df2_temp['requested_at'] = df2_temp['requested_at'].dt.normalize()
    df2_temp = df2_temp[df2_temp['requested_at']>=pd.to_datetime(dates[0][0])-pd.Timedelta(days=31)]
    df2_temp = df2_temp.drop_duplicates(['user_id','requested_at'])

    reactivated_user_counts = []
    returning_user_counts = []
    active_user_7d_counts = []
    active_user_30d_counts = []
    lost_user_counts = []
    retained_user_7d_counts = []
    
    for date in dates:
        date = pd.to_datetime(np.array(date))

        reactivated_user_count = 0
        returning_user_count = 0
        active_user_7d_count = 0
        active_user_30d_count = 0
        lost_user_count = 0
        retained_user_7d_count = 0


        for _,g in df2_temp.groupby('user_id'):
            for i in range(1):
                g['diff'] = (date[0]-g['requested_at']).dt.days-i
                diff = set(g['diff'])
                if 0 in diff:
                    if len(set(range(1,7)).intersection(diff))==0:
                        if len(set(range(7,30)).intersection(diff))>0:
                            reactivated_user_count += 1
                            break
                        elif max(diff)>29:
                            returning_user_count += 1
                            break
                    #if 7 in diff:
                    #    retained_user_7d_count += 1
                else:
                    #if len(set(range(1,30)).intersection(diff))>0:
                    #    active_user_30d_count += 1
                    #    if len(set(range(1,7)).intersection(diff))>0:
                    #        active_user_7d_count += 1
                    #else:
                    #    lost_user_count += 1
                    if len(set(range(1,30)).intersection(diff))==0:
                        lost_user_count += 1
                        break

        reactivated_user_counts.append(reactivated_user_count)
        returning_user_counts.append(returning_user_count)
        #active_user_7d_counts.append(active_user_7d_count)
        #active_user_30d_counts.append(active_user_30d_count)
        lost_user_counts.append(lost_user_count)
        #retained_user_7d_counts.append(retained_user_7d_count)
    return {'reactivated_users':reactivated_user_counts,
            'returning_users':returning_user_counts,
            #'active_users_7d':active_user_7d_counts,
            #'active_users_30d':active_user_30d_counts,
            'lost_users':lost_user_counts
            #'retained_users_7d':retained_user_7d_counts
            }



@st.cache_data
def engagement_rate(dates):
    return active_users(dates)/registered_users([date[1] for date in dates])

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
temp = pd.DataFrame({'au':active_user_counts})
if au_freq=='Daily':
    temp['date'] = [x.strftime('%b-%d %a') for x in date_range_end]
    fig = px.line(temp, x="date", y='au', labels={'date':'Day', 'au':'DAU'})
elif au_freq=='Weekly':
    temp['date'] = [x[0].strftime('%b %d')+"-"+x[1].strftime('%b %d') for x in zip(date_range_start,date_range_end)]
    fig = px.line(temp, x="date", y='au', labels={'date':'Week', 'au':'WAU'},markers=True)
elif au_freq=='Bi-weekly':
    temp['date'] = [x[0].strftime('%b %d')+"-"+x[1].strftime('%b %d') for x in zip(date_range_start,date_range_end)]
    fig = px.line(temp, x="date", y='au', labels={'date':'Bi-week', 'au':'2WAU'},markers=True)
elif au_freq=='Monthly':
    temp['date'] = [x.strftime('%Y %b') for x in date_range_end]
    fig = px.line(temp, x="date", y='au', labels={'date':'Month', 'au':'MAU'},markers=True)
fig.update_yaxes(range=au_yrange)
au_expander.plotly_chart(fig, use_container_width=True)



nu_expander = st.expander("NU")
nu_expander.write("New Users")
nu_col1, nu_col2, nu_col3 = nu_expander.columns(3)
nu_from = nu_col1.date_input(label="From",value=default_from,key='nu_from')
nu_to = nu_col2.date_input(label="To",value=default_to,key='nu_to')
nu_freq = nu_col3.selectbox('Frequency',('Daily', 'Weekly', 'Weekly average', 'Bi-weekly', 'Monthly'),index=2,key='nu_freq')
nu_yrange = nu_expander.slider("Y-axis range", value=(0, 200), min_value=0, max_value=2000, step=50, key='nu_yrange')

date_range_start, date_range_end, date_range_str = get_dates(nu_from,nu_to,nu_freq)
new_user_counts = new_users(date_range_str)
if nu_freq=='Weekly average':
    date_range_start, date_range_end, date_range_str = get_dates(nu_from,nu_to,'Weekly')
    new_user_counts = np.round(new_users(date_range_str)/7,1)
else:
    date_range_start, date_range_end, date_range_str = get_dates(nu_from,nu_to,nu_freq)
    new_user_counts = new_users(date_range_str)

temp = pd.DataFrame({'nu':new_user_counts})
if nu_freq=='Daily':
    temp['date'] = [x.strftime('%b-%d %a') for x in date_range_end]
    fig = px.line(temp, x="date", y='nu', labels={'date':'Day', 'nu':'DNU'})
elif nu_freq=='Weekly':
    temp['date'] = [x[0].strftime('%b %d')+"-"+x[1].strftime('%b %d') for x in zip(date_range_start,date_range_end)]
    fig = px.line(temp, x="date", y='nu', labels={'date':'Week', 'nu':'WNU'},markers=True)
elif nu_freq=='Weekly average':
    temp['date'] = [x[0].strftime('%b %d')+"-"+x[1].strftime('%b %d') for x in zip(date_range_start,date_range_end)]
    fig = px.line(temp, x="date", y='nu', labels={'date':'Week', 'nu':'DNU (Weekly Average)'},markers=True)
elif nu_freq=='Bi-weekly':
    temp['date'] = [x[0].strftime('%b %d')+"-"+x[1].strftime('%b %d') for x in zip(date_range_start,date_range_end)]
    fig = px.line(temp, x="date", y='nu', labels={'date':'Bi-week', 'nu':'2WNU'},markers=True)
elif nu_freq=='Monthly':
    temp['date'] = [x.strftime('%Y %b') for x in date_range_end]
    fig = px.line(temp, x="date", y='nu', labels={'date':'Month', 'nu':'MNU'},markers=True)
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
er = engagement_rate(date_range_str)*100

temp = pd.DataFrame({'er':er})
if er_freq=='Daily':
    temp['date'] = [x.strftime('%b-%d %a') for x in date_range_end]
    fig = px.line(temp, x="date", y='er', labels={'date':'Day', 'er':'Daily Engagement Rate (%)'})
elif er_freq=='Weekly':
    temp['date'] = [x[0].strftime('%b %d')+"-"+x[1].strftime('%b %d') for x in zip(date_range_start,date_range_end)]
    fig = px.line(temp, x="date", y='er', labels={'date':'Week', 'er':'Weekly Engagement Rate (%)'},markers=True)
elif er_freq=='Bi-weekly':
    temp['date'] = [x[0].strftime('%b %d')+"-"+x[1].strftime('%b %d') for x in zip(date_range_start,date_range_end)]
    fig = px.line(temp, x="date", y='er', labels={'date':'Bi-week', 'er':'Bi-weekly Engagement Rate (%)'},markers=True)
elif er_freq=='Monthly':
    temp['date'] = [x.strftime('%Y %b') for x in date_range_end]
    fig = px.line(temp, x="date", y='er', labels={'date':'Month', 'er':'Monthly Engagement Rate (%)'},markers=True)
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
stickiness = au/mau*100
temp = pd.DataFrame({'stickiness':stickiness})

if stickiness_freq=='Daily':
    temp['date'] = [x.strftime('%b-%d %a') for x in date_range_end]
    fig = px.line(temp, x="date", y='stickiness', labels={'date':'Day', 'stickiness':'DAU/MAU (%)'})
else:
    temp['date'] = [x[0].strftime('%b %d')+"-"+x[1].strftime('%b %d') for x in zip(date_range_start,date_range_end)]
    fig = px.line(temp, x="date", y='stickiness', labels={'date':'Week', 'stickiness':'WAU/MAU (%)'},markers=True)

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
for i in range(len(date_range_str)):
    date = pd.to_datetime(np.array(date_range_str[i]))
    current_user_ids = set(df2[(df2['username_persistent']!='Anonymous')&(df2['requested_at'].dt.normalize()>=date[0])&(df2['requested_at'].dt.normalize()<=date[1])]['user_id'])
    #l = np.array([len(current_user_ids)]+list(active_users(date_range_str[i+1:],current_user_ids)))
    l = np.array(active_users(date_range_str[i:],current_user_ids))
    l = list(np.round(l/len(current_user_ids),3))
    data.append(l+[np.nan]*(len(date_range_str)-len(l)))
#data.append([1]+[np.nan]*(len(date_range_str)-1))
data = np.array(data)

if curr_freq=='Weekly' or curr_freq=='Bi-weekly':
    y = [x[0].strftime('%b %d')+"-"+x[1].strftime('%b %d') for x in zip(date_range_start,date_range_end)]
else:
    y = [x.strftime('%Y %b') for x in date_range_end]
fig = px.imshow(data,
                labels=dict(x="Period", y="Date", color="Retention Rate"),
                y=y,
                color_continuous_scale='rdylgn'
               )
fig.update_xaxes(side="top")

annotations = []
for i in range(len(date_range_str)):
    for j in range(len(date_range_str)):
        if not np.isnan(data[i, j]):
            if 0.6>data[i, j]>0.3:
                annotations.append(dict(x=j, y=i, text=f"{data[i, j]*100:.1f}%", showarrow=False, font=dict(color="black")))
            else:
                annotations.append(dict(x=j, y=i, text=f"{data[i, j]*100:.1f}%", showarrow=False, font=dict(color="white")))

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
for i in range(len(date_range_str)):
    date = pd.to_datetime(np.array(date_range_str[i]))
    new_user_ids = df1[(df1['date_joined'].dt.normalize()>=date[0])&(df1['date_joined'].dt.normalize()<=date[1])]['id']
    l = list(np.round(active_users(date_range_str[i:],new_user_ids)/len(new_user_ids),3))
    data.append(l+[np.nan]*(len(date_range_str)-len(l)))
data = np.array(data)

if nurr_freq=='Weekly' or nurr_freq=='Bi-weekly':
    y = [x[0].strftime('%b %d')+"-"+x[1].strftime('%b %d') for x in zip(date_range_start,date_range_end)]
else:
    y = [x.strftime('%Y %b') for x in date_range_end]
fig = px.imshow(data,
                labels=dict(x="Period", y="Date", color="Retention Rate"),
                y=y,
                color_continuous_scale='rdylgn'
               )
fig.update_xaxes(side="top")

# Customize the annotations to include percentage sign
annotations = []
for i in range(len(date_range_str)):
    for j in range(len(date_range_str)):
        if not np.isnan(data[i, j]):
            if 0.6>data[i, j]>0.3:
                annotations.append(dict(x=j, y=i, text=f"{data[i, j]*100:.1f}%", showarrow=False, font=dict(color="black")))
            else:
                annotations.append(dict(x=j, y=i, text=f"{data[i, j]*100:.1f}%", showarrow=False, font=dict(color="white")))

fig.update_layout(annotations=annotations)

nurr_expander.plotly_chart(fig, use_container_width=True)


