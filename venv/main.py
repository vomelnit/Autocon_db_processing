import mysql.connector
import pandas
import numpy
import math
import sys
import config

#pass=Womelnit_1234%
#user=womelnit

from pandas import DataFrame

period_array = ["7 дней","14 дней","21 день","28 дней","месяц"]





def Get_typeid_from_sql_database(curs,type_name):
        curs.execute("Select type_id from Types where (type_name = '{0}')".format(type_name))
        return curs.fetchall()[0][0]




def Get_dataframe_of_transaction_for_determined_month(curs,year,month):
    curs.execute(
        "SELECT Brands.name,Transactions.period,Transactions.type_name,Transactions.amount_of_units  "
        "from Transactions JOIN Brands ON Transactions.brand_id = Brands.brand_id  where (year ={0})&(month={1}) ORDER BY Transactions.type_name,Transactions.period,Brands.name;".format(year,month))
    month_df = pandas.DataFrame(data=curs.fetchall(),columns = ['Brand' , 'Period', 'Type', 'Amount'])
    month_df.index = month_df.index + 1
    return month_df

def Create_dataframe_of_brands_for_each_period (df,type,period_arr):
    df2 = pandas.DataFrame(data =df[df.Type == n_type ].Brand.unique(),columns= ["Brand"] )
    for nperiod in period_arr:
        df2 = df2.join(df[df.Type == type][df.Period==nperiod][['Brand','Amount']].set_index('Brand'), on='Brand',how='left')
        df2 = df2.rename(columns={'Amount':nperiod})
    return df2

def Get_dataframe_of_resulting_rating_table(curs):
    curs.execute(
        "SELECT  brand_name,amount_for_7_days,amount_for_14_days,amount_for_21_days,amount_for_28_days,amount_for_month,type_name from Rating where (year =2020)&(month=1)"
        " ORDER BY type_name,brand_name;")

    month_df = pandas.DataFrame(data=curs.fetchall(), columns=['Brand', '7 дней', '14 дней', '21 день', '28 дней' , 'месяц','Type'])
    month_df.index = month_df.index + 1
    return month_df

def Get_dataframe_of_current_diagram_table(curs,year,month):
    curs.execute("SELECT  type_name,period,amount_of_units,amount_of_brands,dynamic_compared_to_previous_month,pridicted_market_volume,dynamic_compared_to_previous_year "
                 "from Diagram where (year ={0})&(month={1}) ORDER BY type_name,period;".format(year,month))

    month_df = pandas.DataFrame(data=curs.fetchall(),
                                columns=['Type','Period', 'Present_month_amount', 'Calculated_companies', 'Dynamic_by_month', 'Predicted_volume_of_month', 'Predicted_dynamic_prev_year' ])
    return month_df

def Insert_dataframe_to_rating_table_throught_sql(curs,df,type_id,type_name):

    for currentRow in range(len(df)):
        RowList = []
        for Value in df.loc[currentRow]:
            if pandas.isna(Value):
                RowList.append('NULL')
            elif type(Value) == numpy.float64:
                Value = int(Value)
                RowList.append(Value)
            else:
                RowList.append(Value)
        sql_query = "INSERT INTO Rating (year, month,type_id ,type_name ,brand_id ,brand_name  ,amount_for_7_days ,	amount_for_14_days,	amount_for_21_days ,amount_for_28_days,	amount_for_month)" \
                    " VALUES ({0},{1},{2},'{3}',{4},'{5}',{6},{7},{8},{9},{10});".format(year,month,type_id,type_name,RowList[6],RowList[0],RowList[1],RowList[2],RowList[3],RowList[4],RowList[5])
        curs.execute(sql_query)

def Upload_dataframe_to_rating_table_throught_sql(curs,df,type_id,type_name):

    for currentRow in range(len(df)):
        RowList = []
        for Value in df.loc[currentRow]:
            if pandas.isna(Value):
                RowList.append('NULL')
            elif type(Value) == numpy.float64:
                Value = int(Value)
                RowList.append(Value)
            else:
                RowList.append(Value)
        sql_query = "Update Rating SET amount_for_7_days = {0} ,amount_for_14_days = {1},	amount_for_21_days = {2} ,amount_for_28_days = {3},	amount_for_month = {4} " \
                    "where (year = {5})&(month = {6})& (type_id = {7})&( type_name = '{8}')&(brand_id = {9})" \
                    "&(brand_name = '{10}');".format(RowList[1],RowList[2],RowList[3],RowList[4],RowList[5],year,month,type_id,type_name,RowList[6],RowList[0])
        curs.execute(sql_query)



def Insert_dataframe_to_diagram_table_throught_sql(curs,df,year,month):

    for currentRow in range(len(df)):
        RowList = []
        for Value in df.loc[currentRow]:
            if pandas.isna(Value):
                RowList.append('NULL')
            elif type(Value) == numpy.float64:
                Value = round(Value,2)
                RowList.append(Value)
            else:
                RowList.append(Value)

        RowList[8]= int(RowList[8])
        sql_query = "INSERT INTO Diagram (year, month,period,type_name ,amount_of_brands,amount_of_units ,pridicted_market_volume,	dynamic_compared_to_previous_month ,dynamic_compared_to_previous_year)" \
                    " VALUES ({0},{1},'{2}','{3}',{4},{5},{6},{7},{8});".format(year,month,RowList[1],RowList[0],RowList[3],RowList[2],RowList[8],RowList[5],RowList[9])
        print(sql_query)
        curs.execute(sql_query)


def Upload_dataframe_to_diagram_table_throught_sql(curs,df,year,month):

    for currentRow in range(len(df)):
        RowList = []
        for Value in df.loc[currentRow]:
            if pandas.isna(Value):
                RowList.append('NULL')
            elif type(Value) == numpy.float64:
                Value = int(Value)
                RowList.append(Value)
            else:
                RowList.append(Value)

        sql_query = "Update Diagram SET amount_of_brands = {0} ,amount_of_units = {1},	pridicted_market_volume = {2} ,dynamic_compared_to_previous_month = {3}, dynamic_compared_to_previous_year = {4}" \
                    " where (year = {5})&(month = {6})& (period = {7})&( type_name = '{8}');".format(RowList[3], RowList[2], RowList[8], RowList[5], RowList[9], year, month, RowList[1], RowList[0])
        print(sql_query)
        curs.execute(sql_query)


def Join_to_df_brand_id_from_sql_database(curs,df,type_id):
    curs.execute("Select brand_id,name from Brands where (type_id = {0});".format(type_id))
    brand_df = pandas.DataFrame(data=curs.fetchall(),columns=["brand_id","Brand"])
    df = df.join (brand_df.set_index("Brand"), on = "Brand",how = 'left')
    return df


def Split_df_to_insert_set_and_upload_set_return_list(df_for_split,df_of_existing_rows,param_list,type_name):

    Insert_df = df_for_split
    Upload_df = df_for_split

    if (len(param_list)==2):
        if type(param_list[0]) != str: param_list[0]=str(param_list[0])
        if type(param_list[1]) != str: param_list[1] = str(param_list[1])

        for currentparam1 in df_for_split[param_list[0]]:
            for currentparam2 in df_for_split[df_for_split[param_list[0]] == currentparam1][param_list[1]]:
                if (df_of_existing_rows[df_of_existing_rows[param_list[1]] == currentparam2][
                    df_of_existing_rows[param_list[0]] == currentparam1].empty is False):
                    Insert_df = Insert_df[(Insert_df[param_list[0]] != currentparam1) & (Insert_df[param_list[0]] != currentparam2)]
                else:
                    Upload_df = Upload_df[(Upload_df[param_list[0]] != currentparam1) & (Upload_df[param_list[1]] != currentparam2)]

    elif (len(param_list)==1):
        if type(param_list[0]) != str: param_list[0] = str(param_list[0])

        for currentparam in df_for_split[param_list[0]]:
            if (df_of_existing_rows[df_of_existing_rows.Type == type_name][df_of_existing_rows[currentparam] == currentparam].empty is False):
                Insert_df = Insert_df[Insert_df[currentparam] != currentparam]
            else:
                Upload_df = Upload_df[Upload_df[currentparam] != currentparam]

    else: return [pandas.DataFrame.empty,pandas.DataFrame.empty]

    return [Insert_df,Upload_df]



def Add_sum_rows_of_units_by_period_to_df(df,colum_name_to_sum):
    sum_of_column = df.sum(axis=0, skipna=True)[colum_name_to_sum]
    df = df.append({'Type': "Sum",colum_name_to_sum: sum_of_column}, ignore_index=True)

    return df

if __name__ == "__main__":

    try:
        year = sys.argv[1]
        month = sys.argv[2]
        if (sys.argv[2]<1)or (sys.argv[2]>12): raise Exception
    except IndexError:
        print("Not enough arguments: 1 argv is year, 2 argv is month")
        quit()
    except Exception:
        quit()


    try:
        Autocon_database = mysql.connector.connect(
          host=config.host,
          user=config.user,
          passwd=config.password,
          database=config.database
        )
        print(config.host)
        cursor = Autocon_database.cursor()
    except Exception as err:
        print(err)
        quit()


    try:

        #Fill Rating table
        month_transactions_df = Get_dataframe_of_transaction_for_determined_month(cursor,year,month)
        if (month_transactions_df.empty): raise Exception
        month_general_df = month_transactions_df
        Current_table_of_rating_df = Get_dataframe_of_resulting_rating_table(cursor)
        Current_table_of_rating_df.fillna(value=numpy.nan, inplace=True)
        vehicle_type_array = month_transactions_df.Type.unique()
        for n_type in vehicle_type_array:

            Entered_data_for_month_df = Create_dataframe_of_brands_for_each_period(month_transactions_df,n_type,period_array)
            comparing_result_df = Entered_data_for_month_df.merge(Current_table_of_rating_df[Current_table_of_rating_df.Type == n_type ], how = 'outer' ,indicator=True)

            comparing_result_df = comparing_result_df.drop('Type', 1).loc[lambda x : x['_merge']=='left_only'].drop('_merge', 1)

            if (comparing_result_df.empty is False):
                type_id = Get_typeid_from_sql_database(cursor, n_type)
                comparing_result_df = Join_to_df_brand_id_from_sql_database(cursor,comparing_result_df,type_id)


                list_of_insert_upload = Split_df_to_insert_set_and_upload_set_return_list(df_for_split=comparing_result_df,df_of_existing_rows=Current_table_of_rating_df,param_list=["Brand"],type_name=n_type)
                Insert_df = list_of_insert_upload[0]
                Upload_df = list_of_insert_upload[0]

                # Insert_df = comparing_result_df
                # Upload_df = comparing_result_df
                #
                # for currentBrand in comparing_result_df["Brand"]:
                #
                #     if (Current_table_of_rating_df[Current_table_of_rating_df.Type == n_type ][Current_table_of_rating_df.Brand == currentBrand].empty==False):
                #         Insert_df = Insert_df[Insert_df.Brand != currentBrand]
                #
                #     else:
                #         Upload_df = Upload_df[Upload_df.Brand != currentBrand]


                if (Insert_df.empty == False):
                    Insert_df = Insert_df.reset_index(drop=True)
                    Insert_dataframe_to_rating_table_throught_sql(cursor,Insert_df,type_id,n_type)
                if (Upload_df == False):
                    Upload_df=Upload_df.reset_index(drop=True)
                    Upload_dataframe_to_rating_table_throught_sql(cursor,Upload_df,type_id,n_type)


        #Fill Diagram table

        if (month != 1):
            previous_month_general_df = Get_dataframe_of_transaction_for_determined_month(cursor,year,month-1)
            cursor.execute("select type_name,amount_of_units from Volume_of_market join Types on Types.type_id = Volume_of_market.type_id where (year = {0})&(month = {1});".format(year,month-1))
            previous_month_volume_of_market = pandas.DataFrame(data=cursor.fetchall(), columns=["Type", "Volume_prev_month"])
        else:
            previous_month_general_df = Get_dataframe_of_transaction_for_determined_month(cursor, year-1, 12)
            cursor.execute("select type_name,amount_of_units from Volume_of_market join Types on Types.type_id = Volume_of_market.type_id where (year = {0})&(month = {1});".format(year-1,12))
            previous_month_volume_of_market = pandas.DataFrame(data=cursor.fetchall(),columns=["Type","Volume_prev_month"])

        if (previous_month_general_df.empty):
            pass

        else:
            cursor.execute("select type_name,amount_of_units from Volume_of_market join Types on Types.type_id = Volume_of_market.type_id where (year = {0})&(month = {1});".format(year - 1, month))
            previous_year_volume_of_market = pandas.DataFrame(data=cursor.fetchall(), columns=["Type", "Volume_prev_year"])
            previous_month_volume_of_market = Add_sum_rows_of_units_by_period_to_df(df=previous_month_volume_of_market,colum_name_to_sum="Volume_prev_month")
            previous_year_volume_of_market = Add_sum_rows_of_units_by_period_to_df(df= previous_year_volume_of_market,colum_name_to_sum="Volume_prev_year")




            df_for_dynamic = previous_month_general_df[["Brand","Period","Type"]].merge(month_general_df[["Brand","Period","Type"]], how = 'inner')
            df_for_dynamic = df_for_dynamic.merge(previous_month_general_df,  how='left', left_on=["Brand","Period","Type"], right_on = ["Brand","Period","Type"])
            df_for_dynamic = df_for_dynamic.rename(columns={'Amount':"Previous_month_amount"})
            df_for_dynamic = df_for_dynamic.merge(month_general_df,  how='left', left_on=["Brand","Period","Type"], right_on = ["Brand","Period","Type"])
            df_for_dynamic = df_for_dynamic.rename(columns={'Amount':"Present_month_amount"})


            df_for_dynamic_copy_for_prev_month = df_for_dynamic
            df_for_dynamic_copy_for_prev_month = df_for_dynamic_copy_for_prev_month.groupby(['Type',"Period"]).agg(Sum=('Previous_month_amount', 'sum')).reset_index()
            df_for_dynamic = df_for_dynamic.groupby(['Type',"Period"]).agg(Sum=('Present_month_amount', 'sum'), Count=('Previous_month_amount', 'count')).reset_index()
            df_for_dynamic = df_for_dynamic.rename(columns={'Sum':"Present_month_amount",'Count':"Calculated_companies"})
            df_for_dynamic_copy_for_prev_month = df_for_dynamic_copy_for_prev_month.rename(columns={'Sum':"Previous_month_amount"})


            sum_of_df_for_dynamic = df_for_dynamic.groupby(['Period']).agg(Sum=('Present_month_amount', 'sum'), Count=('Calculated_companies', 'sum')).reset_index()
            sum_of_df_for_dynamic.insert(0, 'Type', 'Sum')
            sum_of_df_for_dynamic = sum_of_df_for_dynamic.rename(columns={'Sum': "Present_month_amount",'Count': 'Calculated_companies'})
            df_for_dynamic = df_for_dynamic.append(sum_of_df_for_dynamic,ignore_index=True)

            sum_of_df_for_dynamic_copy = df_for_dynamic_copy_for_prev_month.groupby(['Period']).agg(Sum=('Previous_month_amount', 'sum')).reset_index()
            sum_of_df_for_dynamic_copy.insert(0, 'Type', 'Sum')
            sum_of_df_for_dynamic_copy = sum_of_df_for_dynamic_copy.rename(columns={'Sum': "Previous_month_amount"})
            df_for_dynamic_copy_for_prev_month = df_for_dynamic_copy_for_prev_month.append(sum_of_df_for_dynamic_copy, ignore_index=True)




            df_for_dynamic = df_for_dynamic.merge(df_for_dynamic_copy_for_prev_month,  how='left', left_on=["Type","Period"], right_on = ["Type","Period"])
            df_for_dynamic['Dynamic_by_month'] = df_for_dynamic.Present_month_amount / df_for_dynamic.Previous_month_amount*100-100




            df_for_dynamic = df_for_dynamic.merge(previous_month_volume_of_market,  how='left', left_on=["Type"], right_on = ["Type"])
            df_for_dynamic = df_for_dynamic.merge(previous_year_volume_of_market,  how='left', left_on=["Type"], right_on = ["Type"])
            df_for_dynamic['Predicted_volume_of_month'] = df_for_dynamic.Volume_prev_month * (df_for_dynamic.Dynamic_by_month +100)/100
            df_for_dynamic['Predicted_dynamic_prev_year'] = df_for_dynamic.Predicted_volume_of_month / df_for_dynamic.Volume_prev_year *100 - 100


            current_diagram_df = Get_dataframe_of_current_diagram_table(cursor,year,month)

            comparing_result_diagram_df = df_for_dynamic.merge(current_diagram_df, how = 'outer' ,indicator=True)
            comparing_result_diagram_df = comparing_result_diagram_df.loc[lambda x: x['_merge'] == 'left_only'].drop('_merge',1)



            if (comparing_result_diagram_df.empty==False):

                list_of_insert_and_upload = Split_df_to_insert_set_and_upload_set_return_list(df_for_split=comparing_result_diagram_df,df_of_existing_rows=current_diagram_df,param_list = ["Period","Type"],type_name=None)

                Insert_df = list_of_insert_and_upload[0]
                Upload_df = list_of_insert_and_upload[1]
                # Insert_df = comparing_result_diagram_df
                # Upload_df = comparing_result_diagram_df
                #
                #
                #
                # for currentPeriod in comparing_result_diagram_df["Period"]:
                #     for currentType in comparing_result_diagram_df[comparing_result_diagram_df.Period == currentPeriod]["Type"]:
                #         if (current_diagram_df[current_diagram_df.Type == currentType ][current_diagram_df.Period == currentPeriod ].empty == False):
                #             Insert_df = Insert_df[(Insert_df['Period'] != currentPeriod) & (Insert_df['Type'] != currentType)]
                #         else:
                #             Upload_df = Upload_df[(Upload_df['Period'] != currentPeriod) & (Upload_df['Type'] != currentType)]

                if (Insert_df.empty is False):
                    Insert_df = Insert_df.reset_index(drop=True)
                    Insert_dataframe_to_diagram_table_throught_sql(curs =cursor,df=Insert_df,year=year,month = month)
                if (Upload_df.empty is False):
                    Upload_df=Upload_df.reset_index(drop=True)
                    Upload_dataframe_to_diagram_table_throught_sql(curs =cursor,df=Insert_df,year=year,month = month)



        Autocon_database.commit()
    except Exception as er:
        print(err)
    finally:
        Autocon_database.close()

