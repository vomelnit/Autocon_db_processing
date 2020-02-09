import mysql.connector
import pandas
import numpy
import math
import sys
import config
import datetime


from pandas import DataFrame

host=config.host
user=config.user
password=config.password
database=config.database
logfile_name = config.logfile_name

period_array = ["7 дней","14 дней","21 день","28 дней","месяц"]





# def Get_typeid_from_sql_database(curs,type_name):
#         curs.execute("Select type_id from Types where (type_name = '{0}')".format(type_name))
#         return curs.fetchall()[0][0]




def Get_dataframe_of_transaction_for_determined_month(curs,year,month,region):
    curs.execute(
        "SELECT aa_brands.name,aa_transactions.tr_day,aa_vehicle_type.title,aa_transactions.amount_of_units  "
        "from aa_transactions JOIN aa_brand_to_vt ON aa_transactions.brand_to_type_id = aa_brand_to_vt.id join  aa_brands on aa_brands.id = aa_brand_to_vt.brand_id join aa_vehicle_type on aa_vehicle_type.id = aa_brand_to_vt.vt_id  where (tr_year ={0})&(tr_month={1})&(region_id={2}) ORDER BY aa_vehicle_type.title,aa_transactions.tr_day,aa_brands.name;".format(year,month,region))
    month_df = pandas.DataFrame(data=curs.fetchall(),columns = ['Brand' , 'Period', 'Type', 'Amount'])
    month_df.index = month_df.index + 1
    return month_df

# def Create_dataframe_of_brands_for_each_period (df,type,period_arr):
#     df2 = pandas.DataFrame(data =df[df.Type == n_type ].Brand.unique(),columns= ["Brand"] )
#     for nperiod in period_arr:
#         df2 = df2.join(df[(df["Type"] == type)&(df["Period"]==nperiod)][['Brand','Amount']].set_index('Brand'), on='Brand',how='left')
#         df2 = df2.rename(columns={'Amount':nperiod})
#     return df2

# def Get_dataframe_of_resulting_rating_table(curs):
#     curs.execute(
#         "SELECT  brand_name,amount_for_7_days,amount_for_14_days,amount_for_21_days,amount_for_28_days,amount_for_month,type_name from Rating where (year =2020)&(month=1)"
#         " ORDER BY type_name,brand_name;")
#
#     month_df = pandas.DataFrame(data=curs.fetchall(), columns=['Brand', '7 дней', '14 дней', '21 день', '28 дней' , 'месяц','Type'])
#     month_df.index = month_df.index + 1
#     return month_df
#
def Get_dataframe_of_current_diagram_table(curs,year,month):
    curs.execute("SELECT  vehicle_type_id,dg_day,amount_of_units,dynamic_compared_to_previous_month,dynamic_compared_to_previous_year "
                 "from aa_diagram where (dg_year ={0})&(dg_month={1}) ORDER BY vehicle_type_id,dg_day;".format(year,month))

    month_df = pandas.DataFrame(data=curs.fetchall(),columns=['Type','Period', 'Present_month_amount', 'Dynamic_by_month', 'Predicted_dynamic_prev_year' ])
    return month_df

# def Insert_dataframe_to_rating_table_throught_sql(curs,df,type_id,type_name):
#
#     for currentRow in range(len(df)):
#         RowList = []
#         for Value in df.loc[currentRow]:
#             if pandas.isna(Value):
#                 RowList.append('NULL')
#             elif type(Value) == numpy.float64:
#                 Value = int(Value)
#                 RowList.append(Value)
#             else:
#                 RowList.append(Value)
#         sql_query = "INSERT INTO Rating (year, month,type_id ,type_name ,brand_id ,brand_name  ,amount_for_7_days ,	amount_for_14_days,	amount_for_21_days ,amount_for_28_days,	amount_for_month)" \
#                     " VALUES ({0},{1},{2},'{3}',{4},'{5}',{6},{7},{8},{9},{10});".format(year,month,type_id,type_name,RowList[6],RowList[0],RowList[1],RowList[2],RowList[3],RowList[4],RowList[5])
#         curs.execute(sql_query)

# def Upload_dataframe_to_rating_table_throught_sql(curs,df,type_id,type_name):
#
#     for currentRow in range(len(df)):
#         RowList = []
#         for Value in df.loc[currentRow]:
#             if pandas.isna(Value):
#                 RowList.append('NULL')
#             elif type(Value) == numpy.float64:
#                 Value = int(Value)
#                 RowList.append(Value)
#             else:
#                 RowList.append(Value)
#         sql_query = "Update Rating SET amount_for_7_days = {0} ,amount_for_14_days = {1},	amount_for_21_days = {2} ,amount_for_28_days = {3},	amount_for_month = {4} " \
#                     "where (year = {5})&(month = {6})& (type_id = {7})&( type_name = '{8}')&(brand_id = {9})" \
#                     "&(brand_name = '{10}');".format(RowList[1],RowList[2],RowList[3],RowList[4],RowList[5],year,month,type_id,type_name,RowList[6],RowList[0])
#         curs.execute(sql_query)



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
        sql_query = "INSERT INTO aa_diagram (dg_year, dg_month,dg_day,vehicle_type_id ,amount_of_units , dynamic_compared_to_previous_month ,dynamic_compared_to_previous_year,predicted_market_volume)" \
                    " VALUES ({0},{1},'{2}','{3}',{4},{5},{6},{7});".format(year,month,RowList[1],RowList[0],RowList[2],RowList[5],RowList[9],RowLsit[8])
        curs.execute(sql_query)


def Upload_dataframe_to_diagram_table_throught_sql(curs,df,year,month):

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
        RowList[8] = int(RowList[8])
        sql_query = "Update aa_diagram SET amount_of_units = {0},dynamic_compared_to_previous_month = {1}, dynamic_compared_to_previous_year = {2}, predicted_market_volume = {3}" \
                    " where (dg_year = {4})&(dg_month = {5})& (dg_day = {6})&( vehicle_type_id = '{7}');".format(RowList[2], RowList[5], RowList[9],RowList[8], year, month, RowList[1], RowList[0])
        curs.execute(sql_query)


# def Join_to_df_brand_id_from_sql_database(curs,df,type_id):
#     curs.execute("Select brand_id,name from Brands where (type_id = {0});".format(type_id))
#     brand_df = pandas.DataFrame(data=curs.fetchall(),columns=["brand_id","Brand"])
#     df = df.join (brand_df.set_index("Brand"), on = "Brand",how = 'left')
#     return df


def Split_df_to_insert_set_and_upload_set_return_list(df_for_split,df_of_existing_rows,param_list,type_name):

    Insert_df = df_for_split
    Upload_df = df_for_split

    if (len(param_list)==2):
        if type(param_list[0]) != str: param_list[0]=str(param_list[0])
        if type(param_list[1]) != str: param_list[1] = str(param_list[1])

        for currentparam1 in df_for_split[param_list[0]]:
            for currentparam2 in df_for_split[df_for_split[param_list[0]] == currentparam1][param_list[1]]:
                if (df_of_existing_rows[(df_of_existing_rows[param_list[1]] == currentparam2)&
                                        (df_of_existing_rows[param_list[0]] == currentparam1)].empty is False):
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

def logging(filename,message):
    file = open(filename, "a")
    file.write(str(datetime.datetime.now())+": "+message+"\n")
    file.close()

if __name__ == "__main__":
    print("Begin")
    try:
        if (len(sys.argv)>4)or(len(sys.argv)<3):raise Exception('Wrong quantity of argv')
        year = int(sys.argv[1])
        month = int(sys.argv[2])
        if len(sys.argv)==3: region = 1
        elif len(sys.argv)==4: region = int(sys.argv[3])
        #if (sys.argv[2]<1)or (sys.argv[2]>12): raise Exception('Wrong month entered')
        print("Get argv[]")
    except IndexError:
        logging(logfile_name, "Not enough arguments: 1 argv is year, 2 argv is month")
        quit()
    except ValueError:
        logging(logfile_name, "Wrong arguments, must be int")
        quit()
    except Exception as ex:
        logging(logfile_name,str(ex))
        quit()





    try:

        Autocon_database = mysql.connector.connect(
          host=host,
          user=user,
          passwd=password,
          database=database
        )
        cursor = Autocon_database.cursor()
        print("Connect to database")
    except Exception as err:
        logging(logfile_name, "Problem with database connection"+str(err))
        quit()


    try:

        #Fill Rating table
        month_transactions_df = Get_dataframe_of_transaction_for_determined_month(cursor,year,month,region)
        if (month_transactions_df.empty): raise Exception('No rows in Transactions table in entered month')
        month_general_df = month_transactions_df
        print("Selected data from Transactions table")
        # Current_table_of_rating_df = Get_dataframe_of_resulting_rating_table(cursor)
        # Current_table_of_rating_df.fillna(value=numpy.nan, inplace=True)
        # vehicle_type_array = month_transactions_df.Type.unique()
        # for n_type in vehicle_type_array:
        #
        #     Entered_data_for_month_df = Create_dataframe_of_brands_for_each_period(month_transactions_df,n_type,period_array)
        #     comparing_result_df = Entered_data_for_month_df.merge(Current_table_of_rating_df[Current_table_of_rating_df.Type == n_type ], how = 'outer' ,indicator=True)
        #
        #     comparing_result_df = comparing_result_df.drop('Type', 1).loc[lambda x : x['_merge']=='left_only'].drop('_merge', 1)
        #
        #     if (comparing_result_df.empty is False):
        #         type_id = Get_typeid_from_sql_database(cursor, n_type)
        #         comparing_result_df = Join_to_df_brand_id_from_sql_database(cursor,comparing_result_df,type_id)
        #
        #
        #         list_of_insert_upload = Split_df_to_insert_set_and_upload_set_return_list(df_for_split=comparing_result_df,df_of_existing_rows=Current_table_of_rating_df,param_list=["Brand"],type_name=n_type)
        #         Insert_df = list_of_insert_upload[0]
        #         Upload_df = list_of_insert_upload[0]
        #
        #         # Insert_df = comparing_result_df
        #         # Upload_df = comparing_result_df
        #         #
        #         # for currentBrand in comparing_result_df["Brand"]:
        #         #
        #         #     if (Current_table_of_rating_df[Current_table_of_rating_df.Type == n_type ][Current_table_of_rating_df.Brand == currentBrand].empty==False):
        #         #         Insert_df = Insert_df[Insert_df.Brand != currentBrand]
        #         #
        #         #     else:
        #         #         Upload_df = Upload_df[Upload_df.Brand != currentBrand]
        #
        #
        #         if (Insert_df.empty == False):
        #             Insert_df = Insert_df.reset_index(drop=True)
        #             Insert_dataframe_to_rating_table_throught_sql(cursor,Insert_df,type_id,n_type)
        #         if (Upload_df == False):
        #             Upload_df=Upload_df.reset_index(drop=True)
        #             Upload_dataframe_to_rating_table_throught_sql(cursor,Upload_df,type_id,n_type)


        #Fill Diagram table

        if (month != 1):
            previous_month_general_df = Get_dataframe_of_transaction_for_determined_month(cursor,year,month-1,region)
            cursor.execute("select aa_vehicle_type.title,aa_real_sales.amount_of_units from aa_real_sales join aa_vehicle_type on aa_vehicle_type.id = aa_real_sales.vehicle_type_id where (dg_year = {0})&(dg_month = {1});".format(year,month-1))
            previous_month_volume_of_market = pandas.DataFrame(data=cursor.fetchall(), columns=["Type", "Volume_prev_month"])
        else:
            previous_month_general_df = Get_dataframe_of_transaction_for_determined_month(cursor, year-1, 12,region)
            cursor.execute("select aa_vehicle_type.title,aa_real_sales.amount_of_units from aa_real_sales join aa_vehicle_type on aa_vehicle_type.id = aa_real_sales.vehicle_type_id where (dg_year = {0})&(dg_month = {1});".format(year-1,12))
            previous_month_volume_of_market = pandas.DataFrame(data=cursor.fetchall(),columns=["Type","Volume_prev_month"])
        if (previous_month_general_df.empty):
            print("if you see it, previous month transactions are not in table")
            logging(logfile_name, "No data for previous month")
        else:
            try:
                cursor.execute("select aa_vehicle_type.title, aa_real_sales.amount_of_units from aa_real_sales join aa_vehicle_type on aa_vehicle_type.id = aa_real_sales.vehicle_type_id where (dg_year = {0})&(dg_month = {1});".format(year - 1, month))
                previous_year_volume_of_market = pandas.DataFrame(data=cursor.fetchall(), columns=["Type", "Volume_prev_year"])

                previous_month_volume_of_market = Add_sum_rows_of_units_by_period_to_df(df=previous_month_volume_of_market,colum_name_to_sum="Volume_prev_month")
                previous_year_volume_of_market = Add_sum_rows_of_units_by_period_to_df(df= previous_year_volume_of_market,colum_name_to_sum="Volume_prev_year")
                print("Selected previous month and year volume and privious month transactions")
            except Exception as err:
                logging(logfile_name,str(err))
                raise Exception("Error with select entire volume of market")



            df_for_dynamic = previous_month_general_df[["Brand","Period","Type"]].merge(month_general_df[["Brand","Period","Type"]], how = 'inner')
            print("Comparing actual and previosu month")
            if (df_for_dynamic.empty is False):
                print("if you see it comparations is not null")
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
                print("Created dataframe of insert/upload rows in diagram table (can be empty")
                if (comparing_result_diagram_df.empty==False):
                    print("Dataframe is not empty")
                    list_of_insert_and_upload = Split_df_to_insert_set_and_upload_set_return_list(df_for_split=comparing_result_diagram_df,df_of_existing_rows=current_diagram_df,param_list = ["Period","Type"],type_name=None)

                    Insert_df = list_of_insert_and_upload[0]
                    Upload_df = list_of_insert_and_upload[1]
                    print("Separated insert df and upload df")
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
                        Upload_dataframe_to_diagram_table_throught_sql(curs =cursor,df=Upload_df,year=year,month = month)



        Autocon_database.commit()
    except Exception as er:
        logging(logfile_name, str(er))
    finally:
        Autocon_database.close()
        logging(logfile_name, "OK")

