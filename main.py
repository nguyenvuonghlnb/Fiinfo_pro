import time
from datetime import datetime
import schedule
from apscheduler.schedulers.blocking import BlockingScheduler
from controller import message, company, financial, corporate_action, ratio, market

scheduler = BlockingScheduler()
print("Start deploying !", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
def import_financial_datas_min():
    date_request = datetime.now().strftime("%Y-%m-%d")
    time.sleep(5)
    financial.get_balance_sheet(date_request)
    time.sleep(30)
    financial.get_income_statement(date_request)
    time.sleep(30)


def import_message_datas():
    date_request = datetime.now().strftime("%Y-%m-%d")
    time.sleep(5)
    message.get_announcement_vi(date_request)
    time.sleep(30)


def import_corporate_action_datas():
    date_request = datetime.now().strftime("%Y-%m-%d")
    time.sleep(10)
    corporate_action.get_cash_dividend_payout(date_request)
    time.sleep(30)
    corporate_action.get_cash_dividend_plan(date_request)
    time.sleep(30)
    corporate_action.get_corporate_deal(date_request)
    time.sleep(30)
    corporate_action.get_person_deal(date_request)
    time.sleep(30)
    corporate_action.get_share_issue(date_request)
    time.sleep(30)
    corporate_action.get_stock_dividend_plan(date_request)
    time.sleep(30)


def import_company_datas():
    date_request = datetime.now().strftime("%Y-%m-%d")
    time.sleep(10)
    company.get_organization(date_request)
    time.sleep(30)
    company.get_company_information(date_request)
    time.sleep(30)


def import_ratio_datas():
    date_request = datetime.now().strftime("%Y-%m-%d")
    time.sleep(10)
    ratio.get_ratio_IcbIndustry(date_request)
    time.sleep(30)
    ratio.get_ratio_index(date_request)
    time.sleep(30)
    ratio.get_ratio_TTM(date_request)
    time.sleep(30)
    ratio.get_return_stock(date_request)
    time.sleep(30)
    ratio.get_ttm_daily(date_request)
    time.sleep(30)


def import_market_datas():
    date_request = datetime.now().strftime("%Y-%m-%d")
    time.sleep(10)
    market.get_hnx_stock(date_request)
    time.sleep(30)
    market.get_hose_index(date_request)
    time.sleep(30)
    market.get_hose_stock(date_request)
    time.sleep(30)
    market.get_icb_industry(date_request)
    time.sleep(30)
    market.get_upcom_stock(date_request)
    time.sleep(30)


# if __name__ == '__main__':
#     import_financial_datas_min()
#     import_message_datas()
#     import_corporate_action_datas()
#     import_company_datas()
#     import_ratio_datas()
#     import_market_datas()

# def job_financial():
#     now = datetime.now(import_financial_datas_min())
#     datetime_string = now.strftime('%Y-%m-%d %H:%M:%S')
#     print("Insert data 2tab /Financial !", datetime_string)
#
#
# def job_message():
#     now = datetime.now(import_message_datas())
#     datetime_string = now.strftime('%Y-%m-%d %H:%M:%S')
#     print("Insert data /Messages !", datetime_string)
#
#
# def job_corporate_action():
#     now = datetime.now(import_corporate_action_datas())
#     datetime_string = now.strftime('%Y-%m-%d %H:%M:%S')
#     print("Insert data 6tab /CorporateAction !", datetime_string)
#
#
# def job_company():
#     now = datetime.now(import_company_datas())
#     datetime_string = now.strftime('%Y-%m-%d %H:%M:%S')
#     print("Insert data 2tab /Company !", datetime_string)
#
#
# def job_ratio():
#     now = datetime.now(import_ratio_datas())
#     datetime_string = now.strftime('%Y-%m-%d %H:%M:%S')
#     print("Insert data 5tab /Ratio !", datetime_string)
#
#
# def job_market():
#     now = datetime.now(import_market_datas())
#     datetime_string = now.strftime('%Y-%m-%d %H:%M:%S')
#     print("Insert data 5tab /Maket !", datetime_string)
#
#
# def schedule_financial_10minutes():  # /Financial: 10min/time 09h-18h
#     job_financial()
#     schedule.every(10).minutes.until("18:00").do(job_financial)
#     print("---------------10 minutes/time 09h to 18h---------------")
#     return schedule.CancelJob
#
#
# def schedule_messages_15minutes():  # /Message: 15min/time 08h-18h
#     job_message()
#     schedule.every(15).minutes.until("18:00").do(job_message)
#     print("---------------15 minutes/time 08h to 18h---------------")
#     return schedule.CancelJob
#
#
# def schedule_corporate_action_40minutes():  # /CorporateAction: 40min/time 09h-18h
#     job_corporate_action()
#     schedule.every(40).minutes.until("18:00").do(job_corporate_action)
#     print("---------------40 minutes/time 09h to 18h---------------")
#     return schedule.CancelJob
#
#
# def schedule_company_one_hours():  # /Company: 1h/time 09h-18h
#     job_company()
#     schedule.every(1).hours.until("18:00").do(job_company)
#     print("---------------1 hours/time 08h to 18h---------------")
#     return schedule.CancelJob
#
#
# def schedule_ratio_two_times():  # /Ratio: Call 2 time 18h, 21h
#     job_ratio()
#     schedule.every(3).hours.until("23:20").do(job_ratio)
#     print("---------------Call 2 time 18h & 21h---------------")
#     return schedule.CancelJob
#
#
# def schedule_market_three_times():  # /Market: Call 3 time 15h30, 17h30, 19h30
#     job_market()
#     schedule.every(2).hours.until("23:00").do(job_market)
#     print("---------------Call 3 time 15h30 & 17h30 & 19h30---------------")
#     return schedule.CancelJob
#
#
# schedule.every().day.at("09:02").do(schedule_financial_10minutes)  # 10min/time 09h-18h
# schedule.every().day.at("09:00").do(schedule_messages_15minutes)  # 15min/time 08h-18h
# schedule.every().day.at("09:04").do(schedule_corporate_action_40minutes)  # 40min/time 09h-18h
# schedule.every().day.at("09:08").do(schedule_company_one_hours)  # 1h/time 09h-18h
# #
# schedule.every().day.at("21:36").do(schedule_ratio_two_times)  # Call 2 time 18h, 21h
# schedule.every().day.at("20:05").do(schedule_market_three_times)  # Call 3 time 15h30, 17h30, 19h30
#
# print("Started service !")
#
# total_interrupt = 0
# while True:
#     schedule.run_pending()
#     try:
#         time.sleep(1)
#     except KeyboardInterrupt:
#         total_interrupt += 1
#         print(f"KeyboardInterrupt: {total_interrupt}")


# # /Financial: 10min/time 09h-18h 02
# scheduler.add_job(import_financial_datas_min, 'cron', day_of_week='mon-sun', hour='09-17', minute='5-45/20', start_date='2022-08-22 09:00:00', end_date='2023-08-22 08:00:00', timezone='Asia/Ho_Chi_Minh', replace_existing=True)
# # /Message: 15min/time 08h-18h 01
# scheduler.add_job(import_message_datas, 'cron', day_of_week='mon-sun', hour='08-17', minute='1-31/30', start_date='2022-08-22 08:00:00', end_date='2023-08-22 08:00:00', timezone='Asia/Ho_Chi_Minh', replace_existing=True)
# # /CorporateAction: 40min/time 09h-18h 06
# scheduler.add_job(import_corporate_action_datas, 'cron', day_of_week='mon-sun', hour='09-17', minute=10, start_date='2022-08-22 09:00:00', end_date='2023-08-22 08:00:00', timezone='Asia/Ho_Chi_Minh', replace_existing=True)
# # /Company: 1h/time 09h-18h 02
# scheduler.add_job(import_company_datas, 'cron', day_of_week='mon-sun', hour='09-17', minute=20, start_date='2022-08-22 09:00:00', end_date='2023-08-22 08:00:00', timezone='Asia/Ho_Chi_Minh', replace_existing=True)
# # /Ratio: Call 2 time 18h, 21h 05
# scheduler.add_job(import_ratio_datas, 'cron', day_of_week='mon-sun', hour='18-21', minute=12, start_date='2022-08-22 08:00:00', end_date='2023-08-22 08:00:00', timezone='Asia/Ho_Chi_Minh', replace_existing=True)
# # /Market: Call 3 time 15h30, 17h30, 19h30 05
# scheduler.add_job(import_market_datas, 'cron', day_of_week='mon-sun', hour='15-19', minute=35, start_date='2022-08-22 08:00:00', end_date='2023-08-22 08:00:00', timezone='Asia/Ho_Chi_Minh', replace_existing=True)
# # Start the scheduler
# scheduler.start()

# /Financial: 10min/time 09h-18h 02
scheduler.add_job(import_financial_datas_min, 'cron', day_of_week='mon-sun', hour='09-18', minute='1-31/30', start_date='2022-09-06 09:00:00', end_date='2023-09-06 08:00:00', timezone='Asia/Ho_Chi_Minh', replace_existing=True)
# /Message: 15min/time 08h-18h 01
scheduler.add_job(import_message_datas, 'cron', day_of_week='mon-sun', hour='08-18', minute=6, start_date='2022-09-06 08:00:00', end_date='2023-09-06 08:00:00', timezone='Asia/Ho_Chi_Minh', replace_existing=True)
# /CorporateAction: 40min/time 09h-18h 06
scheduler.add_job(import_corporate_action_datas, 'cron', day_of_week='mon-sun', hour='09-18', minute=10, start_date='2022-09-06 09:00:00', end_date='2023-09-06 08:00:00', timezone='Asia/Ho_Chi_Minh', replace_existing=True)
# /Company: 1h/time 09h-18h 02
scheduler.add_job(import_company_datas, 'cron', day_of_week='mon-sun', hour='09-18', minute=20, start_date='2022-09-06 09:00:00', end_date='2023-09-06 08:00:00', timezone='Asia/Ho_Chi_Minh', replace_existing=True)
# /Ratio: Call 2 time 18h, 21h 05
scheduler.add_job(import_ratio_datas, 'cron', day_of_week='mon-sun', hour='20-23', minute=12, start_date='2022-09-06 08:00:00', end_date='2023-09-06 08:00:00', timezone='Asia/Ho_Chi_Minh', replace_existing=True)
# /Market: Call 3 time 15h30, 17h30, 19h30 05
scheduler.add_job(import_market_datas, 'cron', day_of_week='mon-sun', hour='15-20', minute=35, start_date='2022-09-06 08:00:00', end_date='2023-09-06 08:00:00', timezone='Asia/Ho_Chi_Minh', replace_existing=True)
# Start the scheduler
scheduler.start()
