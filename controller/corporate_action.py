import service
import time
import database
import telegram
from datetime import datetime

BASE_URL = "http://df31.fiintek.com"
DATE_TYPE = "updatedate"


def get_cash_dividend_payout(date_request):
    # Init parameter
    if not date_request:  # Nếu không truyền ngày thì mặc định lấy ngày hiện tại
        date_request = datetime.now().strftime("%Y-%m-%d")
    page_index = 1
    page_size = 100
    url = BASE_URL + "/CorporateAction/GetCashDividendPayout?pageIndex=" + str(page_index) \
          + "&pageSize=" + str(page_size) \
          + "&DateType="+DATE_TYPE+"&fromDate=" + date_request + "&toDate=" + date_request
    print("[get_cash_dividend_payout] get date >> ", url)
    response = service.make_request(url)
    if response is None:
        return

    data = response["Data"]
    paging = response["Paging"]

    while paging.get("NextPage") is not None:
        time.sleep(10)
        print("[get_cash_dividend_payout] NextPage : %s in TotalPage : %s", paging.get("NextPage"),
              paging.get("TotalPage"))
        url = BASE_URL + "/CorporateAction/GetCashDividendPayout?pageIndex=" + str(paging.get("NextPage")) \
              + "&pageSize=" + str(page_size) \
              + "&DateType="+DATE_TYPE+"&fromDate=" + date_request + "&toDate=" + date_request
        print("[get_cash_dividend_payout] Next page found >> ", url)
        response = service.make_request(url)
        data = data + response["Data"]
        paging = response["Paging"]

        print("[get_cash_dividend_payout] Data size: ", len(data))

    database.execute_many('''INSERT INTO fiin_dtf.stx_cpa_cashdividendpayout (CashDividendPayoutId, OrganCode, 
    PublicDate, RecordDate, ExrightDate, PayoutDate, ValuePershare, ExerciseRate, DividendYear, 
    DividendStageCode, PlaceForDeposited, PlaceForUnDeposited, Note, SourceUrl, en_Note, en_SourceUrl, 
    en_PlaceForDeposited, en_PlaceForUnDeposited, Status, CreateDate, UpdateDate) VALUES (%s, %s, %s, %s, %s, %s, 
    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT ON CONSTRAINT 
    xpkstx_cpa_cashdividendpayout DO UPDATE SET (CashDividendPayoutId, OrganCode, PublicDate, RecordDate, 
    ExrightDate, PayoutDate, ValuePershare, ExerciseRate, DividendYear, DividendStageCode, PlaceForDeposited, 
    PlaceForUnDeposited, Note, SourceUrl, en_Note, en_SourceUrl, en_PlaceForDeposited, en_PlaceForUnDeposited, 
    Status, CreateDate, UpdateDate) = (EXCLUDED.CashDividendPayoutId, EXCLUDED.OrganCode, EXCLUDED.PublicDate, 
    EXCLUDED. RecordDate, EXCLUDED.ExrightDate, EXCLUDED.PayoutDate, EXCLUDED.ValuePershare, 
    EXCLUDED.ExerciseRate, EXCLUDED.DividendYear, EXCLUDED.DividendStageCode, EXCLUDED. PlaceForDeposited, 
    EXCLUDED.PlaceForUnDeposited, EXCLUDED.Note, EXCLUDED.SourceUrl, EXCLUDED.en_Note, EXCLUDED.en_SourceUrl, 
    EXCLUDED.en_PlaceForDeposited, EXCLUDED. en_PlaceForUnDeposited, EXCLUDED.Status, EXCLUDED.CreateDate, 
    EXCLUDED.UpdateDate)''',
                          [[item.get("CashDividendPayoutId"), item.get("OrganCode"), item.get("PublicDate"),
                            item.get("RecordDate"),
                            item.get("ExrightDate"),
                            item.get("PayoutDate"), item.get("ValuePershare"), item.get("ExerciseRate"),
                            item.get("DividendYear"),
                            item.get("DividendStageCode"),
                            item.get("PlaceForDeposited"), item.get("PlaceForUnDeposited"), item.get("Note"),
                            item.get("SourceUrl"),
                            item.get("en_Note"),
                            item.get("en_SourceUrl"), item.get("en_PlaceForDeposited"),
                            item.get("en_PlaceForUnDeposited"),
                            item.get("Status"),
                            item.get("CreateDate"), item.get("UpdateDate")] for item in data])
    print("Done insert data table stx_cpa_cashdividendpayout / CorporateAction",
          datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    telegram.send(mess=f"Done insert data table stx_cpa_cashdividendpayout / CorporateAction / Data size: {len(data)}")


def get_cash_dividend_plan(date_request):
    # Init parameter
    if not date_request:  # Nếu không truyền ngày thì mặc định lấy ngày hiện tại
        date_request = datetime.now().strftime("%Y-%m-%d")
    page_index = 1
    page_size = 100
    url = BASE_URL + "/CorporateAction/GetCashDividendPlan?pageIndex=" + str(page_index) \
          + "&pageSize=" + str(page_size) \
          + "&DateType="+DATE_TYPE+"&fromDate=" + date_request + "&toDate=" + date_request
    print("[get_cash_dividend_plan] get date >> ", url)
    response = service.make_request(url)
    if response is None:
        return

    data = response["Data"]
    paging = response["Paging"]

    while paging.get("NextPage") is not None:
        time.sleep(10)
        print("[get_cash_dividend_plan] NextPage : %s in TotalPage : %s", paging.get("NextPage"),
              paging.get("TotalPage"))
        url = BASE_URL + "/CorporateAction/GetCashDividendPlan?pageIndex=" + str(paging.get("NextPage")) \
              + "&pageSize=" + str(page_size) \
              + "&DateType="+DATE_TYPE+"&fromDate=" + date_request + "&toDate=" + date_request
        print("[get_cash_dividend_plan] Next page found >> ", url)
        response = service.make_request(url)
        data = data + response["Data"]
        paging = response["Paging"]

        print("[get_cash_dividend_plan] Data size: ", len(data))

    database.execute_many('''INSERT INTO fiin_dtf.stx_cpa_cashdividendplan (CashDividendPlanId, OrganCode, PublicDate, 
    ValuePerSharePlan, TotalValuePlan, ValuePerShareApprove, TotalValueApprove, CurrencyCode, ExerciseRatePlan, 
    ExerciseRateApprove, DividendYear, SourceName, SourceApprove, Note, SourceUrl, en_SourceName, 
    en_SourceApprove, en_Note, en_SourceUrl, Status, CreateDate, UpdateDate, ValuePerShareNotApprove, 
    TotalValueNotApprove, ExerciseRateNotApprove) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT ON CONSTRAINT xpkstx_cpa_cashdividendplan DO UPDATE 
    SET (CashDividendPlanId, OrganCode, PublicDate, ValuePerSharePlan, TotalValuePlan, ValuePerShareApprove, 
    TotalValueApprove, CurrencyCode, ExerciseRatePlan, ExerciseRateApprove, DividendYear, SourceName, 
    SourceApprove, Note, SourceUrl, en_SourceName, en_SourceApprove, en_Note, en_SourceUrl, Status, CreateDate, 
    UpdateDate, ValuePerShareNotApprove, TotalValueNotApprove, ExerciseRateNotApprove) = (
    EXCLUDED.CashDividendPlanId, EXCLUDED.OrganCode, EXCLUDED.PublicDate, EXCLUDED. ValuePerSharePlan, 
    EXCLUDED.TotalValuePlan, EXCLUDED.ValuePerShareApprove, EXCLUDED.TotalValueApprove, EXCLUDED.CurrencyCode, 
    EXCLUDED.ExerciseRatePlan, EXCLUDED. ExerciseRateApprove, EXCLUDED.DividendYear, EXCLUDED.SourceName, 
    EXCLUDED.SourceApprove, EXCLUDED.Note, EXCLUDED.SourceUrl, EXCLUDED.en_SourceName, EXCLUDED. 
    en_SourceApprove, EXCLUDED.en_Note, EXCLUDED.en_SourceUrl, EXCLUDED.Status, EXCLUDED.CreateDate, 
    EXCLUDED.UpdateDate, EXCLUDED.ValuePerShareNotApprove, EXCLUDED. TotalValueNotApprove, 
    EXCLUDED.ExerciseRateNotApprove)''',
                          [[item.get("CashDividendPlanId"), item.get("OrganCode"), item.get("PublicDate"),
                            item.get("ValuePerSharePlan"),
                            item.get("TotalValuePlan"),
                            item.get("ValuePerShareApprove"), item.get("TotalValueApprove"), item.get("CurrencyCode"),
                            item.get("ExerciseRatePlan"), item.get("ExerciseRateApprove"),
                            item.get("DividendYear"), item.get("SourceName"), item.get("SourceApprove"),
                            item.get("Note"), item.get("SourceUrl"),
                            item.get("en_SourceName"), item.get("en_SourceApprove"), item.get("en_Note"),
                            item.get("en_SourceUrl"),
                            item.get("Status"),
                            item.get("CreateDate"), item.get("UpdateDate"), item.get("ValuePerShareNotApprove"),
                            item.get("TotalValueNotApprove"), item.get("ExerciseRateNotApprove")] for item in data])
    print("Done insert data table stx_cpa_cashdividendplan / CorporateAction",
          datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    telegram.send(mess=f"Done insert data table stx_cpa_cashdividendplan / CorporateAction / Data size: {len(data)}")


def get_corporate_deal(date_request):
    # Init parameter
    if not date_request:  # Nếu không truyền ngày thì mặc định lấy ngày hiện tại
        date_request = datetime.now().strftime("%Y-%m-%d")
    page_index = 1
    page_size = 100
    url = BASE_URL + "/CorporateAction/GetCorporateDeal?pageIndex=" + str(page_index) \
          + "&pageSize=" + str(page_size) \
          + "&DateType="+DATE_TYPE+"&fromDate=" + date_request + "&toDate=" + date_request
    print("[get_corporate_deal] get date >> ", url)
    response = service.make_request(url)
    if response is None:
        return

    data = response["Data"]
    paging = response["Paging"]

    while paging.get("NextPage") is not None:
        time.sleep(10)
        print("[get_corporate_deal] NextPage : %s in TotalPage : %s", paging.get("NextPage"),
              paging.get("TotalPage"))
        url = BASE_URL + "/CorporateAction/GetCorporateDeal?pageIndex=" + str(paging.get("NextPage")) \
              + "&pageSize=" + str(page_size) \
              + "&DateType="+DATE_TYPE+"&fromDate=" + date_request + "&toDate=" + date_request
        print("[get_corporate_deal] Next page found >> ", url)
        response = service.make_request(url)
        data = data + response["Data"]
        paging = response["Paging"]

        print("[get_corporate_deal] Data size: ", len(data))

    database.execute_many('''INSERT INTO fiin_dtf.stx_cpa_corporatedeal (CorporateDealId, OrganCode, TradeTypeCode, 
    DealTypeCode, ActionTypeCode, TradeStatusCode, TraderOrganCode, PublicDate, PersonId, ShareBeforeTrade, 
    OwnershipBeforeTrade, ShareRegister, ShareAcquire, PriceAcquire, ValueAcquire, ShareAdded, ShareAfterTrade, 
    OwnershipAfterTrade, StartDate, EndDate, Goal, SourceUrl, Note, en_Goal, en_SourceUrl, en_Note, Status, 
    CreateDate, UpdateDate) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT ON CONSTRAINT xpkstx_cpa_corporatedeal DO UPDATE SET (
    CorporateDealId, OrganCode, TradeTypeCode, DealTypeCode, ActionTypeCode, TradeStatusCode, TraderOrganCode, 
    PublicDate, PersonId, ShareBeforeTrade, OwnershipBeforeTrade, ShareRegister, ShareAcquire, PriceAcquire, 
    ValueAcquire, ShareAdded, ShareAfterTrade, OwnershipAfterTrade, StartDate, EndDate, Goal, SourceUrl, Note, 
    en_Goal, en_SourceUrl, en_Note, Status, CreateDate, UpdateDate) = (EXCLUDED.CorporateDealId, 
    EXCLUDED.OrganCode, EXCLUDED.TradeTypeCode, EXCLUDED. DealTypeCode, EXCLUDED.ActionTypeCode, 
    EXCLUDED.TradeStatusCode, EXCLUDED.TraderOrganCode, EXCLUDED.PublicDate, EXCLUDED.PersonId, EXCLUDED. 
    ShareBeforeTrade, EXCLUDED.OwnershipBeforeTrade, EXCLUDED.ShareRegister, EXCLUDED.ShareAcquire, 
    EXCLUDED.PriceAcquire, EXCLUDED.ValueAcquire, EXCLUDED.ShareAdded, EXCLUDED. ShareAfterTrade, 
    EXCLUDED.OwnershipAfterTrade, EXCLUDED.StartDate, EXCLUDED.EndDate, EXCLUDED.Goal, EXCLUDED.SourceUrl, 
    EXCLUDED.Note, EXCLUDED.en_Goal, EXCLUDED.en_SourceUrl, EXCLUDED. en_Note, EXCLUDED.Status, 
    EXCLUDED.CreateDate, EXCLUDED.UpdateDate)''',
                          [[item.get("CorporateDealId"), item.get("OrganCode"), item.get("TradeTypeCode"),
                            item.get("DealTypeCode"),
                            item.get("ActionTypeCode"),
                            item.get("TradeStatusCode"), item.get("TraderOrganCode"), item.get("PublicDate"),
                            item.get("PersonId"),
                            item.get("ShareBeforeTrade"), item.get("OwnershipBeforeTrade"), item.get("ShareRegister"),
                            item.get("ShareAcquire"),
                            item.get("PriceAcquire"),
                            item.get("ValueAcquire"), item.get("ShareAdded"), item.get("ShareAfterTrade"),
                            item.get("OwnershipAfterTrade"),
                            item.get("StartDate"), item.get("EndDate"),
                            item.get("Goal"), item.get("SourceUrl"), item.get("Note"), item.get("en_Goal"),
                            item.get("en_SourceUrl"),
                            item.get("en_Note"), item.get("Status"), item.get("CreateDate"), item.get("UpdateDate")]
                           for item in data])
    print("Done insert data table stx_cpa_corporatedeal / CorporateAction",
          datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    telegram.send(mess=f"Done insert data table stx_cpa_corporatedeal / CorporateAction / Data size: {len(data)}")


def get_person_deal(date_request):
    # Init parameter
    if not date_request:  # Nếu không truyền ngày thì mặc định lấy ngày hiện tại
        date_request = datetime.now().strftime("%Y-%m-%d")
    page_index = 1
    page_size = 100
    url = BASE_URL + "/CorporateAction/GetPersonDeal?pageIndex=" + str(page_index) \
          + "&pageSize=" + str(page_size) \
          + "&DateType="+DATE_TYPE+"&fromDate=" + date_request + "&toDate=" + date_request
    print("[get_person_deal] get date >> ", url)
    response = service.make_request(url)
    if response is None:
        return

    data = response["Data"]
    paging = response["Paging"]

    while paging.get("NextPage") is not None:
        time.sleep(10)
        print("[get_person_deal] NextPage : %s in TotalPage : %s", paging.get("NextPage"),
              paging.get("TotalPage"))
        url = BASE_URL + "/CorporateAction/GetPersonDeal?pageIndex=" + str(paging.get("NextPage")) \
              + "&pageSize=" + str(page_size) \
              + "&DateType="+DATE_TYPE+"&fromDate=" + date_request + "&toDate=" + date_request
        print("[get_person_deal] Next page found >> ", url)
        response = service.make_request(url)
        data = data + response["Data"]
        paging = response["Paging"]

        print("[get_person_deal] Data size: ", len(data))

    database.execute_many('''INSERT INTO fiin_dtf.stx_cpa_persondeal (PersonDealId, OrganCode, TradeTypeCode, 
    DealTypeCode, ActionTypeCode, TradeStatusCode, TraderPersonId, RoleId, RolePersonId, PublicDate, 
    ShareBeforeTrade, OwnershipBeforeTrade, ShareRegister, ShareAcquire, PriceAcquire, ValueAcquire, ShareAdded, 
    ShareAfterTrade, OwnershipAfterTrade, StartDate, EndDate, Goal, SourceUrl, Note, en_Goal, en_SourceUrl, 
    en_Note, Status, CreateDate, UpdateDate) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT ON CONSTRAINT xpkstx_cpa_persondeal 
    DO UPDATE SET (PersonDealId, OrganCode, TradeTypeCode, DealTypeCode, ActionTypeCode, TradeStatusCode, 
    TraderPersonId, RoleId, RolePersonId, PublicDate, ShareBeforeTrade, OwnershipBeforeTrade, ShareRegister, 
    ShareAcquire, PriceAcquire, ValueAcquire, ShareAdded, ShareAfterTrade, OwnershipAfterTrade, StartDate, 
    EndDate, Goal, SourceUrl, Note, en_Goal, en_SourceUrl, en_Note, Status, CreateDate, UpdateDate) = (
    EXCLUDED.PersonDealId, EXCLUDED.OrganCode, EXCLUDED.TradeTypeCode, EXCLUDED. DealTypeCode, 
    EXCLUDED.ActionTypeCode, EXCLUDED.TradeStatusCode, EXCLUDED.TraderPersonId, EXCLUDED.RoleId, 
    EXCLUDED.RolePersonId, EXCLUDED.PublicDate, EXCLUDED. ShareBeforeTrade, EXCLUDED.OwnershipBeforeTrade, 
    EXCLUDED.ShareRegister, EXCLUDED.ShareAcquire, EXCLUDED.PriceAcquire, EXCLUDED.ValueAcquire, 
    EXCLUDED.ShareAdded, EXCLUDED. ShareAfterTrade, EXCLUDED.OwnershipAfterTrade, EXCLUDED.StartDate, 
    EXCLUDED.EndDate, EXCLUDED.Goal, EXCLUDED.SourceUrl, EXCLUDED.Note, EXCLUDED.en_Goal, EXCLUDED.en_SourceUrl, 
    EXCLUDED. en_Note, EXCLUDED.Status, EXCLUDED.CreateDate, EXCLUDED.UpdateDate)''',
                          [[item.get("PersonDealId"), item.get("OrganCode"), item.get("TradeTypeCode"),
                            item.get("DealTypeCode"),
                            item.get("ActionTypeCode"),
                            item.get("TradeStatusCode"), item.get("TraderPersonId"), item.get("RoleId"),
                            item.get("RolePersonId"),
                            item.get("PublicDate"),
                            item.get("ShareBeforeTrade"), item.get("OwnershipBeforeTrade"), item.get("ShareRegister"),
                            item.get("ShareAcquire"),
                            item.get("PriceAcquire"),
                            item.get("ValueAcquire"), item.get("ShareAdded"), item.get("ShareAfterTrade"),
                            item.get("OwnershipAfterTrade"),
                            item.get("StartDate"), item.get("EndDate"),
                            item.get("Goal"), item.get("SourceUrl"), item.get("Note"), item.get("en_Goal"),
                            item.get("en_SourceUrl"),
                            item.get("en_Note"), item.get("Status"), item.get("CreateDate"), item.get("UpdateDate")]
                           for item in data])
    print("Done insert data table stx_cpa_persondeal / CorporateAction", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    telegram.send(mess=f"Done insert data table stx_cpa_persondeal / CorporateAction / Data size: {len(data)}")


def get_share_issue(date_request):
    # Init parameter
    if not date_request:  # Nếu không truyền ngày thì mặc định lấy ngày hiện tại
        date_request = datetime.now().strftime("%Y-%m-%d")
    page_index = 1
    page_size = 100
    url = BASE_URL + "/CorporateAction/GetShareIssue?pageIndex=" + str(page_index) \
          + "&pageSize=" + str(page_size) \
          + "&DateType="+DATE_TYPE+"&fromDate=" + date_request + "&toDate=" + date_request
    print("[get_share_issue] get date >> ", url)
    response = service.make_request(url)
    if response is None:
        return

    data = response["Data"]
    paging = response["Paging"]

    while paging.get("NextPage") is not None:
        time.sleep(10)
        print("[get_share_issue] NextPage : %s in TotalPage : %s", paging.get("NextPage"),
              paging.get("TotalPage"))
        url = BASE_URL + "/CorporateAction/GetShareIssue?pageIndex=" + str(paging.get("NextPage")) \
              + "&pageSize=" + str(page_size) \
              + "&DateType="+DATE_TYPE+"&fromDate=" + date_request + "&toDate=" + date_request
        print("[get_share_issue] Next page found >> ", url)
        response = service.make_request(url)
        data = data + response["Data"]
        paging = response["Paging"]

        print("[get_share_issue] Data size: ", len(data))

    database.execute_many('''INSERT INTO fiin_dtf.stx_cpa_shareissue (ShareIssueId, OrganCode, PublicDate, 
    IssueMethodCode, ShareTypeCode, ExerciseRatio, ExerciseRatioOwn, ExerciseRatioEarn, HandingOddShare, 
    PlaceForDeposited, PlaceForUnDeposited, PlanVolumn, IssueVolumn, IssueStatusCode, IssuePrice, TotalValue, 
    IssueYear, RecordDate, ExrightDate, IssueDate, EffectiveDate, ListingDate, Dividend_Year, Dividend_StageCode, 
    IsTransfer, NonTransferablePeriod, NonTransferablePeriodUnitCode, NonTransferablePeriodFrom, 
    NonTransferablePeriodTo, Goal, Note, FinancialResource, SourceUrl, en_Goal, en_Note, en_FinancialResource, 
    en_HandingOddShare, en_PlaceForDeposited, en_PlaceForUnDeposited, en_SourceUrl, Status, CreateDate, 
    UpdateDate, NewTicker, SubscriptionPeriodStart, SubscriptionPeriodEnd, RightsTransferingPeriodStart, 
    RightsTransferingPeriodEnd, DebitDate, RightCode, RightIsinCode, HandingOddSharePrice, ExecutionRateShare, 
    ExecutionRateRight) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
    %s, %s, %s, %s, %s, %s, %s) ON CONFLICT ON CONSTRAINT xpkstx_cpa_shareissue DO UPDATE SET (ShareIssueId, 
    OrganCode, PublicDate, IssueMethodCode, ShareTypeCode, ExerciseRatio, ExerciseRatioOwn, ExerciseRatioEarn, 
    HandingOddShare, PlaceForDeposited, PlaceForUnDeposited, PlanVolumn, IssueVolumn, IssueStatusCode, 
    IssuePrice, TotalValue, IssueYear, RecordDate, ExrightDate, IssueDate, EffectiveDate, ListingDate, 
    Dividend_Year, Dividend_StageCode, IsTransfer, NonTransferablePeriod, NonTransferablePeriodUnitCode, 
    NonTransferablePeriodFrom, NonTransferablePeriodTo, Goal, Note, FinancialResource, SourceUrl, en_Goal, 
    en_Note, en_FinancialResource, en_HandingOddShare, en_PlaceForDeposited, en_PlaceForUnDeposited, 
    en_SourceUrl, Status, CreateDate, UpdateDate, NewTicker, SubscriptionPeriodStart, SubscriptionPeriodEnd, 
    RightsTransferingPeriodStart, RightsTransferingPeriodEnd, DebitDate, RightCode, RightIsinCode, 
    HandingOddSharePrice, ExecutionRateShare, ExecutionRateRight) = (EXCLUDED.ShareIssueId, EXCLUDED.OrganCode, 
    EXCLUDED.PublicDate, EXCLUDED.IssueMethodCode, EXCLUDED.ShareTypeCode, EXCLUDED.ExerciseRatio, 
    EXCLUDED.ExerciseRatioOwn, EXCLUDED.ExerciseRatioEarn, EXCLUDED.HandingOddShare, EXCLUDED.PlaceForDeposited, 
    EXCLUDED.PlaceForUnDeposited, EXCLUDED.PlanVolumn, EXCLUDED.IssueVolumn, EXCLUDED.IssueStatusCode, 
    EXCLUDED.IssuePrice, EXCLUDED.TotalValue, EXCLUDED.IssueYear, EXCLUDED.RecordDate, EXCLUDED.ExrightDate, 
    EXCLUDED.IssueDate, EXCLUDED.EffectiveDate, EXCLUDED.ListingDate, EXCLUDED.Dividend_Year, 
    EXCLUDED.Dividend_StageCode, EXCLUDED.IsTransfer, EXCLUDED.NonTransferablePeriod, 
    EXCLUDED.NonTransferablePeriodUnitCode, EXCLUDED.NonTransferablePeriodFrom, EXCLUDED.NonTransferablePeriodTo, 
    EXCLUDED.Goal, EXCLUDED.Note, EXCLUDED.FinancialResource, EXCLUDED.SourceUrl, EXCLUDED.en_Goal, EXCLUDED.en_Note, 
    EXCLUDED.en_FinancialResource, EXCLUDED.en_HandingOddShare, EXCLUDED.en_PlaceForDeposited, 
    EXCLUDED.en_PlaceForUnDeposited, EXCLUDED.en_SourceUrl, EXCLUDED.Status, EXCLUDED.CreateDate, 
    EXCLUDED.UpdateDate, EXCLUDED.NewTicker, EXCLUDED.SubscriptionPeriodStart, EXCLUDED.SubscriptionPeriodEnd, 
    EXCLUDED.RightsTransferingPeriodStart, EXCLUDED.RightsTransferingPeriodEnd, EXCLUDED.DebitDate, 
    EXCLUDED.RightCode, EXCLUDED.RightIsinCode, EXCLUDED.HandingOddSharePrice, EXCLUDED.ExecutionRateShare, 
    EXCLUDED.ExecutionRateRight)''',
                          [[item.get("ShareIssueId"), item.get("OrganCode"), item.get("PublicDate"),
                            item.get("IssueMethodCode"),
                            item.get("ShareTypeCode"),
                            item.get("ExerciseRatio"), item.get("ExerciseRatioOwn"), item.get("ExerciseRatioEarn"),
                            item.get("HandingOddShare"),
                            item.get("PlaceForDeposited"),
                            item.get("PlaceForUnDeposited"), item.get("PlanVolumn"), item.get("IssueVolumn"),
                            item.get("IssueStatusCode"),
                            item.get("IssuePrice"),
                            item.get("TotalValue"), item.get("IssueYear"), item.get("RecordDate"),
                            item.get("ExrightDate"),
                            item.get("IssueDate"),
                            item.get("EffectiveDate"), item.get("ListingDate"), item.get("Dividend_Year"),
                            item.get("Dividend_StageCode"),
                            item.get("IsTransfer"),
                            item.get("NonTransferablePeriod"), item.get("NonTransferablePeriodUnitCode"),
                            item.get("NonTransferablePeriodFrom"), item.get("NonTransferablePeriodTo"),
                            item.get("Goal"),
                            item.get("Note"), item.get("FinancialResource"), item.get("SourceUrl"), item.get("en_Goal"),
                            item.get("en_Note"),
                            item.get("en_FinancialResource"), item.get("en_HandingOddShare"),
                            item.get("en_PlaceForDeposited"),
                            item.get("en_PlaceForUnDeposited"), item.get("en_SourceUrl"),
                            item.get("Status"), item.get("CreateDate"), item.get("UpdateDate"), item.get("NewTicker"),
                            item.get("SubscriptionPeriodStart"),
                            item.get("SubscriptionPeriodEnd"), item.get("RightsTransferingPeriodStart"),
                            item.get("RightsTransferingPeriodEnd"), item.get("DebitDate"), item.get("RightCode"),
                            item.get("RightIsinCode"), item.get("HandingOddSharePrice"), item.get("ExecutionRateShare"),
                            item.get("ExecutionRateRight")] for item in data])
    print("Done insert data table stx_cpa_shareissue / CorporateAction", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    telegram.send(mess=f"Done insert data table stx_cpa_shareissue / CorporateAction / Data size: {len(data)}")


def get_stock_dividend_plan(date_request):
    # Init parameter
    if not date_request:  # Nếu không truyền ngày thì mặc định lấy ngày hiện tại
        date_request = datetime.now().strftime("%Y-%m-%d")
    page_index = 1
    page_size = 100
    url = BASE_URL + "/CorporateAction/GetStockDividendPlan?pageIndex=" + str(page_index) \
          + "&pageSize=" + str(page_size) \
          + "&DateType="+DATE_TYPE+"&fromDate=" + date_request + "&toDate=" + date_request
    print("[get_stock_dividend_plan] get date >> ", url)
    response = service.make_request(url)
    if response is None:
        return

    data = response["Data"]
    paging = response["Paging"]

    while paging.get("NextPage") is not None:
        time.sleep(10)
        print("[get_stock_dividend_plan] NextPage : %s in TotalPage : %s", paging.get("NextPage"),
              paging.get("TotalPage"))
        url = BASE_URL + "/CorporateAction/GetStockDividendPlan?pageIndex=" + str(paging.get("NextPage")) \
              + "&pageSize=" + str(page_size) \
              + "&DateType="+DATE_TYPE+"&fromDate=" + date_request + "&toDate=" + date_request
        print("[get_stock_dividend_plan] Next page found >> ", url)
        response = service.make_request(url)
        data = data + response["Data"]
        paging = response["Paging"]

        print("[get_stock_dividend_plan] Data size: ", len(data))

    database.execute_many('''INSERT INTO fiin_dtf.stx_cpa_stockdividendplan (StockDividendPlanId, OrganCode, PublicDate, 
    ExerciseRatio, ExerciseRatioOwn, ExerciseRatioEarn, PlanVolumn, StageCode, DividendYear, IsTransfer, 
    NonTransferablePeriod, NonTransferablePeriodUnitCode, NonTransferablePeriodFrom, NonTransferablePeriodTo, 
    Goal, Note, FinancialResource, SourceName, SourceUrl, en_Goal, en_Note, en_FinancialResource, en_SourceName, 
    en_SourceUrl, Status, CreateDate, UpdateDate) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT ON CONSTRAINT xpkstx_cpa_stockdividendplan DO 
    UPDATE SET (StockDividendPlanId, OrganCode, PublicDate, ExerciseRatio, ExerciseRatioOwn, ExerciseRatioEarn, 
    PlanVolumn, StageCode, DividendYear, IsTransfer, NonTransferablePeriod, NonTransferablePeriodUnitCode, 
    NonTransferablePeriodFrom, NonTransferablePeriodTo, Goal, Note, FinancialResource, SourceName, SourceUrl, 
    en_Goal, en_Note, en_FinancialResource, en_SourceName, en_SourceUrl, Status, CreateDate, UpdateDate) = (
    EXCLUDED.StockDividendPlanId, EXCLUDED.OrganCode, EXCLUDED.PublicDate, EXCLUDED.ExerciseRatio, 
    EXCLUDED.ExerciseRatioOwn, EXCLUDED.ExerciseRatioEarn, EXCLUDED.PlanVolumn, EXCLUDED.StageCode, 
    EXCLUDED.DividendYear, EXCLUDED.IsTransfer, EXCLUDED.NonTransferablePeriod, 
    EXCLUDED.NonTransferablePeriodUnitCode, EXCLUDED.NonTransferablePeriodFrom, EXCLUDED.NonTransferablePeriodTo, 
    EXCLUDED.Goal, EXCLUDED.Note, EXCLUDED.FinancialResource, EXCLUDED.SourceName, EXCLUDED.SourceUrl, 
    EXCLUDED.en_Goal, EXCLUDED.en_Note, EXCLUDED.en_FinancialResource, EXCLUDED.en_SourceName, EXCLUDED.en_SourceUrl, 
    EXCLUDED.Status, EXCLUDED.CreateDate, EXCLUDED.UpdateDate)''',
                          [[item.get("StockDividendPlanId"), item.get("OrganCode"), item.get("PublicDate"),
                            item.get("ExerciseRatio"),
                            item.get("ExerciseRatioOwn"),
                            item.get("ExerciseRatioEarn"), item.get("PlanVolumn"), item.get("StageCode"),
                            item.get("DividendYear"),
                            item.get("IsTransfer"),
                            item.get("NonTransferablePeriod"), item.get("NonTransferablePeriodUnitCode"),
                            item.get("NonTransferablePeriodFrom"), item.get("NonTransferablePeriodTo"),
                            item.get("Goal"),
                            item.get("Note"), item.get("FinancialResource"), item.get("SourceName"),
                            item.get("SourceUrl"), item.get("en_Goal"),
                            item.get("en_Note"), item.get("en_FinancialResource"), item.get("en_SourceName"),
                            item.get("en_SourceUrl"),
                            item.get("Status"),
                            item.get("CreateDate"), item.get("UpdateDate")] for item in data])
    print("Done insert data table stx_cpa_stockdividendplan / CorporateAction",
          datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    telegram.send(mess=f"Done insert data table stx_cpa_stockdividendplan / CorporateAction / Data size: {len(data)}")
