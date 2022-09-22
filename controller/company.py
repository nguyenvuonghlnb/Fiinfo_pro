import time
import service
import database
import telegram
from datetime import datetime

BASE_URL = "http://df31.fiintek.com"
DATE_TYPE = "updatedate"


def get_organization(date_request):
    # Init parameter
    if not date_request:  # Nếu không truyền ngày thì mặc định lấy ngày hiện tại
        date_request = datetime.now().strftime("%Y-%m-%d")
    page_index = 1
    page_size = 100
    url = BASE_URL + "/Company/GetOrganization?pageIndex=" + str(page_index) \
          + "&pageSize=" + str(page_size) \
          + "&DateType="+DATE_TYPE+"&fromDate=" + date_request + "&toDate=" + date_request
    print("[Get_Organization] get date >> ", url)
    response = service.make_request(url)
    if response is None:
        return

    data = response["Data"]
    paging = response["Paging"]

    while paging.get("NextPage") is not None:
        time.sleep(10)
        print("[Get_Organization] NextPage : %s in TotalPage : %s", paging.get("NextPage"), paging.get("TotalPage"))
        url = BASE_URL + "/Company/GetOrganization?pageIndex=" + str(paging.get("NextPage")) \
              + "&pageSize=" + str(page_size) \
              + "&DateType="+DATE_TYPE+"&fromDate=" + date_request + "&toDate=" + date_request
        print("[Get-Organization] Next page found >> ", url)
        response = service.make_request(url)
        data = data + response["Data"]
        paging = response["Paging"]

        print("[Get_Organization] Data size: ", len(data))
    database.execute_many('''INSERT INTO fiin_dtf.stx_cpf_organization (OrganCode, IsinCode, ComGroupCode, IcbCode,
    OrganTypeCode, ComTypeCode, BusTypeBeforeListCode, CountryLocationCode, SecurityOrganCode, MarginStatusCode,
    ControlStatusCode, Ticker, OrganName, OrganShortName, OrganFriendlyName, OutstandingShare, FreeFloat,
    FreeFloatRate, IssueShare, CharterCapital, ListingDate, FirstPrice, FirstVolumn, NumberOfShareHolder,
    ExDateShareHolder, en_OrganName, en_OrganShortName, en_OrganFriendlyName, Status, CreateDate, UpdateDate,
    AccountingPeriod) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT ON CONSTRAINT pk_stx_cpf_company_id DO UPDATE SET (
    OrganCode, IsinCode, ComGroupCode, IcbCode, OrganTypeCode, ComTypeCode, BusTypeBeforeListCode,
    CountryLocationCode, SecurityOrganCode, MarginStatusCode, ControlStatusCode, Ticker, OrganName,
    OrganShortName, OrganFriendlyName, OutstandingShare, FreeFloat, FreeFloatRate, IssueShare, CharterCapital,
    ListingDate, FirstPrice, FirstVolumn, NumberOfShareHolder, ExDateShareHolder, en_OrganName,
    en_OrganShortName, en_OrganFriendlyName, Status, CreateDate, UpdateDate, AccountingPeriod) = (
    EXCLUDED.OrganCode, EXCLUDED.IsinCode, EXCLUDED.ComGroupCode, EXCLUDED.IcbCode, EXCLUDED.OrganTypeCode,
    EXCLUDED.ComTypeCode, EXCLUDED.BusTypeBeforeListCode, EXCLUDED.CountryLocationCode,
    EXCLUDED.SecurityOrganCode, EXCLUDED.MarginStatusCode, EXCLUDED.ControlStatusCode, EXCLUDED.Ticker,
    EXCLUDED.OrganName, EXCLUDED.OrganShortName, EXCLUDED.OrganFriendlyName, EXCLUDED.OutstandingShare,
    EXCLUDED.FreeFloat, EXCLUDED.FreeFloatRate, EXCLUDED.IssueShare, EXCLUDED.CharterCapital,
    EXCLUDED.ListingDate, EXCLUDED.FirstPrice, EXCLUDED.FirstVolumn, EXCLUDED.NumberOfShareHolder,
    EXCLUDED.ExDateShareHolder, EXCLUDED.en_OrganName, EXCLUDED.en_OrganShortName, EXCLUDED.en_OrganFriendlyName,
    EXCLUDED.Status, EXCLUDED.CreateDate, EXCLUDED.UpdateDate, EXCLUDED.AccountingPeriod)''',
                          [[
                              item.get("OrganCode"), item.get("IsinCode"), item.get("ComGroupCode"),
                              item.get("IcbCode"),
                              item.get("OrganTypeCode"),
                              item.get("ComTypeCode"),
                              item.get("BusTypeBeforeListCode"), item.get("CountryLocationCode"),
                              item.get("SecurityOrganCode"),
                              item.get("MarginStatusCode"), item.get("ControlStatusCode"), item.get("Ticker"),
                              item.get("OrganName"), item.get("OrganShortName"), item.get("OrganFriendlyName"),
                              item.get("OutstandingShare"),
                              item.get("FreeFloat"), item.get("FreeFloatRate"),
                              item.get("IssueShare"), item.get("CharterCapital"), item.get("ListingDate"),
                              item.get("FirstPrice"),
                              item.get("FirstVolumn"), item.get("NumberOfShareHolder"),
                              item.get("ExDateShareHolder"), item.get("en_OrganName"), item.get("en_OrganShortName"),
                              item.get("en_OrganFriendlyName"), item.get("Status"), item.get("CreateDate"),
                              item.get("UpdateDate"), item.get("AccountingPeriod")] for item in data])
    print("Done insert data table stx_cpf_organization / Company", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    telegram.send(mess=f"Done insert data table stx_cpf_organization / Company / Data size: {len(data)}")


def get_company_information(date_request):
    # Init parameter
    if not date_request:  # Nếu không truyền ngày thì mặc định lấy ngày hiện tại
        date_request = datetime.now().strftime("%Y-%m-%d")
    page_index = 1
    page_size = 100
    url = BASE_URL + "/Company/GetCompanyInformation?pageIndex=" + str(page_index) \
          + "&pageSize=" + str(page_size) \
          + "&DateType="+DATE_TYPE+"&fromDate=" + date_request + "&toDate=" + date_request
    print("[Get_Company_Infomation] Request data >> ", url)
    response = service.make_request(url)
    if response is None:
        return
    data = response["Data"]
    paging = response["Paging"]

    while paging.get("NextPage") is not None:
        time.sleep(10)
        print("[Get_Company_Infomation] NextPage : %s in TotalPage : %s", paging.get("NextPage"),
              paging.get("TotalPage"))
        url = BASE_URL + "/Company/GetCompanyInformation?pageIndex=" + str(paging.get("NextPage")) \
              + "&pageSize=" + str(page_size) \
              + "&DateType="+DATE_TYPE+"&fromDate=" + date_request + "&toDate=" + date_request
        print("[Get_Company_Infomation] Request data >> ", url)
        response = service.make_request(url)
        data = data + response["Data"]
        paging = response["Paging"]

        print("[Get_Company_Infomation] Data size: ", len(data))

    database.execute_many('''INSERT INTO fiin_dtf.stx_cpf_companyinformation (OrganCode, REPPersonId, OwernerPersonId,
    LocationCode, BusTypeCode, EnterpriseStatusCode, VsicCode, Ticker, OrganName, OrganShortName, FriendlyName,
    TaxCode, EnterpriseCode, Address, Telephone, Fax, Email, Website, Logo, TaxCodeStatus, FoundingDate,
    NumberOfChange, NumberOfEmployee, ExDateEmployee, en_OrganName, en_OrganShortName, en_FriendlyName,
    en_Address, Status, CreateDate, UpdateDate) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT ON CONSTRAINT
    pk_stx_cpf_companyinformation_id DO UPDATE SET (OrganCode, REPPersonId, OwernerPersonId, LocationCode,
    BusTypeCode, EnterpriseStatusCode, VsicCode, Ticker, OrganName, OrganShortName, FriendlyName, TaxCode,
    EnterpriseCode, Address, Telephone, Fax, Email, Website, Logo, TaxCodeStatus, FoundingDate, NumberOfChange,
    NumberOfEmployee, ExDateEmployee, en_OrganName, en_OrganShortName, en_FriendlyName, en_Address, Status,
    CreateDate, UpdateDate) = (EXCLUDED.OrganCode, EXCLUDED.REPPersonId, EXCLUDED.OwernerPersonId,
    EXCLUDED.LocationCode, EXCLUDED.BusTypeCode, EXCLUDED.EnterpriseStatusCode, EXCLUDED.VsicCode,
    EXCLUDED.Ticker, EXCLUDED.OrganName, EXCLUDED.OrganShortName, EXCLUDED.FriendlyName, EXCLUDED.TaxCode,
    EXCLUDED.EnterpriseCode, EXCLUDED.Address, EXCLUDED.Telephone, EXCLUDED.Fax, EXCLUDED.Email,
    EXCLUDED.Website, EXCLUDED.Logo, EXCLUDED.TaxCodeStatus, EXCLUDED.FoundingDate, EXCLUDED.NumberOfChange,
    EXCLUDED.NumberOfEmployee, EXCLUDED.ExDateEmployee, EXCLUDED.en_OrganName, EXCLUDED.en_OrganShortName,
    EXCLUDED.en_FriendlyName, EXCLUDED.en_Address, EXCLUDED.Status, EXCLUDED.CreateDate, EXCLUDED.UpdateDate)''',
                          [[item.get("OrganCode"), item.get("REPPersonId"), item.get("OwernerPersonId"),
                            item.get("LocationCode"),
                            item.get("BusTypeCode"),
                            item.get("EnterpriseStatusCode"), item.get("VsicCode"), item.get("Ticker"),
                            item.get("OrganName"),
                            item.get("OrganShortName"),
                            item.get("FriendlyName"), item.get("TaxCode"), item.get("EnterpriseCode"),
                            item.get("Address"),
                            item.get("Telephone"),
                            item.get("Fax"), item.get("Email"), item.get("Website"), item.get("Logo"),
                            item.get("TaxCodeStatus"),
                            item.get("FoundingDate"), item.get("NumberOfChange"), item.get("NumberOfEmployee"),
                            item.get("ExDateEmployee"),
                            item.get("en_OrganName"),
                            item.get("en_OrganShortName"), item.get("en_FriendlyName"), item.get("en_Address"),
                            item.get("Status"),
                            item.get("CreateDate"),
                            item.get("UpdateDate")] for item in data])
    print("Done insert data table stx_cpf_companyinformation / Company", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    telegram.send(mess=f"Done insert data table stx_cpf_companyinformation / Company / Data size: {len(data)}")

# dateReq = '2022-08-12'
# get_organization(dateReq)
# get_company_information(dateReq)
