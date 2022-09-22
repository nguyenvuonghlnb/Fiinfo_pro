import service
import database
import time
import telegram
from datetime import datetime

BASE_URL = "http://df31.fiintek.com"
DATE_TYPE = "updatedate"


def get_announcement_vi(date_request):
    # Init parameter
    if not date_request:  # Nếu không truyền ngày thì mặc định lấy ngày hiện tại
        date_request = datetime.now().strftime("%Y-%m-%d")
    page_index = 1
    page_size = 100
    url = BASE_URL + "/Message/GetAnnoucementVi?pageIndex=" + str(page_index) \
          + "&pageSize=" + str(page_size) \
          + "&DateType=" + DATE_TYPE + "&fromDate=" + date_request + "&toDate=" + date_request
    print("[get_announcement_vi] get date >> ", url)
    response = service.make_request(url)
    if response is None:
        return

    data = response["Data"]
    paging = response["Paging"]

    while paging.get("NextPage") is not None:
        time.sleep(10)
        print("NextPage : %s in TotalPage : %s", paging.get("NextPage"), paging.get("TotalPage"))
        url = BASE_URL + "/Message/GetAnnoucementVi?pageIndex=" + str(paging.get("NextPage")) \
              + "&pageSize=" + str(page_size) \
              + "&DateType=" + DATE_TYPE + "&fromDate=" + date_request + "&toDate=" + date_request
        print("[get_announcement_vi] Next page found >> ", url)
        response = service.make_request(url)
        data = data + response["Data"]
        paging = response["Paging"]

        print("[get_announcement_vi] Data size: ", len(data))
    database.execute_many('''INSERT INTO fiin_dtf.stx_msg_newsvi (NewsId, OrganCode, NewsCategoryCode, IcbCode, 
    ComGroupCode, ExpertId, Ticker, NewsTitle, FriendlyTitle, NewsSubTitle, FriendlySubTitle, NewsShortContent, 
    NewsFullContent, NewsImageUrl, NewsSmallImageUrl, NewsSource, NewsSourceLink, NewsAuthor, NewsKeyword, 
    FriendlyKeyword, PublicDate, IsTranfer, Status, CreateDate, UpdateDate, SourceCode) VALUES (%s, %s, %s, %s, 
    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT ON 
    CONSTRAINT pk_stx_msg_newsvi DO UPDATE SET (NewsId, OrganCode, NewsCategoryCode, IcbCode, ComGroupCode, 
    ExpertId, Ticker, NewsTitle, FriendlyTitle, NewsSubTitle, FriendlySubTitle, NewsShortContent, 
    NewsFullContent, NewsImageUrl, NewsSmallImageUrl, NewsSource, NewsSourceLink, NewsAuthor, NewsKeyword, 
    FriendlyKeyword, PublicDate, IsTranfer, Status, CreateDate, UpdateDate, SourceCode) = (EXCLUDED.NewsId, 
    EXCLUDED.OrganCode, EXCLUDED.NewsCategoryCode, EXCLUDED.IcbCode, EXCLUDED.ComGroupCode, EXCLUDED.ExpertId, 
    EXCLUDED.Ticker, EXCLUDED.NewsTitle, EXCLUDED.FriendlyTitle, EXCLUDED.NewsSubTitle, 
    EXCLUDED.FriendlySubTitle, EXCLUDED.NewsShortContent, EXCLUDED.NewsFullContent, EXCLUDED.NewsImageUrl, 
    EXCLUDED.NewsSmallImageUrl, EXCLUDED.NewsSource, EXCLUDED.NewsSourceLink, EXCLUDED.NewsAuthor, 
    EXCLUDED.NewsKeyword, EXCLUDED.FriendlyKeyword, EXCLUDED.PublicDate, EXCLUDED.IsTranfer, EXCLUDED.Status, 
    EXCLUDED.CreateDate, EXCLUDED.UpdateDate, EXCLUDED.SourceCode)''',
                          [[item.get("NewsId"), item.get("OrganCode"), item.get("NewsCategoryCode"),
                            item.get("IcbCode"),
                            item.get("ComGroupCode"),
                            item.get("ExpertId"), item.get("Ticker"), item.get("NewsTitle"), item.get("FriendlyTitle"),
                            item.get("NewsSubTitle"),
                            item.get("FriendlySubTitle"), item.get("NewsShortContent"), item.get("NewsFullContent"),
                            item.get("NewsImageUrl"),
                            item.get("NewsSmallImageUrl"),
                            item.get("NewsSource"), item.get("NewsSourceLink"), item.get("NewsAuthor"),
                            item.get("NewsKeyword"),
                            item.get("FriendlyKeyword"),
                            item.get("PublicDate"), item.get("IsTranfer"), item.get("Status"), item.get("CreateDate"),
                            item.get("UpdateDate"),
                            item.get("SourceCode")] for item in data])
    print("Done insert data table stx_msg_newsvi / Message", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    telegram.send(mess=f"Done insert data table stx_msg_newsvi / Message / Data size: {len(data)}")
