import service
import database
import time
import telegram
from datetime import datetime

BASE_URL = "http://df31.fiintek.com"
DATE_TYPE = "tradingdate"


def get_hnx_stock(date_request):
    # Init parameter
    if not date_request:  # Nếu không truyền ngày thì mặc định lấy ngày hiện tại
        date_request = datetime.now().strftime("%Y-%m-%d")
    page_index = 1
    page_size = 100
    url = BASE_URL + "/Market/GetHnxStock?pageIndex=" + str(page_index) \
          + "&pageSize=" + str(page_size) \
          + "&DateType=" + DATE_TYPE + "&fromDate=" + date_request + "&toDate=" + date_request
    print("[get_hnx_stock] get date >> ", url)
    response = service.make_request(url)
    if response is None:
        return

    data = response["Data"]
    paging = response["Paging"]

    while paging.get("NextPage") is not None:
        time.sleep(10)
        print("[get_hnx_stock] NextPage : %s in TotalPage : %s", paging.get("NextPage"), paging.get("TotalPage"))
        url = BASE_URL + "/Market/GetHnxStock?pageIndex=" + str(paging.get("NextPage")) \
              + "&pageSize=" + str(page_size) \
              + "&DateType=" + DATE_TYPE + "&fromDate=" + date_request + "&toDate=" + date_request
        print("[get_hnx_stock] Next page found >> ", url)
        response = service.make_request(url)
        data = data + response["Data"]
        paging = response["Paging"]

        print("[get_hnx_stock] Data size: ", len(data))

    database.execute_many('''INSERT INTO fiin_dtf.stx_mrk_hnxstock (HnxStockId, OrganCode, Ticker, TradingDate, 
    StockType, CeilingPrice, FloorPrice, ReferencePrice, ReferenceDate, OpenPrice, ClosePrice, MatchPrice, 
    MatchVolume, MatchValue, PriceChange, PercentPriceChange, HighestPrice, LowestPrice, AveragePrice, 
    DealVolume, DealPrice, TotalMatchVolumeEven, TotalMatchValueEven, TotalMatchVolume, TotalMatchValue, 
    TotalDealVolume, TotalDealValue, TotalVolume, TotalValue, ForeignBuyValueMatched, ForeignBuyVolumeMatched, 
    ForeignSellValueMatched, ForeignSellVolumeMatched, ForeignBuyValueDeal, ForeignBuyVolumeDeal, 
    ForeignSellValueDeal, ForeignSellVolumeDeal, ForeignBuyValueTotal, ForeignBuyVolumeTotal, 
    ForeignSellValueTotal, ForeignSellVolumeTotal, ForeignTotalRoom, ForeignCurrentRoom, Parvalue, 
    SecurityTradingStatus, ListingStatus, IssueDate, BestBidPrice, BestBidVolume, BestOfferPrice, 
    BestOfferVolume, PriorOpenPrice, PriorClosePrice, TotalListingVolume, ReferenceStatus, CurrentPrice, 
    CurrentVolume, PriorPrice, TotalBuyTradingVolume, BuyCount, TotalBuyTradingValue, TotalSellTradingVolume, 
    SellCount, TotalSellTradingValue, TotalBidQtty_OD, TotalOfferQtty_OD, TotalTrade, TotalBuyTrade, 
    TotalBuyTradeVolume, TotalSellTrade, TotalSellTradeVolume, ReferencePriceAdjusted, OpenPriceAdjusted, 
    ClosePriceAdjusted, PriceChangeAdjusted, PercentPriceChangeAdjusted, HighestPriceAdjusted, 
    LowestPriceAdjusted, RateAdjusted, Status, CreateDate, UpdateDate, MaturityDate, CouponRate, BoardCode, 
    IndexCode, DealValue, ShareIssue) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT ON CONSTRAINT 
    xpkstx_mrk_hnxstock DO UPDATE SET (HnxStockId, OrganCode, Ticker, TradingDate, StockType, CeilingPrice, 
    FloorPrice, ReferencePrice, ReferenceDate, OpenPrice, ClosePrice, MatchPrice, MatchVolume, MatchValue, 
    PriceChange, PercentPriceChange, HighestPrice, LowestPrice, AveragePrice, DealVolume, DealPrice, 
    TotalMatchVolumeEven, TotalMatchValueEven, TotalMatchVolume, TotalMatchValue, TotalDealVolume, 
    TotalDealValue, TotalVolume, TotalValue, ForeignBuyValueMatched, ForeignBuyVolumeMatched, 
    ForeignSellValueMatched, ForeignSellVolumeMatched, ForeignBuyValueDeal, ForeignBuyVolumeDeal, 
    ForeignSellValueDeal, ForeignSellVolumeDeal, ForeignBuyValueTotal, ForeignBuyVolumeTotal, 
    ForeignSellValueTotal, ForeignSellVolumeTotal, ForeignTotalRoom, ForeignCurrentRoom, Parvalue, 
    SecurityTradingStatus, ListingStatus, IssueDate, BestBidPrice, BestBidVolume, BestOfferPrice, 
    BestOfferVolume, PriorOpenPrice, PriorClosePrice, TotalListingVolume, ReferenceStatus, CurrentPrice, 
    CurrentVolume, PriorPrice, TotalBuyTradingVolume, BuyCount, TotalBuyTradingValue, TotalSellTradingVolume, 
    SellCount, TotalSellTradingValue, TotalBidQtty_OD, TotalOfferQtty_OD, TotalTrade, TotalBuyTrade, 
    TotalBuyTradeVolume, TotalSellTrade, TotalSellTradeVolume, ReferencePriceAdjusted, OpenPriceAdjusted, 
    ClosePriceAdjusted, PriceChangeAdjusted, PercentPriceChangeAdjusted, HighestPriceAdjusted, 
    LowestPriceAdjusted, RateAdjusted, Status, CreateDate, UpdateDate, MaturityDate, CouponRate, BoardCode, 
    IndexCode, DealValue, ShareIssue) = (EXCLUDED.HnxStockId, EXCLUDED.OrganCode, EXCLUDED.Ticker, 
    EXCLUDED.TradingDate, EXCLUDED. StockType, EXCLUDED.CeilingPrice, EXCLUDED.FloorPrice, 
    EXCLUDED.ReferencePrice, EXCLUDED.ReferenceDate, EXCLUDED.OpenPrice, EXCLUDED.ClosePrice, 
    EXCLUDED.MatchPrice, EXCLUDED. MatchVolume, EXCLUDED.MatchValue, EXCLUDED.PriceChange, 
    EXCLUDED.PercentPriceChange, EXCLUDED.HighestPrice, EXCLUDED.LowestPrice, EXCLUDED.AveragePrice, 
    EXCLUDED. DealVolume, EXCLUDED.DealPrice, EXCLUDED.TotalMatchVolumeEven, EXCLUDED.TotalMatchValueEven, 
    EXCLUDED.TotalMatchVolume, EXCLUDED.TotalMatchValue, EXCLUDED. TotalDealVolume, EXCLUDED.TotalDealValue, 
    EXCLUDED.TotalVolume, EXCLUDED.TotalValue, EXCLUDED.ForeignBuyValueMatched, EXCLUDED. 
    ForeignBuyVolumeMatched, EXCLUDED.ForeignSellValueMatched, EXCLUDED.ForeignSellVolumeMatched, 
    EXCLUDED.ForeignBuyValueDeal, EXCLUDED. ForeignBuyVolumeDeal, EXCLUDED.ForeignSellValueDeal, 
    EXCLUDED.ForeignSellVolumeDeal, EXCLUDED.ForeignBuyValueTotal, EXCLUDED. ForeignBuyVolumeTotal, 
    EXCLUDED.ForeignSellValueTotal, EXCLUDED.ForeignSellVolumeTotal, EXCLUDED.ForeignTotalRoom, EXCLUDED. 
    ForeignCurrentRoom, EXCLUDED.Parvalue, EXCLUDED.SecurityTradingStatus, EXCLUDED.ListingStatus, 
    EXCLUDED.IssueDate, EXCLUDED.BestBidPrice, EXCLUDED. BestBidVolume, EXCLUDED.BestOfferPrice, 
    EXCLUDED.BestOfferVolume, EXCLUDED.PriorOpenPrice, EXCLUDED.PriorClosePrice, EXCLUDED.TotalListingVolume, 
    EXCLUDED. ReferenceStatus, EXCLUDED.CurrentPrice, EXCLUDED.CurrentVolume, EXCLUDED.PriorPrice, 
    EXCLUDED.TotalBuyTradingVolume, EXCLUDED.BuyCount, EXCLUDED. TotalBuyTradingValue, 
    EXCLUDED.TotalSellTradingVolume, EXCLUDED.SellCount, EXCLUDED.TotalSellTradingValue, 
    EXCLUDED.TotalBidQtty_OD, EXCLUDED. TotalOfferQtty_OD, EXCLUDED.TotalTrade, EXCLUDED.TotalBuyTrade, 
    EXCLUDED.TotalBuyTradeVolume, EXCLUDED.TotalSellTrade, EXCLUDED.TotalSellTradeVolume, EXCLUDED. 
    ReferencePriceAdjusted, EXCLUDED.OpenPriceAdjusted, EXCLUDED.ClosePriceAdjusted, 
    EXCLUDED.PriceChangeAdjusted, EXCLUDED. PercentPriceChangeAdjusted, EXCLUDED.HighestPriceAdjusted, 
    EXCLUDED.LowestPriceAdjusted, EXCLUDED.RateAdjusted, EXCLUDED.Status, EXCLUDED.CreateDate, EXCLUDED. 
    UpdateDate, EXCLUDED.MaturityDate, EXCLUDED.CouponRate, EXCLUDED.BoardCode, EXCLUDED.IndexCode, 
    EXCLUDED.DealValue, EXCLUDED.ShareIssue)''',
                          [[item.get("HnxStockId"), item.get("OrganCode"), item.get("Ticker"), item.get("TradingDate"),
                            item.get("StockType"),
                            item.get("CeilingPrice"), item.get("FloorPrice"),
                            item.get("ReferencePrice"), item.get("ReferenceDate"), item.get("OpenPrice"),
                            item.get("ClosePrice"),
                            item.get("MatchPrice"), item.get("MatchVolume"), item.get("MatchValue"),
                            item.get("PriceChange"), item.get("PercentPriceChange"),
                            item.get("HighestPrice"), item.get("LowestPrice"),
                            item.get("AveragePrice"), item.get("DealVolume"), item.get("DealPrice"),
                            item.get("TotalMatchVolumeEven"), item.get("TotalMatchValueEven"),
                            item.get("TotalMatchVolume"),
                            item.get("TotalMatchValue"), item.get("TotalDealVolume"), item.get("TotalDealValue"),
                            item.get("TotalVolume"), item.get("TotalValue"), item.get("ForeignBuyValueMatched"),
                            item.get("ForeignBuyVolumeMatched"), item.get("ForeignSellValueMatched"),
                            item.get("ForeignSellVolumeMatched"),
                            item.get("ForeignBuyValueDeal"),
                            item.get("ForeignBuyVolumeDeal"), item.get("ForeignSellValueDeal"),
                            item.get("ForeignSellVolumeDeal"),
                            item.get("ForeignBuyValueTotal"), item.get("ForeignBuyVolumeTotal"),
                            item.get("ForeignSellValueTotal"),
                            item.get("ForeignSellVolumeTotal"),
                            item.get("ForeignTotalRoom"), item.get("ForeignCurrentRoom"), item.get("Parvalue"),
                            item.get("SecurityTradingStatus"), item.get("ListingStatus"), item.get("IssueDate"),
                            item.get("BestBidPrice"),
                            item.get("BestBidVolume"), item.get("BestOfferPrice"), item.get("BestOfferVolume"),
                            item.get("PriorOpenPrice"),
                            item.get("PriorClosePrice"), item.get("TotalListingVolume"), item.get("ReferenceStatus"),
                            item.get("CurrentPrice"), item.get("CurrentVolume"), item.get("PriorPrice"),
                            item.get("TotalBuyTradingVolume"),
                            item.get("BuyCount"), item.get("TotalBuyTradingValue"), item.get("TotalSellTradingVolume"),
                            item.get("SellCount"), item.get("TotalSellTradingValue"), item.get("TotalBidQtty_OD"),
                            item.get("TotalOfferQtty_OD"), item.get("TotalTrade"), item.get("TotalBuyTrade"),
                            item.get("TotalBuyTradeVolume"),
                            item.get("TotalSellTrade"), item.get("TotalSellTradeVolume"),
                            item.get("ReferencePriceAdjusted"),
                            item.get("OpenPriceAdjusted"), item.get("ClosePriceAdjusted"),
                            item.get("PriceChangeAdjusted"),
                            item.get("PercentPriceChangeAdjusted"),
                            item.get("HighestPriceAdjusted"), item.get("LowestPriceAdjusted"), item.get("RateAdjusted"),
                            item.get("Status"),
                            item.get("CreateDate"), item.get("UpdateDate"), item.get("MaturityDate"),
                            item.get("CouponRate"),
                            item.get("BoardCode"),
                            item.get("IndexCode"), item.get("DealValue"), item.get("ShareIssue")] for item in data])
    print("Done insert data table stx_mrk_hnxstock / Market", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    telegram.send(mess=f"Done insert data table stx_mrk_hnxstock / Market / Data size: {len(data)}")


def get_hose_index(date_request):
    # Init parameter
    if not date_request:  # Nếu không truyền ngày thì mặc định lấy ngày hiện tại
        date_request = datetime.now().strftime("%Y-%m-%d")
    page_index = 1
    page_size = 100
    url = BASE_URL + "/Market/GetHoseIndex?pageIndex=" + str(page_index) \
          + "&pageSize=" + str(page_size) \
          + "&DateType=" + DATE_TYPE + "&fromDate=" + date_request + "&toDate=" + date_request
    print("[get_hose_index] get date >> ", url)
    response = service.make_request(url)
    if response is None:
        return

    data = response["Data"]
    paging = response["Paging"]

    while paging.get("NextPage") is not None:
        time.sleep(10)
        print("[get_hose_index] NextPage : %s in TotalPage : %s", paging.get("NextPage"), paging.get("TotalPage"))
        url = BASE_URL + "/Market/GetHoseIndex?pageIndex=" + str(paging.get("NextPage")) \
              + "&pageSize=" + str(page_size) \
              + "&DateType=" + DATE_TYPE + "&fromDate=" + date_request + "&toDate=" + date_request
        print("[get_hose_index] Next page found >> ", url)
        response = service.make_request(url)
        data = data + response["Data"]
        paging = response["Paging"]

        print("[get_hose_index] Data size: ", len(data))

    database.execute_many('''INSERT INTO fiin_dtf.stx_mrk_hoseindex (HoseIndexId, ComGroupCode, IndexValue, TradingDate, 
    IndexChange, PercentIndexChange, ReferenceIndex, OpenIndex, CloseIndex, HighestIndex, LowestIndex, TypeIndex, 
    TotalMatchVolume, TotalMatchValue, TotalDealVolume, TotalDealValue, TotalVolume, TotalValue, 
    TotalStockUpPrice, TotalStockDownPrice, TotalStockNoChangePrice, TotalUpVolume, TotalDownVolume, 
    TotalNoChangeVolume, TotalTrade, TotalBuyTrade, TotalBuyTradeVolume, TotalSellTrade, TotalSellTradeVolume, 
    ForeignBuyValueMatched, ForeignBuyVolumeMatched, ForeignSellValueMatched, ForeignSellVolumeMatched, 
    ForeignBuyValueDeal, ForeignBuyVolumeDeal, ForeignSellValueDeal, ForeignSellVolumeDeal, ForeignBuyValueTotal, 
    ForeignBuyVolumeTotal, ForeignSellValueTotal, ForeignSellVolumeTotal, ForeignTotalRoom, ForeignCurrentRoom, 
    Status, CreateDate, UpdateDate, ShareIssue) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
    %s, %s, %s, %s, %s, %s) ON CONFLICT ON CONSTRAINT xpkstx_mrk_hoseindex DO UPDATE SET (HoseIndexId, 
    ComGroupCode, IndexValue, TradingDate, IndexChange, PercentIndexChange, ReferenceIndex, OpenIndex, 
    CloseIndex, HighestIndex, LowestIndex, TypeIndex, TotalMatchVolume, TotalMatchValue, TotalDealVolume, 
    TotalDealValue, TotalVolume, TotalValue, TotalStockUpPrice, TotalStockDownPrice, TotalStockNoChangePrice, 
    TotalUpVolume, TotalDownVolume, TotalNoChangeVolume, TotalTrade, TotalBuyTrade, TotalBuyTradeVolume, 
    TotalSellTrade, TotalSellTradeVolume, ForeignBuyValueMatched, ForeignBuyVolumeMatched, 
    ForeignSellValueMatched, ForeignSellVolumeMatched, ForeignBuyValueDeal, ForeignBuyVolumeDeal, 
    ForeignSellValueDeal, ForeignSellVolumeDeal, ForeignBuyValueTotal, ForeignBuyVolumeTotal, 
    ForeignSellValueTotal, ForeignSellVolumeTotal, ForeignTotalRoom, ForeignCurrentRoom, Status, CreateDate, 
    UpdateDate, ShareIssue) = (EXCLUDED.HoseIndexId, EXCLUDED.ComGroupCode, EXCLUDED.IndexValue, 
    EXCLUDED. TradingDate, EXCLUDED.IndexChange, EXCLUDED.PercentIndexChange, EXCLUDED.ReferenceIndex, 
    EXCLUDED.OpenIndex, EXCLUDED.CloseIndex, EXCLUDED.HighestIndex, EXCLUDED. LowestIndex, EXCLUDED.TypeIndex, 
    EXCLUDED.TotalMatchVolume, EXCLUDED.TotalMatchValue, EXCLUDED.TotalDealVolume, EXCLUDED.TotalDealValue, 
    EXCLUDED.TotalVolume, EXCLUDED. TotalValue, EXCLUDED.TotalStockUpPrice, EXCLUDED.TotalStockDownPrice, 
    EXCLUDED.TotalStockNoChangePrice, EXCLUDED.TotalUpVolume, EXCLUDED.TotalDownVolume, EXCLUDED. 
    TotalNoChangeVolume, EXCLUDED.TotalTrade, EXCLUDED.TotalBuyTrade, EXCLUDED.TotalBuyTradeVolume, 
    EXCLUDED.TotalSellTrade, EXCLUDED.TotalSellTradeVolume, EXCLUDED. ForeignBuyValueMatched, 
    EXCLUDED.ForeignBuyVolumeMatched, EXCLUDED.ForeignSellValueMatched, EXCLUDED.ForeignSellVolumeMatched, 
    EXCLUDED. ForeignBuyValueDeal, EXCLUDED.ForeignBuyVolumeDeal, EXCLUDED.ForeignSellValueDeal, 
    EXCLUDED.ForeignSellVolumeDeal, EXCLUDED.ForeignBuyValueTotal, EXCLUDED. ForeignBuyVolumeTotal, 
    EXCLUDED.ForeignSellValueTotal, EXCLUDED.ForeignSellVolumeTotal, EXCLUDED.ForeignTotalRoom, 
    EXCLUDED.ForeignCurrentRoom, EXCLUDED. Status, EXCLUDED.CreateDate, EXCLUDED.UpdateDate, 
    EXCLUDED.ShareIssue)''',
                          [[item.get("HoseIndexId"), item.get("ComGroupCode"), item.get("IndexValue"),
                            item.get("TradingDate"),
                            item.get("IndexChange"),
                            item.get("PercentIndexChange"), item.get("ReferenceIndex"), item.get("OpenIndex"),
                            item.get("CloseIndex"),
                            item.get("HighestIndex"),
                            item.get("LowestIndex"), item.get("TypeIndex"), item.get("TotalMatchVolume"),
                            item.get("TotalMatchValue"),
                            item.get("TotalDealVolume"),
                            item.get("TotalDealValue"), item.get("TotalVolume"), item.get("TotalValue"),
                            item.get("TotalStockUpPrice"),
                            item.get("TotalStockDownPrice"),
                            item.get("TotalStockNoChangePrice"), item.get("TotalUpVolume"), item.get("TotalDownVolume"),
                            item.get("TotalNoChangeVolume"), item.get("TotalTrade"),
                            item.get("TotalBuyTrade"), item.get("TotalBuyTradeVolume"), item.get("TotalSellTrade"),
                            item.get("TotalSellTradeVolume"), item.get("ForeignBuyValueMatched"),
                            item.get("ForeignBuyVolumeMatched"), item.get("ForeignSellValueMatched"),
                            item.get("ForeignSellVolumeMatched"),
                            item.get("ForeignBuyValueDeal"), item.get("ForeignBuyVolumeDeal"),
                            item.get("ForeignSellValueDeal"), item.get("ForeignSellVolumeDeal"),
                            item.get("ForeignBuyValueTotal"),
                            item.get("ForeignBuyVolumeTotal"), item.get("ForeignSellValueTotal"),
                            item.get("ForeignSellVolumeTotal"), item.get("ForeignTotalRoom"),
                            item.get("ForeignCurrentRoom"),
                            item.get("Status"), item.get("CreateDate"),
                            item.get("UpdateDate"), item.get("ShareIssue")] for item in data])
    print("Done insert data table stx_mrk_hoseindex / Market", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    telegram.send(mess=f"Done insert data table stx_mrk_hoseindex / Market / Data size: {len(data)}")


def get_hose_stock(date_request):
    # Init parameter
    if not date_request:  # Nếu không truyền ngày thì mặc định lấy ngày hiện tại
        date_request = datetime.now().strftime("%Y-%m-%d")
    page_index = 1
    page_size = 100
    url = BASE_URL + "/Market/GetHoseStock?pageIndex=" + str(page_index) \
          + "&pageSize=" + str(page_size) \
          + "&DateType=" + DATE_TYPE + "&fromDate=" + date_request + "&toDate=" + date_request
    print("[get_hose_stock] get date >> ", url)
    response = service.make_request(url)
    if response is None:
        return

    data = response["Data"]
    paging = response["Paging"]

    while paging.get("NextPage") is not None:
        time.sleep(10)
        print("[get_hose_stock] NextPage : %s in TotalPage : %s", paging.get("NextPage"), paging.get("TotalPage"))
        url = BASE_URL + "/Market/GetHoseStock?pageIndex=" + str(paging.get("NextPage")) \
              + "&pageSize=" + str(page_size) \
              + "&DateType=" + DATE_TYPE + "&fromDate=" + date_request + "&toDate=" + date_request
        print("[get_hose_stock] Next page found >> ", url)
        response = service.make_request(url)
        data = data + response["Data"]
        paging = response["Paging"]

        print("[get_hose_stock] Data size: ", len(data))

    database.execute_many('''INSERT INTO fiin_dtf.stx_mrk_hosestock (HoseStockId, OrganCode, Ticker, TradingDate, 
    StockType, CeilingPrice, FloorPrice, ReferencePrice, ReferenceDate, OpenPrice, ClosePrice, MatchPrice, 
    PriceChange, PercentPriceChange, HighestPrice, LowestPrice, AveragePrice, MatchVolume, MatchValue, 
    DealVolume, DealValue, TotalMatchVolume, TotalMatchValue, TotalDealVolume, TotalDealValue, TotalVolume, 
    TotalValue, ForeignBuyValueMatched, ForeignBuyVolumeMatched, ForeignSellValueMatched, 
    ForeignSellVolumeMatched, ForeignBuyValueDeal, ForeignBuyVolumeDeal, ForeignSellValueDeal, 
    ForeignSellVolumeDeal, ForeignBuyValueTotal, ForeignBuyVolumeTotal, ForeignSellValueTotal, 
    ForeignSellVolumeTotal, ForeignTotalRoom, ForeignCurrentRoom, ParValue, Suspension, Delist, HaltResumeFlag, 
    Split, Benefit, Meeting, Notice, IssueDate, INav, IIndex, TotalTrade, TotalBuyTrade, TotalBuyTradeVolume, 
    TotalSellTrade, TotalSellTradeVolume, ReferencePriceAdjusted, OpenPriceAdjusted, ClosePriceAdjusted, 
    PriceChangeAdjusted, PercentPriceChangeAdjusted, HighestPriceAdjusted, LowestPriceAdjusted, RateAdjusted, 
    Status, CreateDate, UpdateDate, Best1OfferPrice, Best2OfferPrice, Best3OfferPrice, Best1OfferVolume, 
    Best2OfferVolume, Best3OfferVolume, Best1BidPrice, Best2BidPrice, Best3BidPrice, Best1BidVolume, 
    Best2BidVolume, Best3BidVolume, ShareIssue) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT ON CONSTRAINT xpkstx_mrk_hosestock DO UPDATE 
    SET (HoseStockId, OrganCode, Ticker, TradingDate, StockType, CeilingPrice, FloorPrice, ReferencePrice, 
    ReferenceDate, OpenPrice, ClosePrice, MatchPrice, PriceChange, PercentPriceChange, HighestPrice, LowestPrice, 
    AveragePrice, MatchVolume, MatchValue, DealVolume, DealValue, TotalMatchVolume, TotalMatchValue, 
    TotalDealVolume, TotalDealValue, TotalVolume, TotalValue, ForeignBuyValueMatched, ForeignBuyVolumeMatched, 
    ForeignSellValueMatched, ForeignSellVolumeMatched, ForeignBuyValueDeal, ForeignBuyVolumeDeal, 
    ForeignSellValueDeal, ForeignSellVolumeDeal, ForeignBuyValueTotal, ForeignBuyVolumeTotal, 
    ForeignSellValueTotal, ForeignSellVolumeTotal, ForeignTotalRoom, ForeignCurrentRoom, ParValue, Suspension, 
    Delist, HaltResumeFlag, Split, Benefit, Meeting, Notice, IssueDate, INav, IIndex, TotalTrade, TotalBuyTrade, 
    TotalBuyTradeVolume, TotalSellTrade, TotalSellTradeVolume, ReferencePriceAdjusted, OpenPriceAdjusted, 
    ClosePriceAdjusted, PriceChangeAdjusted, PercentPriceChangeAdjusted, HighestPriceAdjusted, 
    LowestPriceAdjusted, RateAdjusted, Status, CreateDate, UpdateDate, Best1OfferPrice, Best2OfferPrice, 
    Best3OfferPrice, Best1OfferVolume, Best2OfferVolume, Best3OfferVolume, Best1BidPrice, Best2BidPrice, 
    Best3BidPrice, Best1BidVolume, Best2BidVolume, Best3BidVolume, ShareIssue) = (EXCLUDED.HoseStockId, 
    EXCLUDED.OrganCode, EXCLUDED.Ticker, EXCLUDED.TradingDate, EXCLUDED. StockType, EXCLUDED.CeilingPrice, 
    EXCLUDED.FloorPrice, EXCLUDED.ReferencePrice, EXCLUDED.ReferenceDate, EXCLUDED.OpenPrice, 
    EXCLUDED.ClosePrice, EXCLUDED.MatchPrice, EXCLUDED.PriceChange, EXCLUDED.PercentPriceChange, 
    EXCLUDED.HighestPrice, EXCLUDED.LowestPrice, EXCLUDED.AveragePrice, EXCLUDED.MatchVolume, 
    EXCLUDED.MatchValue, EXCLUDED. DealVolume, EXCLUDED.DealValue, EXCLUDED.TotalMatchVolume, 
    EXCLUDED.TotalMatchValue, EXCLUDED.TotalDealVolume, EXCLUDED.TotalDealValue, EXCLUDED.TotalVolume, 
    EXCLUDED. TotalValue, EXCLUDED.ForeignBuyValueMatched, EXCLUDED.ForeignBuyVolumeMatched, 
    EXCLUDED.ForeignSellValueMatched, EXCLUDED. ForeignSellVolumeMatched, EXCLUDED.ForeignBuyValueDeal, 
    EXCLUDED.ForeignBuyVolumeDeal, EXCLUDED.ForeignSellValueDeal, EXCLUDED. ForeignSellVolumeDeal, 
    EXCLUDED.ForeignBuyValueTotal, EXCLUDED.ForeignBuyVolumeTotal, EXCLUDED.ForeignSellValueTotal, 
    EXCLUDED. ForeignSellVolumeTotal, EXCLUDED.ForeignTotalRoom, EXCLUDED.ForeignCurrentRoom, EXCLUDED.ParValue, 
    EXCLUDED.Suspension, EXCLUDED.Delist, EXCLUDED.HaltResumeFlag, EXCLUDED. Split, EXCLUDED.Benefit, 
    EXCLUDED.Meeting, EXCLUDED.Notice, EXCLUDED.IssueDate, EXCLUDED.INav, EXCLUDED.IIndex, EXCLUDED.TotalTrade, 
    EXCLUDED.TotalBuyTrade, EXCLUDED.TotalBuyTradeVolume, EXCLUDED. TotalSellTrade, 
    EXCLUDED.TotalSellTradeVolume, EXCLUDED.ReferencePriceAdjusted, EXCLUDED.OpenPriceAdjusted, 
    EXCLUDED.ClosePriceAdjusted, EXCLUDED. PriceChangeAdjusted, EXCLUDED.PercentPriceChangeAdjusted, 
    EXCLUDED.HighestPriceAdjusted, EXCLUDED.LowestPriceAdjusted, EXCLUDED.RateAdjusted, EXCLUDED. Status, 
    EXCLUDED.CreateDate, EXCLUDED.UpdateDate, EXCLUDED.Best1OfferPrice, EXCLUDED.Best2OfferPrice, 
    EXCLUDED.Best3OfferPrice, EXCLUDED.Best1OfferVolume, EXCLUDED. Best2OfferVolume, EXCLUDED.Best3OfferVolume, 
    EXCLUDED.Best1BidPrice, EXCLUDED.Best2BidPrice, EXCLUDED.Best3BidPrice, EXCLUDED.Best1BidVolume, 
    EXCLUDED. Best2BidVolume, EXCLUDED.Best3BidVolume, EXCLUDED.ShareIssue)''',
                          [[item.get("HoseStockId"), item.get("OrganCode"), item.get("Ticker"), item.get("TradingDate"),
                            item.get("StockType"),
                            item.get("CeilingPrice"), item.get("FloorPrice"),
                            item.get("ReferencePrice"), item.get("ReferenceDate"), item.get("OpenPrice"),
                            item.get("ClosePrice"),
                            item.get("MatchPrice"),
                            item.get("PriceChange"), item.get("PercentPriceChange"),
                            item.get("HighestPrice"), item.get("LowestPrice"), item.get("AveragePrice"),
                            item.get("MatchVolume"),
                            item.get("MatchValue"), item.get("DealVolume"), item.get("DealValue"),
                            item.get("TotalMatchVolume"), item.get("TotalMatchValue"), item.get("TotalDealVolume"),
                            item.get("TotalDealValue"), item.get("TotalVolume"), item.get("TotalValue"),
                            item.get("ForeignBuyValueMatched"), item.get("ForeignBuyVolumeMatched"),
                            item.get("ForeignSellValueMatched"),
                            item.get("ForeignSellVolumeMatched"), item.get("ForeignBuyValueDeal"),
                            item.get("ForeignBuyVolumeDeal"),
                            item.get("ForeignSellValueDeal"),
                            item.get("ForeignSellVolumeDeal"), item.get("ForeignBuyValueTotal"),
                            item.get("ForeignBuyVolumeTotal"),
                            item.get("ForeignSellValueTotal"), item.get("ForeignSellVolumeTotal"),
                            item.get("ForeignTotalRoom"),
                            item.get("ForeignCurrentRoom"),
                            item.get("ParValue"), item.get("Suspension"), item.get("Delist"),
                            item.get("HaltResumeFlag"), item.get("Split"), item.get("Benefit"), item.get("Meeting"),
                            item.get("Notice"), item.get("IssueDate"), item.get("INav"), item.get("IIndex"),
                            item.get("TotalTrade"), item.get("TotalBuyTrade"), item.get("TotalBuyTradeVolume"),
                            item.get("TotalSellTrade"), item.get("TotalSellTradeVolume"),
                            item.get("ReferencePriceAdjusted"),
                            item.get("OpenPriceAdjusted"), item.get("ClosePriceAdjusted"),
                            item.get("PriceChangeAdjusted"),
                            item.get("PercentPriceChangeAdjusted"),
                            item.get("HighestPriceAdjusted"), item.get("LowestPriceAdjusted"), item.get("RateAdjusted"),
                            item.get("Status"), item.get("CreateDate"), item.get("UpdateDate"),
                            item.get("Best1OfferPrice"),
                            item.get("Best2OfferPrice"), item.get("Best3OfferPrice"), item.get("Best1OfferVolume"),
                            item.get("Best2OfferVolume"), item.get("Best3OfferVolume"), item.get("Best1BidPrice"),
                            item.get("Best2BidPrice"),
                            item.get("Best3BidPrice"), item.get("Best1BidVolume"), item.get("Best2BidVolume"),
                            item.get("Best3BidVolume"),
                            item.get("ShareIssue")] for item in data])
    print("Done insert data table stx_mrk_hosestock / Market", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    telegram.send(mess=f"Done insert data table stx_mrk_hosestock / Market / Data size: {len(data)}")


def get_icb_industry(date_request):
    # Init parameter
    if not date_request:  # Nếu không truyền ngày thì mặc định lấy ngày hiện tại
        date_request = datetime.now().strftime("%Y-%m-%d")
    page_index = 1
    page_size = 100
    url = BASE_URL + "/Market/GetIcbIndustry?pageIndex=" + str(page_index) \
          + "&pageSize=" + str(page_size) \
          + "&DateType=" + DATE_TYPE + "&fromDate=" + date_request + "&toDate=" + date_request
    print("[get_icb_industry] get date >> ", url)
    response = service.make_request(url)
    if response is None:
        return

    data = response["Data"]
    paging = response["Paging"]

    while paging.get("NextPage") is not None:
        time.sleep(10)
        print("[get_icb_industry] NextPage : %s in TotalPage : %s", paging.get("NextPage"), paging.get("TotalPage"))
        url = BASE_URL + "/Market/GetIcbIndustry?pageIndex=" + str(paging.get("NextPage")) \
              + "&pageSize=" + str(page_size) \
              + "&DateType=" + DATE_TYPE + "&fromDate=" + date_request + "&toDate=" + date_request
        print("[get_icb_industry] Next page found >> ", url)
        response = service.make_request(url)
        data = data + response["Data"]
        paging = response["Paging"]

        print("[get_icb_industry] Data size: ", len(data))

    database.execute_many('''INSERT INTO fiin_dtf.stx_mrk_IcbIndustry (IcbIndustryId, IcbCode, ComGroupCode, 
    TradingDate, Ceiling, Floor, ReferenceIndex, ReferenceDate, OpenIndex, CloseIndex, IndexChange, 
    PercentIndexChange, MatchIndex, HighestIndex, LowestIndex, AverageIndex, MatchVolume, MatchValue, DealVolume, 
    DealValue, TotalVolume, TotalValue, ForeignBuyValueMatched, ForeignBuyVolumeMatched, ForeignSellValueMatched, 
    ForeignSellVolumeMatched, ForeignBuyValueDeal, ForeignBuyVolumeDeal, ForeignSellValueDeal, 
    ForeignSellVolumeDeal, ForeignBuyValueTotal, ForeignBuyVolumeTotal, ForeignSellValueTotal, 
    ForeignSellVolumeTotal, ParValue, MarketCap, TotalStockUpPrice, TotalStockDownPrice, TotalStockNoChangePrice, 
    TotalStockUpCelling, TotalStockDownFloor, PPHUp, PPHDown, PMarketCap, PPHStand20, PPHStand02, TotalTrade, 
    TotalBuyTrade, TotalBuyTradeVolume, TotalSellTrade, TotalSellTradeVolume, Status, CreateDate, UpdateDate, 
    IndustryID, ForeignTotalRoom, ForeignCurrentRoom, TotalMatchVolume, TotalMatchValue, TotalDealVolume, 
    TotalDealValue) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT ON CONSTRAINT XPKstx_mrk_IcbIndustry DO 
    UPDATE SET (IcbIndustryId, IcbCode, ComGroupCode, TradingDate, Ceiling, Floor, ReferenceIndex, ReferenceDate, 
    OpenIndex, CloseIndex, IndexChange, PercentIndexChange, MatchIndex, HighestIndex, LowestIndex, AverageIndex, 
    MatchVolume, MatchValue, DealVolume, DealValue, TotalVolume, TotalValue, ForeignBuyValueMatched, 
    ForeignBuyVolumeMatched, ForeignSellValueMatched, ForeignSellVolumeMatched, ForeignBuyValueDeal, 
    ForeignBuyVolumeDeal, ForeignSellValueDeal, ForeignSellVolumeDeal, ForeignBuyValueTotal, 
    ForeignBuyVolumeTotal, ForeignSellValueTotal, ForeignSellVolumeTotal, ParValue, MarketCap, TotalStockUpPrice, 
    TotalStockDownPrice, TotalStockNoChangePrice, TotalStockUpCelling, TotalStockDownFloor, PPHUp, PPHDown, 
    PMarketCap, PPHStand20, PPHStand02, TotalTrade, TotalBuyTrade, TotalBuyTradeVolume, TotalSellTrade, 
    TotalSellTradeVolume, Status, CreateDate, UpdateDate, IndustryID, ForeignTotalRoom, ForeignCurrentRoom, 
    TotalMatchVolume, TotalMatchValue, TotalDealVolume, TotalDealValue) = (EXCLUDED.IcbIndustryId, 
    EXCLUDED.IcbCode, EXCLUDED.ComGroupCode, EXCLUDED.TradingDate, EXCLUDED. Ceiling, EXCLUDED.Floor, 
    EXCLUDED.ReferenceIndex, EXCLUDED.ReferenceDate, EXCLUDED.OpenIndex, EXCLUDED.CloseIndex, 
    EXCLUDED.IndexChange, EXCLUDED.PercentIndexChange, EXCLUDED. MatchIndex, EXCLUDED.HighestIndex, 
    EXCLUDED.LowestIndex, EXCLUDED.AverageIndex, EXCLUDED.MatchVolume, EXCLUDED.MatchValue, EXCLUDED.DealVolume, 
    EXCLUDED.DealValue, EXCLUDED. TotalVolume, EXCLUDED.TotalValue, EXCLUDED.ForeignBuyValueMatched, 
    EXCLUDED.ForeignBuyVolumeMatched, EXCLUDED.ForeignSellValueMatched, EXCLUDED. ForeignSellVolumeMatched, 
    EXCLUDED.ForeignBuyValueDeal, EXCLUDED.ForeignBuyVolumeDeal, EXCLUDED.ForeignSellValueDeal, EXCLUDED. 
    ForeignSellVolumeDeal, EXCLUDED.ForeignBuyValueTotal, EXCLUDED.ForeignBuyVolumeTotal, 
    EXCLUDED.ForeignSellValueTotal, EXCLUDED. ForeignSellVolumeTotal, EXCLUDED.ParValue, EXCLUDED.MarketCap, 
    EXCLUDED.TotalStockUpPrice, EXCLUDED.TotalStockDownPrice, EXCLUDED.TotalStockNoChangePrice, EXCLUDED. 
    TotalStockUpCelling, EXCLUDED.TotalStockDownFloor, EXCLUDED.PPHUp, EXCLUDED.PPHDown, EXCLUDED.PMarketCap, 
    EXCLUDED.PPHStand20, EXCLUDED.PPHStand02, EXCLUDED.TotalTrade, EXCLUDED. TotalBuyTrade, 
    EXCLUDED.TotalBuyTradeVolume, EXCLUDED.TotalSellTrade, EXCLUDED.TotalSellTradeVolume, EXCLUDED.Status, 
    EXCLUDED.CreateDate, EXCLUDED.UpdateDate, EXCLUDED. IndustryID, EXCLUDED.ForeignTotalRoom, 
    EXCLUDED.ForeignCurrentRoom, EXCLUDED.TotalMatchVolume, EXCLUDED.TotalMatchValue, EXCLUDED.TotalDealVolume, 
    EXCLUDED. TotalDealValue)''',
                          [[item.get("IcbIndustryId"), item.get("IcbCode"), item.get("ComGroupCode"),
                            item.get("TradingDate"), item.get("Ceiling"),
                            item.get("Floor"),
                            item.get("ReferenceIndex"), item.get("ReferenceDate"), item.get("OpenIndex"),
                            item.get("CloseIndex"),
                            item.get("IndexChange"), item.get("PercentIndexChange"),
                            item.get("MatchIndex"), item.get("HighestIndex"), item.get("LowestIndex"),
                            item.get("AverageIndex"),
                            item.get("MatchVolume"), item.get("MatchValue"),
                            item.get("DealVolume"), item.get("DealValue"), item.get("TotalVolume"),
                            item.get("TotalValue"),
                            item.get("ForeignBuyValueMatched"), item.get("ForeignBuyVolumeMatched"),
                            item.get("ForeignSellValueMatched"), item.get("ForeignSellVolumeMatched"),
                            item.get("ForeignBuyValueDeal"),
                            item.get("ForeignBuyVolumeDeal"), item.get("ForeignSellValueDeal"),
                            item.get("ForeignSellVolumeDeal"),
                            item.get("ForeignBuyValueTotal"), item.get("ForeignBuyVolumeTotal"),
                            item.get("ForeignSellValueTotal"),
                            item.get("ForeignSellVolumeTotal"), item.get("ParValue"), item.get("MarketCap"),
                            item.get("TotalStockUpPrice"), item.get("TotalStockDownPrice"),
                            item.get("TotalStockNoChangePrice"),
                            item.get("TotalStockUpCelling"), item.get("TotalStockDownFloor"), item.get("PPHUp"),
                            item.get("PPHDown"), item.get("PMarketCap"), item.get("PPHStand20"), item.get("PPHStand02"),
                            item.get("TotalTrade"),
                            item.get("TotalBuyTrade"),
                            item.get("TotalBuyTradeVolume"), item.get("TotalSellTrade"),
                            item.get("TotalSellTradeVolume"), item.get("Status"),
                            item.get("CreateDate"), item.get("UpdateDate"),
                            item.get("IndustryID"), item.get("ForeignTotalRoom"), item.get("ForeignCurrentRoom"),
                            item.get("TotalMatchVolume"),
                            item.get("TotalMatchValue"), item.get("TotalDealVolume"),
                            item.get("TotalDealValue")] for item in data])
    print("Done insert data table stx_mrk_IcbIndustry / Market", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    telegram.send(mess=f"Done insert data table stx_mrk_IcbIndustry / Market / Data size: {len(data)}")


def get_upcom_stock(date_request):
    # Init parameter
    if not date_request:  # Nếu không truyền ngày thì mặc định lấy ngày hiện tại
        date_request = datetime.now().strftime("%Y-%m-%d")
    page_index = 1
    page_size = 100
    url = BASE_URL + "/Market/GetUpcomStock?pageIndex=" + str(page_index) \
          + "&pageSize=" + str(page_size) \
          + "&DateType=" + DATE_TYPE + "&fromDate=" + date_request + "&toDate=" + date_request
    print("[get_upcom_stock] get date >> ", url)
    response = service.make_request(url)
    if response is None:
        return

    data = response["Data"]
    paging = response["Paging"]

    while paging.get("NextPage") is not None:
        time.sleep(10)
        print("[get_upcom_stock] NextPage : %s in TotalPage : %s", paging.get("NextPage"), paging.get("TotalPage"))
        url = BASE_URL + "/Market/GetUpcomStock?pageIndex=" + str(paging.get("NextPage")) \
              + "&pageSize=" + str(page_size) \
              + "&DateType=" + DATE_TYPE + "&fromDate=" + date_request + "&toDate=" + date_request
        print("[get_upcom_stock] Next page found >> ", url)
        response = service.make_request(url)
        data = data + response["Data"]
        paging = response["Paging"]

        print("[get_upcom_stock] Data size: ", len(data))

    database.execute_many('''INSERT INTO fiin_dtf.stx_mrk_upcomstock (UpcomStockId, OrganCode, Ticker, TradingDate, 
    StockType, CeilingPrice, FloorPrice, ReferencePrice, ReferenceDate, OpenPrice, ClosePrice, MatchPrice, 
    MatchVolume, MatchValue, PriceChange, PercentPriceChange, HighestPrice, LowestPrice, AveragePrice, 
    DealVolume, DealPrice, TotalMatchVolumeEven, TotalMatchValueEven, TotalMatchVolume, TotalMatchValue, 
    TotalDealVolume, TotalDealValue, TotalVolume, TotalValue, ForeignBuyValueMatched, ForeignBuyVolumeMatched, 
    ForeignSellValueMatched, ForeignSellVolumeMatched, ForeignBuyValueDeal, ForeignBuyVolumeDeal, 
    ForeignSellValueDeal, ForeignSellVolumeDeal, ForeignBuyValueTotal, ForeignBuyVolumeTotal, 
    ForeignSellValueTotal, ForeignSellVolumeTotal, ForeignTotalRoom, ForeignCurrentRoom, Parvalue, 
    SecurityTradingStatus, ListingStatus, IssueDate, BestBidPrice, BestBidVolume, BestOfferPrice, 
    BestOfferVolume, PriorOpenPrice, PriorClosePrice, TotalListingVolume, ReferenceStatus, CurrentPrice, 
    CurrentVolume, PriorPrice, TotalBuyTradingVolume, BuyCount, TotalBuyTradingValue, TotalSellTradingVolume, 
    SellCount, TotalSellTradingValue, TotalBidQtty_OD, TotalOfferQtty_OD, TotalTrade, TotalBuyTrade, 
    TotalBuyTradeVolume, TotalSellTrade, TotalSellTradeVolume, ReferencePriceAdjusted, OpenPriceAdjusted, 
    ClosePriceAdjusted, PriceChangeAdjusted, PercentPriceChangeAdjusted, HighestPriceAdjusted, 
    LowestPriceAdjusted, RateAdjusted, Status, CreateDate, UpdateDate, DealValue, ShareIssue) VALUES (%s, %s, %s, 
    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
    %s) ON CONFLICT ON CONSTRAINT xpkstx_mrk_upcomstock DO UPDATE SET (UpcomStockId, OrganCode, Ticker, 
    TradingDate, StockType, CeilingPrice, FloorPrice, ReferencePrice, ReferenceDate, OpenPrice, ClosePrice, 
    MatchPrice, MatchVolume, MatchValue, PriceChange, PercentPriceChange, HighestPrice, LowestPrice, 
    AveragePrice, DealVolume, DealPrice, TotalMatchVolumeEven, TotalMatchValueEven, TotalMatchVolume, 
    TotalMatchValue, TotalDealVolume, TotalDealValue, TotalVolume, TotalValue, ForeignBuyValueMatched, 
    ForeignBuyVolumeMatched, ForeignSellValueMatched, ForeignSellVolumeMatched, ForeignBuyValueDeal, 
    ForeignBuyVolumeDeal, ForeignSellValueDeal, ForeignSellVolumeDeal, ForeignBuyValueTotal, 
    ForeignBuyVolumeTotal, ForeignSellValueTotal, ForeignSellVolumeTotal, ForeignTotalRoom, ForeignCurrentRoom, 
    Parvalue, SecurityTradingStatus, ListingStatus, IssueDate, BestBidPrice, BestBidVolume, BestOfferPrice, 
    BestOfferVolume, PriorOpenPrice, PriorClosePrice, TotalListingVolume, ReferenceStatus, CurrentPrice, 
    CurrentVolume, PriorPrice, TotalBuyTradingVolume, BuyCount, TotalBuyTradingValue, TotalSellTradingVolume, 
    SellCount, TotalSellTradingValue, TotalBidQtty_OD, TotalOfferQtty_OD, TotalTrade, TotalBuyTrade, 
    TotalBuyTradeVolume, TotalSellTrade, TotalSellTradeVolume, ReferencePriceAdjusted, OpenPriceAdjusted, 
    ClosePriceAdjusted, PriceChangeAdjusted, PercentPriceChangeAdjusted, HighestPriceAdjusted, 
    LowestPriceAdjusted, RateAdjusted, Status, CreateDate, UpdateDate, DealValue, ShareIssue) = (
    EXCLUDED.UpcomStockId, EXCLUDED.OrganCode, EXCLUDED.Ticker, EXCLUDED.TradingDate, EXCLUDED. StockType, 
    EXCLUDED.CeilingPrice, EXCLUDED.FloorPrice, EXCLUDED.ReferencePrice, EXCLUDED.ReferenceDate, 
    EXCLUDED.OpenPrice, EXCLUDED.ClosePrice, EXCLUDED.MatchPrice, EXCLUDED. MatchVolume, EXCLUDED.MatchValue, 
    EXCLUDED.PriceChange, EXCLUDED.PercentPriceChange, EXCLUDED.HighestPrice, EXCLUDED.LowestPrice, 
    EXCLUDED.AveragePrice, EXCLUDED. DealVolume, EXCLUDED.DealPrice, EXCLUDED.TotalMatchVolumeEven, 
    EXCLUDED.TotalMatchValueEven, EXCLUDED.TotalMatchVolume, EXCLUDED.TotalMatchValue, EXCLUDED. TotalDealVolume, 
    EXCLUDED.TotalDealValue, EXCLUDED.TotalVolume, EXCLUDED.TotalValue, EXCLUDED.ForeignBuyValueMatched, 
    EXCLUDED.ForeignBuyVolumeMatched, EXCLUDED. ForeignSellValueMatched, EXCLUDED.ForeignSellVolumeMatched, 
    EXCLUDED.ForeignBuyValueDeal, EXCLUDED.ForeignBuyVolumeDeal, EXCLUDED. ForeignSellValueDeal, 
    EXCLUDED.ForeignSellVolumeDeal, EXCLUDED.ForeignBuyValueTotal, EXCLUDED.ForeignBuyVolumeTotal, 
    EXCLUDED. ForeignSellValueTotal, EXCLUDED.ForeignSellVolumeTotal, EXCLUDED.ForeignTotalRoom, 
    EXCLUDED.ForeignCurrentRoom, EXCLUDED.Parvalue, EXCLUDED. SecurityTradingStatus, EXCLUDED.ListingStatus, 
    EXCLUDED.IssueDate, EXCLUDED.BestBidPrice, EXCLUDED.BestBidVolume, EXCLUDED.BestOfferPrice, EXCLUDED. 
    BestOfferVolume, EXCLUDED.PriorOpenPrice, EXCLUDED.PriorClosePrice, EXCLUDED.TotalListingVolume, 
    EXCLUDED.ReferenceStatus, EXCLUDED.CurrentPrice, EXCLUDED. CurrentVolume, EXCLUDED.PriorPrice, 
    EXCLUDED.TotalBuyTradingVolume, EXCLUDED.BuyCount, EXCLUDED.TotalBuyTradingValue, 
    EXCLUDED.TotalSellTradingVolume, EXCLUDED. SellCount, EXCLUDED.TotalSellTradingValue, 
    EXCLUDED.TotalBidQtty_OD, EXCLUDED.TotalOfferQtty_OD, EXCLUDED.TotalTrade, EXCLUDED.TotalBuyTrade, 
    EXCLUDED. TotalBuyTradeVolume, EXCLUDED.TotalSellTrade, EXCLUDED.TotalSellTradeVolume, 
    EXCLUDED.ReferencePriceAdjusted, EXCLUDED.OpenPriceAdjusted, EXCLUDED. ClosePriceAdjusted, 
    EXCLUDED.PriceChangeAdjusted, EXCLUDED.PercentPriceChangeAdjusted, EXCLUDED.HighestPriceAdjusted, 
    EXCLUDED. LowestPriceAdjusted, EXCLUDED.RateAdjusted, EXCLUDED.Status, EXCLUDED.CreateDate, 
    EXCLUDED.UpdateDate, EXCLUDED.DealValue, EXCLUDED.ShareIssue)''',
                          [[item.get("UpcomStockId"), item.get("OrganCode"), item.get("Ticker"),
                            item.get("TradingDate"),
                            item.get("StockType"),
                            item.get("CeilingPrice"), item.get("FloorPrice"), item.get("ReferencePrice"),
                            item.get("ReferenceDate"),
                            item.get("OpenPrice"),
                            item.get("ClosePrice"), item.get("MatchPrice"), item.get("MatchVolume"),
                            item.get("MatchValue"),
                            item.get("PriceChange"),
                            item.get("PercentPriceChange"), item.get("HighestPrice"), item.get("LowestPrice"),
                            item.get("AveragePrice"),
                            item.get("DealVolume"),
                            item.get("DealPrice"), item.get("TotalMatchVolumeEven"), item.get("TotalMatchValueEven"),
                            item.get("TotalMatchVolume"), item.get("TotalMatchValue"),
                            item.get("TotalDealVolume"), item.get("TotalDealValue"), item.get("TotalVolume"),
                            item.get("TotalValue"),
                            item.get("ForeignBuyValueMatched"),
                            item.get("ForeignBuyVolumeMatched"), item.get("ForeignSellValueMatched"),
                            item.get("ForeignSellVolumeMatched"),
                            item.get("ForeignBuyValueDeal"), item.get("ForeignBuyVolumeDeal"),
                            item.get("ForeignSellValueDeal"), item.get("ForeignSellVolumeDeal"),
                            item.get("ForeignBuyValueTotal"),
                            item.get("ForeignBuyVolumeTotal"), item.get("ForeignSellValueTotal"),
                            item.get("ForeignSellVolumeTotal"), item.get("ForeignTotalRoom"),
                            item.get("ForeignCurrentRoom"),
                            item.get("Parvalue"), item.get("SecurityTradingStatus"),
                            item.get("ListingStatus"), item.get("IssueDate"), item.get("BestBidPrice"),
                            item.get("BestBidVolume"),
                            item.get("BestOfferPrice"),
                            item.get("BestOfferVolume"), item.get("PriorOpenPrice"), item.get("PriorClosePrice"),
                            item.get("TotalListingVolume"), item.get("ReferenceStatus"),
                            item.get("CurrentPrice"), item.get("CurrentVolume"), item.get("PriorPrice"),
                            item.get("TotalBuyTradingVolume"),
                            item.get("BuyCount"),
                            item.get("TotalBuyTradingValue"), item.get("TotalSellTradingVolume"), item.get("SellCount"),
                            item.get("TotalSellTradingValue"), item.get("TotalBidQtty_OD"),
                            item.get("TotalOfferQtty_OD"), item.get("TotalTrade"), item.get("TotalBuyTrade"),
                            item.get("TotalBuyTradeVolume"),
                            item.get("TotalSellTrade"),
                            item.get("TotalSellTradeVolume"), item.get("ReferencePriceAdjusted"),
                            item.get("OpenPriceAdjusted"),
                            item.get("ClosePriceAdjusted"), item.get("PriceChangeAdjusted"),
                            item.get("PercentPriceChangeAdjusted"), item.get("HighestPriceAdjusted"),
                            item.get("LowestPriceAdjusted"),
                            item.get("RateAdjusted"), item.get("Status"),
                            item.get("CreateDate"), item.get("UpdateDate"), item.get("DealValue"),
                            item.get("ShareIssue")] for item in data])
    print("Done insert data table stx_mrk_upcomstock / Market", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    telegram.send(mess=f"Done insert data table stx_mrk_upcomstock / Market / Data size: {len(data)}")
