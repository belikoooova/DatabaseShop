using NewVariant.Interfaces;
using NewVariant.Models;

namespace DataLib;

public class DataAccessLayer: IDataAccessLayer
{
    /// <summary>
    /// Метод возвращает список всех товаров, купленных покупателем с самым длинным именем.
    /// </summary>
    /// <param name="dataBase"> База данных, по которой делается запрос. </param>
    /// <returns> Список товаров. </returns>
    public IEnumerable<Good> GetAllGoodsOfLongestNameBuyer(IDataBase dataBase)
    {
        var goods = from good in dataBase.GetTable<Good>()
            let goodIds = from sale in dataBase.GetTable<Sale>()
                let buyerId = (from buyer in dataBase.GetTable<Buyer>()
                               orderby buyer.Name.Length, buyer.Name
                               select buyer).LastOrDefault().Id
                where sale.BuyerId == buyerId
                select sale.GoodId
            where goodIds.Contains(good.Id)
            select good;
        return goods;
    }

    /// <summary>
    /// Метод возвращает название категории самого дорогого товара.
    /// </summary>
    /// <param name="dataBase"> База данных, по которой делается запрос. </param>
    /// <returns> Название категории товара. </returns>
    public string? GetMostExpensiveGoodCategory(IDataBase dataBase)
    {
        var category = (from good in dataBase.GetTable<Good>()
                        orderby good.Price descending
                        select good).FirstOrDefault()?.Category;
        return category;
    }

    /// <summary>
    /// Метод возвращает название города, в котором было потрачено меньше всего денег.
    /// </summary>
    /// <param name="dataBase"> База данных, по которой делается запрос. </param>
    /// <returns> Название города. </returns>
    public string? GetMinimumSalesCity(IDataBase dataBase)
    {
        var city = (from groupCityByCoast in (from cityWithCoast in (from shop in dataBase.GetTable<Shop>()
                                                                     join shopWithCoast in (from saleWithCoastByShop in (from saleWithCoast in (from sale in dataBase.GetTable<Sale>()
                                                                                                                    join good in dataBase.GetTable<Good>() on sale.GoodId equals good.Id
                                                                                                                    select new {PriceOfSale = sale.GoodCount * good.Price, sale.ShopId})
                                                                                                       group saleWithCoast by saleWithCoast.ShopId)
                                                                                            let prices = from element in saleWithCoastByShop select element.PriceOfSale
                                                                                            select new { PriceOfShop = prices.Sum(), ShopId = saleWithCoastByShop.Key }) on shop.Id equals shopWithCoast.ShopId
                                                                     select new { shopWithCoast.PriceOfShop, shop.City })
                                              group cityWithCoast by cityWithCoast.City)
                    let price = (from el in groupCityByCoast select el.PriceOfShop).Sum()
                    orderby price
                    select groupCityByCoast).FirstOrDefault()?.Key;
        return city;
    }

    /// <summary>
    /// Метод возвращает список покупателей, которые купили самый популярный товар – такой товар, чьих единиц приобретено максимальное число.
    /// </summary>
    /// <param name="dataBase"> База данных, по которой делается запрос. </param>
    /// <returns> Список покупателей. </returns>
    public IEnumerable<Buyer> GetMostPopularGoodBuyers(IDataBase dataBase)
    {
        var buyers = from buyer in dataBase.GetTable<Buyer>()
                     join buyerId in (from newSale in dataBase.GetTable<Sale>()
                                  let mostPopularGoodId = (from goodWithCount in (from groupSaleByGood in (from sale in dataBase.GetTable<Sale>() 
                                                                                                           group sale by sale.GoodId)
                                                                                  let counts = (from el in groupSaleByGood select el.GoodCount).Sum()
                                                                                  select new { CountOfGood = counts, GoodId = groupSaleByGood.Key })
                                                           orderby goodWithCount.CountOfGood descending
                                                           select goodWithCount).First().GoodId
                                  where newSale.GoodId == mostPopularGoodId
                                  select newSale.BuyerId) on buyer.Id equals buyerId
                     select buyer;
        return buyers;
    }

    /// <summary>
    /// Метод возвращает минимальное количество магазинов в странах.
    /// </summary>
    /// <param name="dataBase"> База данных, по которой делается запрос. </param>
    /// <returns> Минимальное количество магазинов в странах. </returns>
    public int GetMinimumNumberOfShopsInCountry(IDataBase dataBase)
    {
        var minCount = from groupShopByCountry in (from shop in dataBase.GetTable<Shop>()
                                                   group shop by shop.Country)
                       select groupShopByCountry.Count();
        return !minCount.Any() ? 0 : minCount.Min();
    }

    /// <summary>
    /// Метод возвращает список покупок, совершенных покупателями во всех городах, отличных от города их проживания.
    /// </summary>
    /// <param name="dataBase"> База данных, по которой делается запрос. </param>
    /// <returns> Список покупок. </returns>
    public IEnumerable<Sale> GetOtherCitySales(IDataBase dataBase)
    {
        var sales = from sale in dataBase.GetTable<Sale>()
                    join buyer in dataBase.GetTable<Buyer>() on sale.BuyerId equals buyer.Id
                    join shop in dataBase.GetTable<Shop>() on sale.ShopId equals shop.Id
                    where buyer.City != shop.City
                    select sale;
        return sales;
    }

    /// <summary>
    /// Метод возвращает общую стоимость покупок, совершенных всеми покупателями.
    /// </summary>
    /// <param name="dataBase"> База данных, по которой делается запрос. </param>
    /// <returns> Общая стоимость покупок. </returns>
    public long GetTotalSalesValue(IDataBase dataBase)
    {
        var coast = (from sale in dataBase.GetTable<Sale>()
                     join good in dataBase.GetTable<Good>() on sale.GoodId equals good.Id
                     select good.Price * sale.GoodCount).Sum();
        return coast;
    }
}