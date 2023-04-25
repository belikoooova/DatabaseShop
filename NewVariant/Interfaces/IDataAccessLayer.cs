using System.Collections.Generic;
using NewVariant.Models;

namespace NewVariant.Interfaces {
    public interface IDataAccessLayer {
        public IEnumerable<Good> GetAllGoodsOfLongestNameBuyer(IDataBase dataBase); // 1
        public string? GetMostExpensiveGoodCategory(IDataBase dataBase); // 2
        public string? GetMinimumSalesCity(IDataBase dataBase); // 3
        public IEnumerable<Buyer> GetMostPopularGoodBuyers(IDataBase dataBase); // 4
        public int GetMinimumNumberOfShopsInCountry(IDataBase dataBase); // 5
        public IEnumerable<Sale> GetOtherCitySales(IDataBase dataBase); // 6
        public long GetTotalSalesValue(IDataBase dataBase); // 7
    }
}