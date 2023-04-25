using DataLib;
using NewVariant.Exceptions; 
using NewVariant.Interfaces; 
using NewVariant.Models; 
 
namespace GlinaShop; 
 
public class UnitTests 
{ 
    public static void Test() 
    { 
        TestDataBase(); 
        TestDataAccessLayer(); 
 
        Console.ForegroundColor = ConsoleColor.Green; 
        Console.WriteLine($"Тестирование закончено"); 
        Console.ResetColor(); 
    } 
 
    #region DataBase 
 
    public static void TestDataBase() 
    { 
        TestCreating(); 
        TestInserting(); 
        TestGettingTable(); 
        TestJson(); 
    } 
 
    public static void TestCreating() 
    { 
        var db = new DataBase(); 
        db.CreateTable<Shop>(); 
        db.CreateTable<Good>(); 
        db.CreateTable<Sale>(); 
        db.CreateTable<Buyer>(); 
 
        try 
        { 
            db.GetTable<Shop>(); 
            db.GetTable<Good>(); 
            db.GetTable<Sale>(); 
            db.GetTable<Buyer>(); 
        } 
        catch (Exception ex) 
        { 
            GlinaTest.Assert(false); 
        } 
 
        try 
        { 
            db.CreateTable<Shop>(); 
            GlinaTest.Assert(false); 
        } 
        catch (DataBaseException ex) 
        { 
            GlinaTest.Assert(true); 
        } 
    } 
 
    public static void TestInserting() 
    { 
        var db = new DataBase(); 
 
        try 
        { 
            db.InsertInto(() => new Shop("1", "2", "3")); 
            GlinaTest.Assert(false); 
        } 
        catch (DataBaseException ex) 
        { 
            GlinaTest.Assert(true); 
        } 
 
        db.CreateTable<Shop>(); 
        db.CreateTable<Good>(); 
        db.CreateTable<Sale>(); 
        db.CreateTable<Buyer>(); 
 
        //shops 
        db.InsertInto(() => new Shop(0, "MTC", "Moscow", "Russia")); // id = 0 
        db.InsertInto(() => new Shop(1, "Beeline", "Moscow", "Russia")); // id = 1 
        db.InsertInto(() => new Shop(2, "Goldman Sachs", "Dubai", "UAE")); // id = 2 
        //goods 
        db.InsertInto(() => new Good(0, "Iphone XS", 0, "phones", 1000)); // 0 
        db.InsertInto(() => new Good(1, "Iphone X", 0, "phones", 2000)); // 1 
        db.InsertInto(() => new Good(2, "Iphone XS", 1, "phones", 1500)); // 2 
        db.InsertInto(() => new Good(3, "Iphone X", 1, "phones", 1000)); // 3 
        db.InsertInto(() => new Good(4, "Gold", 2, "stocks", 5000)); // 4 
        //buyers 
        db.InsertInto(() => new Buyer(0, "Alex", "Shalaev", "Moscow", "Russia")); // 0 
        db.InsertInto(() => new Buyer(1, "Andrey", "Stepanov", "Moscow", "Russia")); // 1 
        db.InsertInto(() => new Buyer(2, "Andrey", "Grechko", "Moscow", "Russia")); // 2 
        //sales 
        db.InsertInto(() => new Sale(0, 0, 0, 0, 1)); // 0 Alex bought XS in MTC 
        db.InsertInto(() => new Sale(1, 0, 0, 1, 1)); // 1 Alex bought XS in MTC 
        db.InsertInto(() => new Sale(2, 1, 1, 2, 1)); // 2 Stepanov bought XS in MTC 
        db.InsertInto(() => new Sale(3, 0, 2, 3, 1)); // 3 Alex bought Gold in GS 
        db.InsertInto(() => new Sale(4, 0, 2, 4, 1)); // 4 Alex bought Gold in GS 
 
        GlinaTest.Assert(db.GetTable<Shop>().Count() == 3); 
        GlinaTest.Assert(db.GetTable<Good>().Count() == 5); 
        GlinaTest.Assert(db.GetTable<Buyer>().Count() == 3); 
        GlinaTest.Assert(db.GetTable<Sale>().Count() == 5); 
    } 
 
    public static void TestGettingTable() 
    { 
        var db = new DataBase(); 
 
        try 
        { 
            db.GetTable<Shop>(); 
            db.GetTable<Good>(); 
            db.GetTable<Sale>(); 
            db.GetTable<Buyer>(); 
            GlinaTest.Assert(false); 
        } 
        catch (DataBaseException ex) 
        { 
            GlinaTest.Assert(true); 
        } 
 
        db.CreateTable<Shop>(); 
        db.CreateTable<Good>(); 
        db.CreateTable<Sale>(); 
        db.CreateTable<Buyer>(); 
 
        //shops 
        db.InsertInto(() => new Shop(0, "MTC", "Moscow", "Russia")); // id = 0 
        db.InsertInto(() => new Shop(1, "Beeline", "Moscow", "Russia")); // id = 1
        db.InsertInto(() => new Shop(2, "Goldman Sachs", "Dubai", "UAE")); // id = 2 
        //goods 
        db.InsertInto(() => new Good(0, "Iphone XS", 0, "phones", 1000)); // 0 
        db.InsertInto(() => new Good(1, "Iphone X", 0, "phones", 2000)); // 1 
        db.InsertInto(() => new Good(2, "Iphone XS", 1, "phones", 1500)); // 2 
        db.InsertInto(() => new Good(3, "Iphone X", 1, "phones", 1000)); // 3 
        db.InsertInto(() => new Good(4, "Gold", 2, "stocks", 5000)); // 4 
        //buyers 
        db.InsertInto(() => new Buyer(0, "Alex", "Shalaev", "Moscow", "Russia")); // 0 
        db.InsertInto(() => new Buyer(1, "Andrey", "Stepanov", "Moscow", "Russia")); // 1 
        db.InsertInto(() => new Buyer(2, "Andrey", "Grechko", "Moscow", "Russia")); // 2 
        //sales 
        db.InsertInto(() => new Sale(0, 0, 0, 0, 1)); // 0 Alex bought XS in MTC 
        db.InsertInto(() => new Sale(1, 0, 0, 1, 1)); // 1 Alex bought XS in MTC 
        db.InsertInto(() => new Sale(2, 1, 1, 2, 1)); // 2 Stepanov bought XS in MTC 
        db.InsertInto(() => new Sale(3, 0, 2, 3, 1)); // 3 Alex bought Gold in GS 
        db.InsertInto(() => new Sale(4, 0, 2, 4, 1)); // 4 Alex bought Gold in GS 
 
        var shops = db.GetTable<Shop>(); 
        var goods = db.GetTable<Good>(); 
        var buyers = db.GetTable<Buyer>(); 
        var sales = db.GetTable<Sale>(); 
 
        GlinaTest.Assert(shops.Count() == 3); 
        GlinaTest.Assert(goods.Count() == 5); 
        GlinaTest.Assert(buyers.Count() == 3); 
        GlinaTest.Assert(sales.Count() == 5); 
    } 
 
    public static void TestJson() 
    { 
        var db = new DataBase(); 
 
        try 
        { 
            db.Serialize<Shop>("Shop.txt"); 
            db.Serialize<Good>("Good.txt"); 
            db.Serialize<Sale>("Sale.txt"); 
            db.Serialize<Buyer>("Buyer.txt"); 
 
            db.Deserialize<Shop>("Shop.txt"); 
            db.Deserialize<Good>("Good.txt"); 
            db.Deserialize<Sale>("Sale.txt"); 
            db.Deserialize<Buyer>("Buyer.txt"); 
 
            GlinaTest.Assert(false); 
        } 
        catch (DataBaseException ex) 
        { 
            GlinaTest.Assert(true); 
        } 
 
        db.CreateTable<Shop>(); 
        db.CreateTable<Good>(); 
        db.CreateTable<Sale>(); 
        db.CreateTable<Buyer>(); 
 
        try 
        { 
            var nonExistentPath = "QWERTYUIOPASFGHJKL.dat"; 
 
            db.Deserialize<Shop>(nonExistentPath); 
            db.Deserialize<Good>(nonExistentPath); 
            db.Deserialize<Sale>(nonExistentPath); 
            db.Deserialize<Buyer>(nonExistentPath); 
 
            GlinaTest.Assert(true); 
        } 
        catch (Exception ex) 
        { 
            GlinaTest.Assert(false); 
        } 
 
        //shops 
        db.InsertInto(() => new Shop(0, "MTC", "Moscow", "Russia")); // id = 0 
        db.InsertInto(() => new Shop(1, "Beeline", "Moscow", "Russia")); // id = 1 
        db.InsertInto(() => new Shop(2, "Goldman Sachs", "Dubai", "UAE")); // id = 2 
        //goods 
        db.InsertInto(() => new Good(0, "Iphone XS", 0, "phones", 1000)); // 0 
        db.InsertInto(() => new Good(1, "Iphone X", 0, "phones", 2000)); // 1 
        db.InsertInto(() => new Good(2, "Iphone XS", 1, "phones", 1500)); // 2 
        db.InsertInto(() => new Good(3, "Iphone X", 1, "phones", 1000)); // 3 
        db.InsertInto(() => new Good(4, "Gold", 2, "stocks", 5000)); // 4 
        //buyers 
        db.InsertInto(() => new Buyer(0, "Alex", "Shalaev", "Moscow", "Russia")); // 0 
        db.InsertInto(() => new Buyer(1, "Andrey", "Stepanov", "Moscow", "Russia")); // 1 
        db.InsertInto(() => new Buyer(2, "Andrey", "Grechko", "Moscow", "Russia")); // 2 
        //sales 
        db.InsertInto(() => new Sale(0, 0, 0, 0, 1)); // 0 Alex bought XS in MTC 
        db.InsertInto(() => new Sale(1, 0, 0, 1, 1)); // 1 Alex bought XS in MTC 
        db.InsertInto(() => new Sale(2, 1, 1, 2, 1)); // 2 Stepanov bought XS in MTC 
        db.InsertInto(() => new Sale(3, 0, 2, 3, 1)); // 3 Alex bought Gold in GS 
        db.InsertInto(() => new Sale(4, 0, 2, 4, 1)); // 4 Alex bought Gold in GS 
 
        db.Serialize<Shop>("shops.txt"); 
        db.Serialize<Good>("goods.txt"); 
        db.Serialize<Sale>("sales.txt"); 
        db.Serialize<Buyer>("buyers.txt"); 
 
        db.Deserialize<Shop>("shops.txt"); 
        db.Deserialize<Good>("goods.txt"); 
        db.Deserialize<Sale>("sales.txt"); 
        db.Deserialize<Buyer>("buyers.txt"); 
 
        var shops = db.GetTable<Shop>(); 
        var goods = db.GetTable<Good>(); 
        var buyers = db.GetTable<Buyer>(); 
        var sales = db.GetTable<Sale>(); 
 
        GlinaTest.Assert(shops.Count() == 3); 
        GlinaTest.Assert(goods.Count() == 5); 
        GlinaTest.Assert(buyers.Count() == 3); 
        GlinaTest.Assert(sales.Count() == 5); 
    } 
 
    #endregion 
 
    #region DataAccessLayer 
 
    public static void TestDataAccessLayer() 
    { 
        TestGetAllGoodsOfLongestNameBuyer(); 
        TestGetMostExpensiveGoodCategory(); 
        TestGetMinimumSalesCity(); 
        TestGetMostPopularGoodBuyers(); 
        TestGetMinimumNumberOfShopsInCountry(); 
        TestGetOtherCitySales(); 
        TestGetTotalSalesValue(); 
    } 
 
    public static void TestGetTotalSalesValue() 
    { 
        var dal = new DataAccessLayer(); 
        { 
            var db = new DataBase(); 
            db.CreateTable<Shop>(); 
            db.CreateTable<Good>(); 
            db.CreateTable<Sale>(); 
            db.CreateTable<Buyer>(); 
 
            //shops 
            db.InsertInto(() => new Shop(0, "MTC", "Moscow", "Russia")); // id = 0 
            db.InsertInto(() => new Shop(1, "Beeline", "Moscow", "Russia")); // id = 1 
            db.InsertInto(() => new Shop(2, "Dns", "Kazan", "Russia")); // id = 2 
            //goods 
            db.InsertInto(() => new Good(0, "Iphone XS", 0, "phones", 1000)); // 0 
            db.InsertInto(() => new Good(1, "Iphone X", 1, "phones", 2000)); // 1 
            db.InsertInto(() => new Good(2, "Iphone XS", 2, "phones", 1500)); // 2 
            db.InsertInto(() => new Good(3, "Iphone X", 0, "phones", 1000)); // 3 
            db.InsertInto(() => new Good(4, "Gold", 2, "stocks", 5000)); // 4 
            //buyers 
            db.InsertInto(() => new Buyer(0, "Alex", "Shalaev", "Moscow", "Russia")); // 0 
            db.InsertInto(() => new Buyer(1, "Andrey", "Stepanov", "Moscow", "Russia")); // 1 
            db.InsertInto(() => new Buyer(2, "Andrey", "Grechko", "Moscow", "Russia")); // 2 
            //sales 
            db.InsertInto(() => new Sale(0, 0, 0, 0, 1)); // 0 Moscow 1000 
            db.InsertInto(() => new Sale(1, 1, 1, 1, 1)); // 1 Moscow 2000 
            db.InsertInto(() => new Sale(2, 2, 2, 2, 1)); // 2 Kazan 1500 
            db.InsertInto(() => new Sale(3, 0, 0, 3, 1)); // 3 Moscow 1000 
            db.InsertInto(() => new Sale(4, 1, 2, 4, 1)); // 4 Kazan 5000 
 
            GlinaTest.Assert(dal.GetTotalSalesValue(db) == 1000 + 2000 + 1500 + 1000 + 5000); 
        } 
        { 
            var db = new DataBase(); 
            db.CreateTable<Shop>(); 
            db.CreateTable<Good>(); 
            db.CreateTable<Sale>(); 
            db.CreateTable<Buyer>(); 
 
            //shops 
            db.InsertInto(() => new Shop(0, "MTC", "Moscow", "Russia")); // id = 0 
            db.InsertInto(() => new Shop(1, "Beeline", "Moscow", "Russia")); // id = 1 
            db.InsertInto(() => new Shop(2, "Dns", "Kazan", "Russia")); // id = 2 
            //goods 
            db.InsertInto(() => new Good(0, "Iphone XS", 0, "phones", 1000)); // 0 
            db.InsertInto(() => new Good(1, "Iphone X", 1, "phones", 2000)); // 1 
            db.InsertInto(() => new Good(2, "Iphone XS", 2, "phones", 1500)); // 2 
            db.InsertInto(() => new Good(3, "Iphone X", 0, "phones", 1000)); // 3 
            db.InsertInto(() => new Good(4, "Gold", 2, "stocks", 5000)); // 4 
            //buyers 
            db.InsertInto(() => new Buyer(0, "Alex", "Shalaev", "Moscow", "Russia")); // 0 
            db.InsertInto(() => new Buyer(1, "Andrey", "Stepanov", "Moscow", "Russia")); // 1 
            db.InsertInto(() => new Buyer(2, "Andrey", "Grechko", "Moscow", "Russia")); // 2 
            //sales 
            db.InsertInto(() => new Sale(0, 0, 0, 0, 3)); // 0 Moscow 1000 
            db.InsertInto(() => new Sale(1, 1, 1, 1, 1)); // 1 Moscow 2000 
            db.InsertInto(() => new Sale(2, 2, 2, 2, 2)); // 2 Kazan 1500 
            db.InsertInto(() => new Sale(3, 0, 0, 3, 1)); // 3 Moscow 1000 
            db.InsertInto(() => new Sale(4, 1, 2, 4, 1)); // 4 Kazan 5000 
 
            GlinaTest.Assert(dal.GetTotalSalesValue(db) == 3 * 1000 + 2000 + 2 * 1500 + 1000 + 5000); 
        } 
        { 
            var db = new DataBase(); 
            db.CreateTable<Shop>(); 
            db.CreateTable<Good>(); 
            db.CreateTable<Sale>(); 
            db.CreateTable<Buyer>(); 
            GlinaTest.Assert(dal.GetTotalSalesValue(db) == 0); 
        } 
    } 
 
    public static void TestGetOtherCitySales() 
    { 
        var dal = new DataAccessLayer(); 
        { 
            var db = new DataBase(); 
            db.CreateTable<Shop>(); 
            db.CreateTable<Good>(); 
            db.CreateTable<Sale>(); 
            db.CreateTable<Buyer>(); 
 
            //shops 
            db.InsertInto(() => new Shop(0, "MTC", "Moscow", "Russia")); // id = 0 
            db.InsertInto(() => new Shop(1, "Beeline", "Moscow", "Russia")); // id = 1 
            db.InsertInto(() => new Shop(2, "Dns", "Kazan", "Russia")); // id = 2 
            //goods 
            db.InsertInto(() => new Good(0, "Iphone XS", 0, "phones", 1000)); // 0 
            db.InsertInto(() => new Good(1, "Iphone X", 1, "phones", 2000)); // 1 
            db.InsertInto(() => new Good(2, "Iphone XS", 2, "phones", 1500)); // 2 
            db.InsertInto(() => new Good(3, "Iphone X", 0, "phones", 1000)); // 3 
            db.InsertInto(() => new Good(4, "Gold", 2, "stocks", 5000)); // 4 
            //buyers 
            db.InsertInto(() => new Buyer(0, "Alex", "Shalaev", "Kazan", "Russia")); // 0 
            db.InsertInto(() => new Buyer(1, "Andrey", "Stepanov", "Moscow", "Russia")); // 1 
            db.InsertInto(() => new Buyer(2, "Andrey", "Grechko", "Moscow", "Russia")); // 2 
            //sales 
            db.InsertInto(() => new Sale(0, 0, 1, 1, 1)); // 0  + 
            db.InsertInto(() => new Sale(1, 0, 2, 2, 1)); // 1  - 
            db.InsertInto(() => new Sale(2, 1, 2, 2, 1)); // 2  + 
            db.InsertInto(() => new Sale(3, 1, 0, 3, 1)); // 3  - 
            db.InsertInto(() => new Sale(4, 2, 0, 0, 1)); // 4  - 
 
            GlinaTest.Assert(dal.GetOtherCitySales(db).Count() == 2); 
        } 
        { 
            var db = new DataBase(); 
            db.CreateTable<Shop>(); 
            db.CreateTable<Good>(); 
            db.CreateTable<Sale>(); 
            db.CreateTable<Buyer>(); 
            GlinaTest.Assert(dal.GetOtherCitySales(db).Count() == 0); 
        } 
    } 
 
    public static void TestGetMinimumNumberOfShopsInCountry() 
    { 
        var dal = new DataAccessLayer(); 
        { 
            var db = new DataBase(); 
            db.CreateTable<Shop>(); 
 
            //shops 
            db.InsertInto(() => new Shop(0, "MTC", "Moscow", "Russia")); // id = 0 
            db.InsertInto(() => new Shop(1, "Beeline", "Moscow", "Russia")); // id = 1 
            db.InsertInto(() => new Shop(2, "Goldman Sachs", "Dubai", "UAE")); // id = 2 
 
            GlinaTest.Assert(dal.GetMinimumNumberOfShopsInCountry(db) == 1); 
        } 
        { 
            var db = new DataBase(); 
            db.CreateTable<Shop>(); 
 
            //shops 
            db.InsertInto(() => new Shop(0, "MTC", "Moscow", "Russia")); // id = 0 
            db.InsertInto(() => new Shop(1, "Beeline", "Moscow", "Russia")); // id = 1 
            db.InsertInto(() => new Shop(2, "Goldman Sachs", "Dubai", "UAE")); // id = 2 
            db.InsertInto(() => new Shop(3, "Goldman Sachs", "Dubai", "UAE")); // id = 3 
            db.InsertInto(() => new Shop(4, "Goldman Sachs", "Dubai", "UAE")); // id= 4 
 
            GlinaTest.Assert(dal.GetMinimumNumberOfShopsInCountry(db) == 2); 
        } 
 
        { 
            var db = new DataBase(); 
            db.CreateTable<Shop>(); 
            GlinaTest.Assert(dal.GetMinimumNumberOfShopsInCountry(db) == 0); 
        } 
    } 
 
    public static void TestGetMostPopularGoodBuyers() 
    { 
        var dal = new DataAccessLayer(); 
        { 
            var db = new DataBase(); 
            db.CreateTable<Shop>(); 
            db.CreateTable<Good>(); 
            db.CreateTable<Sale>(); 
            db.CreateTable<Buyer>(); 
 
            //shops 
            db.InsertInto(() => new Shop(0, "MTC", "Moscow", "Russia")); // id = 0 
            db.InsertInto(() => new Shop(1, "Beeline", "Moscow", "Russia")); // id = 1 
            db.InsertInto(() => new Shop(2, "Dns", "Kazan", "Russia")); // id = 2 
            //goods 
            db.InsertInto(() => new Good(0, "Iphone XS", 0, "phones", 1000)); // 0 
            db.InsertInto(() => new Good(1, "Iphone X", 1, "phones", 2000)); // 1 
            db.InsertInto(() => new Good(2, "Iphone XS", 2, "phones", 1500)); // 2 
            db.InsertInto(() => new Good(3, "Iphone X", 0, "phones", 1000)); // 3 
            db.InsertInto(() => new Good(4, "Gold", 2, "stocks", 5000)); // 4 
            //buyers 
            db.InsertInto(() => new Buyer(0, "Alex", "Shalaev", "Moscow", "Russia")); // 0 
            db.InsertInto(() => new Buyer(1, "Andrey", "Stepanov", "Moscow", "Russia")); // 1 
            db.InsertInto(() => new Buyer(2, "Andrey", "Grechko", "Moscow", "Russia")); // 2 
            //sales 
            db.InsertInto(() => new Sale(0, 0, 0, 0, 1)); // 0 Moscow 1000 
            db.InsertInto(() => new Sale(1, 1, 1, 1, 1)); // 1 Moscow 2000 
            db.InsertInto(() => new Sale(2, 2, 2, 2, 1)); // 2 Kazan 1500 
            db.InsertInto(() => new Sale(3, 0, 0, 3, 1)); // 3 Moscow 1000 
            db.InsertInto(() => new Sale(4, 1, 2, 0, 1)); // 4 Kazan 5000 
 
            GlinaTest.Assert(dal.GetMostPopularGoodBuyers(db).Count() == 2); 
        } 
 
        { 
            var db = new DataBase(); 
            db.CreateTable<Shop>(); 
            db.CreateTable<Good>(); 
            db.CreateTable<Sale>(); 
            db.CreateTable<Buyer>(); 
 
            //shops 
            db.InsertInto(() => new Shop(0, "MTC", "Moscow", "Russia")); // id = 0 
            db.InsertInto(() => new Shop(1, "Beeline", "Moscow", "Russia")); // id = 1 
            db.InsertInto(() => new Shop(2, "Dns", "Kazan", "Russia")); // id = 2 
            //goods 
            db.InsertInto(() => new Good(0, "Iphone XS", 0, "phones", 1000)); // 0 
            db.InsertInto(() => new Good(1, "Iphone X", 1, "phones", 2000)); // 1 
            db.InsertInto(() => new Good(2, "Iphone XS", 2, "phones", 1500)); // 2 
            db.InsertInto(() => new Good(3, "Iphone X", 0, "phones", 1000)); // 3 
            db.InsertInto(() => new Good(4, "Gold", 2, "stocks", 5000)); // 4 
            //buyers 
            db.InsertInto(() => new Buyer(0, "Alex", "Shalaev", "Moscow", "Russia")); // 0 
            db.InsertInto(() => new Buyer(1, "Andrey", "Stepanov", "Moscow", "Russia")); // 1 
            db.InsertInto(() => new Buyer(2, "Andrey", "Grechko", "Moscow", "Russia")); // 2 
            //sales 
            db.InsertInto(() => new Sale(0, 0, 0, 0, 1)); // 0 Moscow 1000 
            db.InsertInto(() => new Sale(1, 1, 1, 1, 1)); // 1 Moscow 2000 
            db.InsertInto(() => new Sale(2, 2, 2, 2, 5)); // 2 Kazan 1500 
            db.InsertInto(() => new Sale(3, 0, 2, 2, 1)); // 3 Moscow 1000 
            db.InsertInto(() => new Sale(4, 1, 2, 2, 1)); // 4 Kazan 5000 
 
            GlinaTest.Assert(dal.GetMostPopularGoodBuyers(db).Count() == 3); 
        } 
 
        { 
            var db = new DataBase(); 
            db.CreateTable<Shop>(); 
            db.CreateTable<Good>(); 
            db.CreateTable<Sale>(); 
            db.CreateTable<Buyer>(); 
            GlinaTest.Assert(dal.GetMostPopularGoodBuyers(db).Count() == 0); 
        } 
    }
    public static void TestGetMinimumSalesCity() 
    { 
        var dal = new DataAccessLayer(); 
        { 
            var db = new DataBase(); 
            db.CreateTable<Shop>(); 
            db.CreateTable<Good>(); 
            db.CreateTable<Sale>(); 
            db.CreateTable<Buyer>(); 
 
            //shops 
            db.InsertInto(() => new Shop(0, "MTC", "Moscow", "Russia")); // id = 0 
            db.InsertInto(() => new Shop(1, "Beeline", "Moscow", "Russia")); // id = 1 
            db.InsertInto(() => new Shop(2, "Dns", "Kazan", "Russia")); // id = 2 
            //goods 
            db.InsertInto(() => new Good(0, "Iphone XS", 0, "phones", 1000)); // 0 
            db.InsertInto(() => new Good(1, "Iphone X", 1, "phones", 2000)); // 1 
            db.InsertInto(() => new Good(2, "Iphone XS", 2, "phones", 1500)); // 2 
            db.InsertInto(() => new Good(3, "Iphone X", 0, "phones", 1000)); // 3 
            db.InsertInto(() => new Good(4, "Gold", 2, "stocks", 5000)); // 4 
            //buyers 
            db.InsertInto(() => new Buyer(0, "Alex", "Shalaev", "Moscow", "Russia")); // 0 
            db.InsertInto(() => new Buyer(1, "Andrey", "Stepanov", "Moscow", "Russia")); // 1 
            db.InsertInto(() => new Buyer(2, "Andrey", "Grechko", "Moscow", "Russia")); // 2 
            //sales 
            db.InsertInto(() => new Sale(0, 0, 0, 0, 1)); // 0 Moscow 1000 
            db.InsertInto(() => new Sale(1, 1, 1, 1, 1)); // 1 Moscow 2000 
            db.InsertInto(() => new Sale(2, 2, 2, 2, 1)); // 2 Kazan 1500 
            db.InsertInto(() => new Sale(3, 0, 0, 3, 1)); // 3 Moscow 1000 
            db.InsertInto(() => new Sale(4, 1, 2, 4, 1)); // 4 Kazan 5000 
 
            GlinaTest.Assert(dal.GetMinimumSalesCity(db) == "Moscow"); 
        } 
 
        { 
            var db = new DataBase(); 
            db.CreateTable<Shop>(); 
            db.CreateTable<Good>(); 
            db.CreateTable<Sale>(); 
            db.CreateTable<Buyer>(); 
 
            //shops 
            db.InsertInto(() => new Shop(0, "MTC", "Moscow", "Russia")); // id = 0 
            db.InsertInto(() => new Shop(1, "Beeline", "Moscow", "Russia")); // id = 1 
            db.InsertInto(() => new Shop(2, "Dns", "Kazan", "Russia")); // id = 2 
            //goods 
            db.InsertInto(() => new Good(0, "Iphone XS", 0, "phones", 1000)); // 0 
            db.InsertInto(() => new Good(1, "Iphone X", 1, "phones", 2000)); // 1 
            db.InsertInto(() => new Good(2, "Iphone XS", 2, "phones", 1500)); // 2 
            db.InsertInto(() => new Good(3, "Iphone X", 0, "phones", 1000)); // 3 
            db.InsertInto(() => new Good(4, "Gold", 2, "stocks", 5000)); // 4 
            //buyers 
            db.InsertInto(() => new Buyer(0, "Alex", "Shalaev", "Moscow", "Russia")); // 0 
            db.InsertInto(() => new Buyer(1, "Andrey", "Stepanov", "Moscow", "Russia")); // 1 
            db.InsertInto(() => new Buyer(2, "Andrey", "Grechko", "Moscow", "Russia")); // 2 
            //sales 
            db.InsertInto(() => new Sale(0, 0, 0, 0, 3)); // 0 Moscow 1000 
            db.InsertInto(() => new Sale(1, 1, 1, 1, 2)); // 1 Moscow 2000 
            db.InsertInto(() => new Sale(2, 2, 2, 2, 1)); // 2 Kazan 1500 
            db.InsertInto(() => new Sale(3, 0, 0, 3, 1)); // 3 Moscow 1000 
            db.InsertInto(() => new Sale(4, 1, 2, 4, 1)); // 4 Kazan 5000 
 
            GlinaTest.Assert(dal.GetMinimumSalesCity(db) == "Kazan"); 
        } 
 
        { 
            var db = new DataBase(); 
            db.CreateTable<Shop>(); 
            db.CreateTable<Good>(); 
            db.CreateTable<Sale>(); 
            db.CreateTable<Buyer>(); 
            GlinaTest.Assert(dal.GetMinimumSalesCity(db) == null); 
        } 
    } 
 
    public static void TestGetMostExpensiveGoodCategory() 
    { 
        var dal = new DataAccessLayer(); 
        { 
            var db = new DataBase(); 
            db.CreateTable<Shop>(); 
            db.CreateTable<Good>(); 
            db.CreateTable<Sale>(); 
            db.CreateTable<Buyer>();
    //shops 
            db.InsertInto(() => new Shop(0, "Apple", "Moscow", "Russia")); // id = 0 
            //goods 
            db.InsertInto(() => new Good(0, "Iphone XS", 0, "phones", 5000)); // 0 
            db.InsertInto(() => new Good(1, "Iphone X", 0, "phones", 2000)); // 1 
            //buyers 
            db.InsertInto(() => new Buyer(0, "Alex", "Shalaev", "Moscow", "Russia")); // 0 
            db.InsertInto(() => new Buyer(1, "Andrey", "Stepanov", "Moscow", "Russia")); // 1 
            db.InsertInto(() => new Buyer(2, "ZxcGul", "Grechko", "Moscow", "Russia")); // 2 
            //sale 
            db.InsertInto(() => new Sale(0, 0, 0, 0, 1)); // 0  
            db.InsertInto(() => new Sale(1, 1, 0, 1, 1)); // 1  
            db.InsertInto(() => new Sale(2, 1, 0, 1, 1)); // 2 
            db.InsertInto(() => new Sale(3, 2, 0, 0, 1)); // 3 
            db.InsertInto(() => new Sale(4, 2, 0, 1, 1)); // 4 
 
            GlinaTest.Assert(dal.GetMostExpensiveGoodCategory(db) == "phones"); //phones thx to Copilot 
        } 
 
        { 
            var db = new DataBase(); 
            db.CreateTable<Shop>(); 
            db.CreateTable<Good>(); 
            db.CreateTable<Sale>(); 
            db.CreateTable<Buyer>(); 
            //shops 
            db.InsertInto(() => new Shop(0, "Apple", "Moscow", "Russia")); // id = 0 
            //goods 
            db.InsertInto(() => new Good(0, "Iphone XS", 0, "phones", 5000)); // 0 
            db.InsertInto(() => new Good(1, "Iphone X", 0, "phones", 2000)); // 1 
            db.InsertInto(() => new Good(2, "Iphone 15 Pro Max", 0, "kal", 21000)); // 1 
            //buyers 
            db.InsertInto(() => new Buyer(0, "Alex", "Shalaev", "Moscow", "Russia")); // 0 
            db.InsertInto(() => new Buyer(1, "Andrey", "Stepanov", "Moscow", "Russia")); // 1 
            db.InsertInto(() => new Buyer(2, "ZxcGul", "Grechko", "Moscow", "Russia")); // 2 
            //sale 
            db.InsertInto(() => new Sale(0, 0, 0, 0, 1)); // 0  
            db.InsertInto(() => new Sale(1, 1, 0, 1, 1)); // 1  
            db.InsertInto(() => new Sale(2, 1, 0, 1, 1)); // 2 
            db.InsertInto(() => new Sale(3, 2, 0, 0, 1)); // 3 
            db.InsertInto(() => new Sale(4, 2, 0, 1, 1)); // 4 
 
            GlinaTest.Assert(dal.GetMostExpensiveGoodCategory(db) == "kal"); 
        } 
 
        { 
            var db = new DataBase(); 
            db.CreateTable<Shop>(); 
            db.CreateTable<Good>(); 
            db.CreateTable<Sale>(); 
            db.CreateTable<Buyer>(); 
            GlinaTest.Assert(dal.GetMostExpensiveGoodCategory(db) == null); 
        } 
    } 
 
    public static void TestGetAllGoodsOfLongestNameBuyer() 
    { 
        var dal = new DataAccessLayer(); 
        { 
            var db = new DataBase(); 
            db.CreateTable<Shop>(); 
            db.CreateTable<Good>(); 
            db.CreateTable<Sale>(); 
            db.CreateTable<Buyer>(); 
            //shops 
            db.InsertInto(() => new Shop(0, "Apple", "Moscow", "Russia")); // id = 0 
            //goods 
            db.InsertInto(() => new Good(0, "Iphone XS", 0, "phones", 5000)); // 0 
            db.InsertInto(() => new Good(1, "Iphone X", 0, "phones", 2000)); // 1 
            //buyers 
            db.InsertInto(() => new Buyer(0, "Alex", "Shalaev", "Moscow", "Russia")); // 0 
            db.InsertInto(() => new Buyer(1, "Andrey", "Stepanov", "Moscow", "Russia")); // 1 
            db.InsertInto(() => new Buyer(2, "ZxcGul", "Grechko", "Moscow", "Russia")); // 2 
            //sale 
            db.InsertInto(() => new Sale(0, 0, 0, 0, 1)); // 0  
            db.InsertInto(() => new Sale(1, 1, 0, 1, 1)); // 1  
            db.InsertInto(() => new Sale(2, 1, 0, 1, 1)); // 2 
            db.InsertInto(() => new Sale(3, 2, 0, 0, 1)); // 3 
            db.InsertInto(() => new Sale(4, 2, 0, 1, 1)); // 4 
 
            GlinaTest.Assert(dal.GetAllGoodsOfLongestNameBuyer(db).Sum(g => g.Price) == 7000); 
        } 
 
        { 
            var db = new DataBase(); 
            db.CreateTable<Shop>(); 
            db.CreateTable<Good>(); 
            db.CreateTable<Sale>(); 
            db.CreateTable<Buyer>(); 
            //shops 
            db.InsertInto(() => new Shop(0, "Apple", "Moscow", "Russia")); // id = 0 
            //goods 
            db.InsertInto(() => new Good(0, "Iphone XS", 0, "phones", 5000)); // 0 
            db.InsertInto(() => new Good(1, "Iphone X", 0, "phones", 2000)); // 1 
            //buyers 
            db.InsertInto(() => new Buyer(0, "Alex", "Shalaev", "Moscow", "Russia")); // 0 
            db.InsertInto(() => new Buyer(1, "Andrey", "Stepanov", "Moscow", "Russia")); // 1 
            db.InsertInto(() => new Buyer(2, "Andrey", "Grechko", "Moscow", "Russia")); // 2 
            //sale 
            db.InsertInto(() => new Sale(0, 0, 0, 0, 1)); // 0  
            db.InsertInto(() => new Sale(1, 1, 0, 1, 1)); // 1  
            db.InsertInto(() => new Sale(2, 1, 0, 1, 1)); // 2 
            db.InsertInto(() => new Sale(3, 2, 0, 0, 1)); // 3 
            db.InsertInto(() => new Sale(4, 2, 0, 1, 1)); // 4 
 
            GlinaTest.Assert(dal.GetAllGoodsOfLongestNameBuyer(db).Sum(g => g.Price) == 7000); 
        } 
 
        { 
            var db = new DataBase(); 
            db.CreateTable<Shop>(); 
            db.CreateTable<Good>(); 
            db.CreateTable<Sale>(); 
            db.CreateTable<Buyer>(); 
            GlinaTest.Assert(dal.GetAllGoodsOfLongestNameBuyer(db).Sum(g => g.Price) == 0); 
        } 
    } 
 
    #endregion 
} 
 
public static class GlinaTest 
{ 
    public static void Assert(bool expression) 
    { 
        if (!expression) 
        { 
            Print(); 
        } 
    } 
 
    public static void Assert(object lhs, object rhs) 
    { 
        if (lhs != rhs) 
        { 
            Print(); 
        } 
    } 
 
    private static void Print() 
    { 
        Console.ForegroundColor = ConsoleColor.Red; 
        var parent = new System.Diagnostics.StackTrace(true).GetFrame(2); 
        Console.WriteLine($"Не прошел тест {parent.GetMethod().Name}. Строка: {parent.GetFileLineNumber()}"); 
        Console.ResetColor(); 
    } 
} 
 
public static class Program 
{ 
    public static void Main() 
    { 
        UnitTests.Test(); 
    } 
}
        