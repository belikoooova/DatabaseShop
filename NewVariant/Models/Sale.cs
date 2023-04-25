using System.Text.Json.Serialization;
using NewVariant.Interfaces;

namespace NewVariant.Models {
    public record Sale : IEntity {
        [JsonPropertyName("id")] public int Id {get;}
        [JsonPropertyName("buyer_id")] public int BuyerId { get; }
        [JsonPropertyName("shop_id")] public int ShopId { get; }
        [JsonPropertyName("good_id")] public int GoodId { get; }
        [JsonPropertyName("good_count")] public long GoodCount { get; }
        
        public Sale(int buyerId, int shopId, int goodId, long goodCount) {
            Id = _entityCounter++;
            BuyerId = buyerId;
            ShopId = shopId;
            GoodId = goodId;
            GoodCount = goodCount;
        }
        
        [JsonConstructor] 
        public Sale(int id, int buyerId, int shopId, int goodId, long goodCount) : this(buyerId, shopId, goodId, goodCount) {
            Id = id;
        }
        
        private static int _entityCounter;
    }
}