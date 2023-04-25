using System.Text.Json.Serialization;
using NewVariant.Interfaces;

namespace NewVariant.Models {
    public record Good : IEntity {
        [JsonPropertyName("id")] public int Id { get; }
        [JsonPropertyName("name")] public string Name { get; }
        [JsonPropertyName("shop_id")] public int ShopId { get; }
        [JsonPropertyName("category")] public string Category { get; }
        [JsonPropertyName("description")] public long Price { get; }
        
        public Good(string name, int shopId, string category, long price) {
            Id = _entityCounter++;
            Name = name;
            ShopId = shopId;
            Category = category;
            Price = price;
        }

        [JsonConstructor] 
        public Good(int id, string name, int shopId, string category, long price) : this(name, shopId, category, price) {
            Id = id;
        }
        
        private static int _entityCounter;
    }
}