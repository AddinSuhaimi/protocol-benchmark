struct OrderItem {
  1: string item_id
  2: i32 quantity
  3: double price
}

struct OrderPayload {
  1: string order_id
  2: string user_id
  3: list<OrderItem> items
  4: double total_amount
}
