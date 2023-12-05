// @generated automatically by Diesel CLI.

diesel::table! {
    repeat_traders (id) {
        id -> Int4,
        keyhash -> Text,
    }
}

diesel::table! {
    tokens (id) {
        id -> Int4,
        token -> Text,
        creation_ts -> Int8,
        tradable -> Nullable<Bool>,
    }
}

diesel::table! {
    traders (id) {
        id -> Int8,
        trader -> Text,
        token -> Text,
        key_hash -> Text,
        buy_count -> Int4,
        sell_count -> Int4,
        deposit -> Float8,
        token_amount -> Float8,
        min_price -> Float8,
        max_price -> Float8,
        sum_buy_usd_amount -> Float8,
        sum_buy_token_amount -> Float8,
        wmean_buy -> Float8,
        sum_sell_usd_amount -> Float8,
        sum_sell_token_amount -> Float8,
        wmean_sell -> Float8,
        wmean_ratio -> Float4,
        max_single_buy -> Float8,
        min_single_buy -> Float8,
        max_single_sell -> Float8,
        min_single_sell -> Float8,
        max_buy_seq -> Float8,
        cur_buy_seq -> Float8,
        max_sell_seq -> Float8,
        cur_sell_seq -> Float8,
        active -> Bool,
        ts -> Int8,
    }
}

diesel::table! {
    transactions (id) {
        id -> Int8,
        trader -> Text,
        prim -> Text,
        token -> Text,
        key_hash -> Text,
        tx_hash -> Text,
        price_usd -> Float8,
        price -> Float8,
        token_amount -> Float8,
        prim_amount -> Float8,
        usd_amount -> Float8,
        swap_action -> Text,
        ts -> Int8,
    }
}

diesel::allow_tables_to_appear_in_same_query!(
    repeat_traders,
    tokens,
    traders,
    transactions,
);
