CREATE TABLE IF NOT EXISTS Sentiments (
    id int primary key,
    created_at timestamp not null,
    raw_text text not null,
    sentiment text not null
)