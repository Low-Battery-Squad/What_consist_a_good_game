DROP TABLE IF EXISTS games;

CREATE TABLE games (
    app_id             BIGINT PRIMARY KEY,
    name               TEXT NOT NULL,
    release_date       DATE,
    original_price     NUMERIC(10,2),
    current_price      NUMERIC(10,2),
    review_ratio       NUMERIC(8,5),
    owners_proxy       BIGINT,
    days_since_release INTEGER,
    is_free            BOOLEAN,
    main_genre         TEXT,
    total_reviews      BIGINT,
    genres_json        JSONB,
    raw_data_json      JSONB,
    snapshot_time      TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_games_main_genre   ON games(main_genre);
CREATE INDEX idx_games_is_free      ON games(is_free);
CREATE INDEX idx_games_review_ratio ON games(review_ratio);
CREATE INDEX idx_games_genres_json_gin ON games USING GIN (genres_json);
