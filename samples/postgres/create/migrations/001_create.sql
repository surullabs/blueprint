-- +goose Up
-- +goose UnsafeNoTransaction
CREATE DATABASE {{.dbname}};

-- +goose Down
-- +goose UnsafeNoTransaction
DROP DATABASE {{.dbname}};
