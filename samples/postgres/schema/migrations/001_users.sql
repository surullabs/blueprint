-- +goose Up
CREATE TABLE Users;

-- +goose Down
DROP TABLE Users;
